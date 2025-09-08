# ==========================================================================================
# Script : etape_09_spark_bronze_streaming_primes_jbe.py
# But    : Kafka (primes + JBE) → dédup (uid,event_type)
#          → NTFY (résumé via ntfy_helper.envoyer_resume_pipeline, avec garde-fous)
#          → Écriture Delta Lake (MinIO) partitionnée (event_type, ref_temporale=YYYY-MM)
# Auteur : Xavier Rousseau | Août 2025
# ==========================================================================================
import os
import sys
import json
import time
from dotenv import load_dotenv
from loguru import logger

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, from_unixtime, date_format, coalesce as fcoalesce,
    when, lit, trim, lower
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)

# --- Helper notifications uniquement ---
sys.path.append("/opt/airflow/scripts")
from ntfy_helper import (  # type: ignore
    NTFY_URL as NTFY_URL_HELPER_DEFAULT,
    envoyer_resume_pipeline,
    envoyer_message_erreur,
)

# ==========================================================================================
# 0) Logs & ENV
# ==========================================================================================

# --- Logging : pas de secrets dans les logs ---------------------------------
SENSITIVE_TOKENS = ("PASSWORD", "SECRET", "TOKEN", "KEY", "ACCESS")
def _no_secret_logs(record):
    msg = record["message"]
    upper = msg.upper()
    return not any(tok in upper for tok in SENSITIVE_TOKENS)

logger.remove()
logger.add(sys.stdout, level="INFO", filter=_no_secret_logs)

def mask(s: str, keep=3) -> str:
    if not s:
        return ""
    return s[:keep] + "***" if len(s) > keep else "***"

# --- Chargement .env (local puis global) -------------------------------------
try:
    load_dotenv(dotenv_path="/opt/airflow/.env", override=True)
except Exception:
    pass
load_dotenv(dotenv_path=".env", override=True)

# --- Kafka (2 topics) --------------------------------------------------------
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC_PRIMES = os.getenv("KAFKA_TOPIC_PRIMES", "sportdata.sportdata.beneficiaires_primes_sport")
KAFKA_TOPIC_JBE    = os.getenv("KAFKA_TOPIC_JBE",    "sportdata.sportdata.beneficiaires_journees_bien_etre")
TOPICS_CSV         = ",".join([KAFKA_TOPIC_PRIMES, KAFKA_TOPIC_JBE])
STARTING_OFFSETS   = os.getenv("KAFKA_STARTING_OFFSETS", "latest")  # latest/earliest/JSON
MAX_OFFSETS        = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "20000"))

# --- Checkpoint + trigger ----------------------------------------------------
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH_PRIMES_JBE", "/tmp/checkpoints/primes_jbe")
TRIGGER_SECONDS = int(os.getenv("STREAM_TRIGGER_SECONDS", "30"))

# --- MinIO / Delta -----------------------------------------------------------
MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS       = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", ""))
MINIO_SECRET       = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", ""))
DELTA_BASE         = os.getenv("DELTA_BASE_PRIMES_JBE", "s3a://sportdata/bronze_primes_jbe")

# Normalisation endpoint (sans schéma)
ssl_enabled    = MINIO_ENDPOINT_RAW.lower().startswith("https://")
MINIO_ENDPOINT = MINIO_ENDPOINT_RAW.replace("https://", "").replace("http://", "")

# --- Tuning fichiers Delta ---------------------------------------------------
COALESCE_N        = int(os.getenv("DELTA_COALESCE_TARGET", "1"))           # 1 = gros fichiers
MAX_RECORDS_FILE  = int(os.getenv("DELTA_MAX_RECORDS_PER_FILE", "500000"))

# --- NTFY (notifs) -----------------------------------------------------------
NTFY_URL_FALLBACK = os.getenv("NTFY_URL_DEFAULT", "http://sport-ntfy:80")
NTFY_URL     = os.getenv("NTFY_URL", NTFY_URL_FALLBACK or NTFY_URL_HELPER_DEFAULT)
NTFY_TOPIC   = os.getenv("NTFY_TOPIC_PRIMES_JBE", os.getenv("NTFY_TOPIC", "avantages_sportifs"))
NTFY_ENABLED = os.getenv("NTFY_ENABLED", "true").lower() in {"1","true","yes"}
if not NTFY_ENABLED:
    logger.info("🔕 NTFY_ENABLED=false — notifications désactivées.")

# --- Debounce pour résumés + cap toPandas -----------------------------------
RESUME_LOCK       = os.path.join(CHECKPOINT_PATH, "_last_resume.json")
DEBOUNCE_SEC      = int(os.getenv("NTFY_DEBOUNCE_SECONDS", "60"))
NTFY_SAMPLE_ROWS  = int(os.getenv("NTFY_SAMPLE_ROWS", "500"))  # limite prudente pour toPandas()

def _can_send_resume() -> bool:
    try:
        with open(RESUME_LOCK, "r") as f:
            last = json.load(f).get("ts", 0)
    except Exception:
        last = 0
    return (time.time() - last) >= DEBOUNCE_SEC

def _mark_resume_sent():
    try:
        os.makedirs(CHECKPOINT_PATH, exist_ok=True)
        with open(RESUME_LOCK, "w") as f:
            json.dump({"ts": time.time()}, f)
    except Exception:
        pass

# --- Journaux d'état (sans secrets) -----------------------------------------
logger.info(f"Kafka bootstrap: {KAFKA_BOOTSTRAP}")
logger.info(f"Kafka topics: {TOPICS_CSV} | starting_offsets={STARTING_OFFSETS} | max_offsets_per_trigger={MAX_OFFSETS}")
logger.info(f"Checkpoint: {CHECKPOINT_PATH} | trigger={TRIGGER_SECONDS}s")
logger.info(f"MinIO endpoint: {MINIO_ENDPOINT} (SSL: {ssl_enabled}) | access={mask(MINIO_ACCESS)} | secret={mask(MINIO_SECRET)}")
logger.info(f"Delta base: {DELTA_BASE}")
logger.info(f"NTFY URL: {NTFY_URL} | topic: {NTFY_TOPIC} | enabled={NTFY_ENABLED}")

# ==========================================================================================
# 1) Spark session
# ==========================================================================================
spark = (
    SparkSession.builder
      .appName("StreamingPrimesJBE")
      # Delta Lake
      .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      # S3A → MinIO
      .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
      .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
      .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      # Timezone métier
      .config("spark.sql.session.timeZone","Europe/Paris")
      .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

logger.info(f"Kafka topics = {TOPICS_CSV} | bootstrap={KAFKA_BOOTSTRAP} | startingOffsets={STARTING_OFFSETS} | maxOffsetsPerTrigger={MAX_OFFSETS}")
logger.info(f"Delta base   = {DELTA_BASE} | checkpoint={CHECKPOINT_PATH}")
logger.info(f"Files tune   = coalesce={COALESCE_N} | maxRecordsPerFile={MAX_RECORDS_FILE}")
logger.info(f"NTFY topic   = {NTFY_TOPIC} | enabled={NTFY_ENABLED}")

# ==========================================================================================
# 2) Schéma des messages (periode/date_prime optionnelles)
#    ⚠️ id_salarie en StringType pour rester cohérent (VARCHAR en BDD et Kafka)
# ==========================================================================================
schema_after = StructType([
    StructField("uid", StringType()),
    StructField("event_type", StringType()),          # "prime" | "jbe"
    StructField("periode", StringType()),             # optionnel (ancien batch)
    StructField("date_prime", StringType()),          # optionnel (batch actuel, primes only)
    StructField("id_salarie", StringType()),
    StructField("nom", StringType()),
    StructField("prenom", StringType()),
    StructField("nb_activites", IntegerType()),
    StructField("prime_montant_eur", DoubleType()),
    StructField("nb_journees_bien_etre", IntegerType()),
])
schema_env = StructType().add("payload", StructType([
    StructField("op", StringType()),
    StructField("after", schema_after),
    StructField("ts_ms", LongType())
]))

# ==========================================================================================
# 3) Lecture Kafka
# ==========================================================================================
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPICS_CSV)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .load()
)

parsed = (
    raw
    .selectExpr("topic", "CAST(value AS STRING) AS value")
    .select(col("topic"), from_json(col("value"), schema_env).alias("r"))
    .filter(col("r.payload").isNotNull())
    .filter(col("r.payload.op") == "c")  # on reste sur 'create'
    .select("topic", "r.payload.after.*", "r.payload.ts_ms")
)

# Normaliser event_type depuis le topic si manquant, puis canoniser (trim + lower)
parsed = parsed.withColumn(
    "event_type",
    lower(trim(
        when((trim(col("event_type")).isNull()) | (trim(col("event_type")) == ""),
             when(col("topic") == lit(KAFKA_TOPIC_PRIMES), lit("prime"))
             .when(col("topic") == lit(KAFKA_TOPIC_JBE),   lit("jbe"))
             .otherwise(lit("unknown")))
        .otherwise(col("event_type"))
    ))
)

# event_time à partir de ts_ms (epoch millis) + uid nettoyé
events = (
    parsed
    .withColumn("uid", trim(col("uid")))
    .filter(col("uid").isNotNull())
    .withColumn("event_time", to_timestamp(from_unixtime(col("ts_ms")/1000)))
)

# Dédup composite sur (uid, event_type) avec watermark 1h
dedup = events.withWatermark("event_time", "1 hour").dropDuplicates(["uid", "event_type"])

# ==========================================================================================
# 4) foreachBatch : Résumé + Écriture Delta
# ==========================================================================================
def process_batch(df: DataFrame, epoch_id: int):
    try:
        # Vues pour résumé (on limite la conversion Pandas)
        primes_sdf = (
            df.filter(col("event_type") == "prime")
              .select("id_salarie", "nom", "prenom", "prime_montant_eur", "nb_activites")
              .where(col("prime_montant_eur").isNotNull())
        )
        jbe_sdf = (
            df.filter(col("event_type") == "jbe")
              .select("id_salarie", "nb_activites", "nb_journees_bien_etre")
              .where(col("nb_journees_bien_etre").isNotNull())
        )

        # Comptages exacts (batch statique) + échantillon pour le résumé
        n_p = primes_sdf.count()
        n_j = jbe_sdf.count()

        df_primes_pd = primes_sdf.limit(NTFY_SAMPLE_ROWS).toPandas()
        df_jbe_pd    = jbe_sdf.limit(NTFY_SAMPLE_ROWS).toPandas()

        # Notifications groupées (seulement si non vide + debounce)
        if NTFY_ENABLED and (n_p > 0 or n_j > 0) and _can_send_resume():
            url_rapport = f"stream://batch/{epoch_id}"  # placeholder
            envoyer_resume_pipeline(df_primes_pd, df_jbe_pd, url_rapport, topic=NTFY_TOPIC)
            _mark_resume_sent()
            logger.info(f"[{epoch_id}] ntfy résumé envoyé: primes={n_p} | jbe={n_j} (sample≤{NTFY_SAMPLE_ROWS})")
        else:
            logger.info(f"[{epoch_id}] résumé non envoyé (primes={n_p} | jbe={n_j} | debounce/NTFY).")

    except Exception as e:
        msg = f"❌ Streaming primes/JBE — erreur envoi résumé (batch {epoch_id}): {e}"
        logger.warning(msg)
        try:
            if NTFY_ENABLED:
                envoyer_message_erreur(NTFY_TOPIC, msg)
        except Exception as e2:
            logger.error(f"ntfy erreur secondaire: {e2}")

    # Écriture Delta (bronze) — partition: type + ref_temporale (YYYY-MM)
    out = df.withColumn(
        "ref_temporale",
        date_format(
            fcoalesce(
                to_timestamp(trim(col("date_prime"))),  # ex: '2025-08-13' -> mois '2025-08'
                col("event_time")                       # sinon, mois d'event_time
            ),
            "yyyy-MM"
        )
    )

    (
        out.coalesce(COALESCE_N)
           .write
           .format("delta")
           .mode("append")
           .option("mergeSchema", "true")
           .option("maxRecordsPerFile", str(MAX_RECORDS_FILE))
           .partitionBy("event_type", "ref_temporale")
           .save(DELTA_BASE)
    )
    logger.info(f"[{epoch_id}] écrit Delta → {DELTA_BASE} (partition=event_type/ref_temporale, coalesce={COALESCE_N}).")

# ==========================================================================================
# 5) Démarrage du streaming
# ==========================================================================================
query = (
    dedup.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)   # ⚠ un seul job actif par checkpoint
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .foreachBatch(process_batch)
    .start()
)

logger.info("🟢 Streaming primes/JBE démarré.")
query.awaitTermination()
