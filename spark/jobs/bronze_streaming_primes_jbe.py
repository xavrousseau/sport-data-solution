# ==========================================================================================
# Script : bronze_streaming_primes_jbe.py
# But    : Kafka (primes + JBE) → dédup (uid)
#          → NTFY (résumé + détails via ntfy_helper.envoyer_resume_pipeline)
#          → Écriture Delta Lake (MinIO)
# Auteur : Xavier Rousseau | Août 2025
# ==========================================================================================
import os, sys
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, from_unixtime, date_format, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)

# --- On N'UTILISE QUE le helper pour les notifications ---
sys.path.append("/opt/airflow/scripts")
from ntfy_helper import (  # type: ignore
    NTFY_URL as NTFY_URL_DEFAULT,
    NTFY_TOPIC as NTFY_TOPIC_DEFAULT,
    envoyer_resume_pipeline,
    envoyer_message_erreur,
)

# ==========================================================================================
# 0) Logs & ENV
# ==========================================================================================
logger.remove(); logger.add(sys.stdout, level="INFO")
load_dotenv(dotenv_path="/opt/airflow/.env", override=True)
load_dotenv(dotenv_path=".env", override=True)

# Kafka
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC_PRIMES = os.getenv("KAFKA_TOPIC_PRIMES", "sportdata.sportdata.beneficiaires_primes_sport")
KAFKA_TOPIC_JBE    = os.getenv("KAFKA_TOPIC_JBE",    "sportdata.sportdata.beneficiaires_journees_bien_etre")
TOPICS_CSV = ",".join([KAFKA_TOPIC_PRIMES, KAFKA_TOPIC_JBE])

# Checkpoint + trigger
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH_PRIMES_JBE", "tmp/checkpoints/ntfy_activites")
TRIGGER_SECONDS = int(os.getenv("STREAM_TRIGGER_SECONDS", "30"))

# MinIO / Delta
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET    = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
DELTA_BASE      = os.getenv("DELTA_BASE_PRIMES_JBE", "s3a://sportdata/bronze_primes_jbe")

# NTFY (topic imposé par défaut)
NTFY_URL   = os.getenv("NTFY_URL", NTFY_URL_DEFAULT)
NTFY_TOPIC = os.getenv("avantages_sportifs")

# ==========================================================================================
# 1) Spark session
# ==========================================================================================
spark = (
    SparkSession.builder
      .appName("StreamingPrimesJBE")
      # Delta Lake
      .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
      # S3A → MinIO
      .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
      .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
      .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # endpoint http
      # Timezone
      .config("spark.sql.session.timeZone","Europe/Paris")
      .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 2) Schéma des messages (periode/date_prime optionnelles pour rétro-compat)
# ==========================================================================================
schema_after = StructType([
    StructField("uid", StringType()),
    StructField("event_type", StringType()),          # "prime" | "jbe"
    StructField("periode", StringType()),             # optionnel (ancien batch)
    StructField("date_prime", StringType()),          # optionnel (batch actuel, primes only)
    StructField("id_salarie", IntegerType()),
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
    .option("startingOffsets","latest")
    .option("failOnDataLoss","false")
    .load()
)

parsed = (
    raw
    .selectExpr("topic", "CAST(value AS STRING) AS value")
    .select(col("topic"), from_json(col("value"), schema_env).alias("r"))
    .filter(col("r.payload").isNotNull())
    .filter(col("r.payload.op") == "c")
    .select("topic", "r.payload.after.*", "r.payload.ts_ms")
)

events = parsed.withColumn("event_time", to_timestamp(from_unixtime(col("ts_ms")/1000)))
dedup = events.withWatermark("event_time", "1 hour").dropDuplicates(["uid"])

# ==========================================================================================
# 4) foreachBatch : Résumé + détails via helper + Écriture Delta
# ==========================================================================================
def process_batch(df: DataFrame, epoch_id: int):
    try:
        # --- 4.1 Construire les DF pour le helper (format pandas attendu) ---
        # PRIMES
        primes_sdf = (
            df.filter(
                (col("event_type") == "prime") | (col("topic").contains("beneficiaires_primes_sport"))
            )
            .select("id_salarie", "nom", "prenom", "prime_montant_eur", "nb_activites")
            .where(col("prime_montant_eur").isNotNull())
        )
        # JBE
        jbe_sdf = (
            df.filter(
                (col("event_type") == "jbe") | (col("topic").contains("beneficiaires_journees_bien_etre"))
            )
            .select("id_salarie", "nb_activites", "nb_journees_bien_etre")
            .where(col("nb_journees_bien_etre").isNotNull())
        )

        # Convertir en pandas (micro-batch => volumes raisonnables)
        df_primes_pd = primes_sdf.toPandas()
        df_jbe_pd    = jbe_sdf.toPandas()

        # --- 4.2 Notifications groupées via ntfy_helper.envoyer_resume_pipeline ---
        # url_rapport : on n'en a pas côté stream → on met un placeholder
        url_rapport = f"stream://batch/{epoch_id}"
        envoyer_resume_pipeline(df_primes_pd, df_jbe_pd, url_rapport, topic=NTFY_TOPIC)

        logger.info(
            f"[{epoch_id}] ntfy résumé envoyé: primes={len(df_primes_pd)} | jbe={len(df_jbe_pd)}"
        )

    except Exception as e:
        # En cas d'échec de notif → on loggue et on utilise envoyer_message_erreur
        msg = f"❌ Streaming primes/JBE — erreur envoi résumé (batch {epoch_id}): {e}"
        logger.warning(msg)
        try:
            envoyer_message_erreur(NTFY_TOPIC, msg)
        except Exception as e2:
            logger.error(f"ntfy erreur secondaire: {e2}")

    # --- 4.3 Écriture Delta (bronze) — on partitionne par type + ref temporelle ---
    out = df.withColumn(
        "ref_temporale",
        coalesce(col("periode"), col("date_prime"), date_format(col("event_time"), "yyyy-MM"))
    )
    (
        out.coalesce(1)  # moins de petits fichiers
           .write
           .format("delta")
           .mode("append")
           .partitionBy("event_type", "ref_temporale")
           .save(DELTA_BASE)
    )

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
