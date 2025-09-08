# ==========================================================================================
# Script      : etape_07_bronze_streaming_activites.py
# Objectif    : Kafka (Debezium JSON) → Delta Lake (MinIO S3A)
#               - cast date_debut en timestamp
#               - dédoublonnage par uid (watermark)
#               - partitionnement **mois=YYYY-MM** (aligné avec l'autre job)
#               - DLQ pour messages invalides
#               - checkpoints propres (reset optionnel)
#               - options Kafka robustes + AEQ
# Auteur      : Xavier Rousseau | Août 2025
# ==========================================================================================

import os
import sys
from dotenv import load_dotenv
from loguru import logger

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, trim, when, date_format, substring,
    regexp_replace, rand
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType
)

# =============================================================================
# 1) ENV & helpers (sans fuites de secrets)
# =============================================================================
 
from ntfy_helper import envoyer_message_ntfy  # type: ignore

# Filtre de logs : bloque toute ligne contenant ces mots-clés
SENSITIVE_TOKENS = ("PASSWORD", "SECRET", "TOKEN", "KEY", "ACCESS")
def _no_secret_logs(record):
    msg = record["message"]
    return not any(tok in msg.upper() for tok in SENSITIVE_TOKENS)

def mask(s: str, keep=2) -> str:
    if not s:
        return ""
    return s[:keep] + "***" if len(s) > keep else "***"

logger.remove()
logger.add(sys.stdout, level="INFO", filter=_no_secret_logs)

# Chargement .env centralisé
try:
    load_dotenv("/opt/airflow/.env", override=True)
except Exception:
    pass
load_dotenv(".env", override=True)

# --- Kafka
APP_NAME         = os.getenv("APP_NAME", "StreamingActivitesSportives")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "sportdata.sportdata.activites_sportives")
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
MAX_OFFSETS      = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "20000"))
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")  # latest / earliest / JSON

# --- Delta / MinIO
DELTA_PATH          = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/activites_sportives")
CHECKPOINT_PATH     = os.getenv("CHECKPOINT_PATH_DELTA", "s3a://sportdata/bronze/_checkpoints/activites_sportives")
DLQ_PATH            = os.getenv("DELTA_PATH_ACTIVITES_DLQ", "s3a://sportdata/bronze/_errors/activites_sportives")
MINIO_ENDPOINT_RAW  = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS        = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", ""))
MINIO_SECRET        = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", ""))

# --- Tuning écriture/partitionnement
COALESCE_N       = int(os.getenv("DELTA_COALESCE_TARGET", "1"))
MAX_RECS_FILE    = int(os.getenv("DELTA_MAX_RECORDS_PER_FILE", "500000"))
SHUFFLE_PARTS    = int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
TRIGGER_SECONDS  = int(os.getenv("STREAM_TRIGGER_SECONDS", "30"))

# --- Notifications
NTFY_ENABLED         = os.getenv("NTFY_ENABLED", "true").lower() in {"1", "true", "yes"}
NTFY_MAX_PER_BATCH   = int(os.getenv("NTFY_MAX_PER_BATCH", "200"))  # échantillonnage pour éviter le spam

# --- Reset checkpoint optionnel (utile si on change le partitionnement/plan)
RESET_CHECKPOINT = os.getenv("RESET_CHECKPOINT", "false").lower() in {"1", "true", "yes"}

# Logs d'état init (sans secrets)
logger.info(f"Delta path   = {DELTA_PATH}")
logger.info(f"Checkpoint   = {CHECKPOINT_PATH} | RESET_CHECKPOINT={RESET_CHECKPOINT}")
logger.info(f"DLQ path     = {DLQ_PATH}")
logger.info(f"Kafka topic  = {KAFKA_TOPIC} | bootstrap={KAFKA_BOOTSTRAP} | startingOffsets={STARTING_OFFSETS} | maxOffsetsPerTrigger={MAX_OFFSETS}")
logger.info(f"Coalesce     = {COALESCE_N} | maxRecordsPerFile={MAX_RECS_FILE} | trigger={TRIGGER_SECONDS}s")
logger.info(f"MinIO creds  = access={mask(MINIO_ACCESS)} | secret={mask(MINIO_SECRET)}")
logger.info(f"NTFY         = enabled={NTFY_ENABLED} | max_per_batch={NTFY_MAX_PER_BATCH}")

# =============================================================================
# 2) SparkSession (Delta + S3A/MinIO) — timezone Europe/Paris pour alignement
# =============================================================================

ssl_enabled    = MINIO_ENDPOINT_RAW.lower().startswith("https://")
MINIO_ENDPOINT = MINIO_ENDPOINT_RAW.replace("https://", "").replace("http://", "")

# Optionnel : reset local du checkpoint (si local path)
if RESET_CHECKPOINT and (CHECKPOINT_PATH.startswith("s3a://") is False):
    try:
        import shutil, os as _os
        if _os.path.isdir(CHECKPOINT_PATH):
            shutil.rmtree(CHECKPOINT_PATH)
            logger.warning(f"Checkpoint reset → {CHECKPOINT_PATH} supprimé.")
    except Exception as e:
        logger.error(f"Impossible de supprimer le checkpoint {CHECKPOINT_PATH}: {e}")

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .config("spark.sql.session.timeZone", "Europe/Paris")  # alignement mois avec l'autre job
    # Delta
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    # S3A ↔ MinIO
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    # Tuning Spark
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))
    .config("spark.sql.files.maxRecordsPerFile", str(MAX_RECS_FILE))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # Log4j2 ciblable via conf externe si besoin de réduire le bruit
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# 3) Schémas (Debezium enveloppé → payload.after.*)
#     - id_salarie → StringType (cohérence avec le reste du projet)
# =============================================================================

schema_after = StructType([
    StructField("uid",           StringType()),
    StructField("id_salarie",    StringType()),
    StructField("nom",           StringType()),
    StructField("prenom",        StringType()),
    StructField("date",          StringType()),
    StructField("jour",          StringType()),      # yyyy-mm-dd (si fourni)
    StructField("date_debut",    StringType()),      # ISO 8601 string → cast
    StructField("type_activite", StringType()),
    StructField("distance_km",   DoubleType()),
    StructField("temps_sec",     IntegerType()),
    StructField("commentaire",   StringType()),
    StructField("profil",        StringType()),
])

schema_debezium = StructType().add("payload", StructType([
    StructField("op",    StringType()),
    StructField("after", schema_after),
    StructField("ts_ms", LongType())
]))

# =============================================================================
# 4) Lecture Kafka → parse Debezium → DLQ invalides → nettoyer → dedup (uid)
# =============================================================================

# Source Kafka (options robustes)
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)  # latest/earliest/JSON offsets
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .option("kafka.fetch.max.bytes", "52428800")          # 50 MB
    .option("kafka.max.partition.fetch.bytes", "5242880") # 5 MB/partition
    .option("kafka.session.timeout.ms", "45000")
    .option("kafka.request.timeout.ms", "60000")
    .load()
)

parsed = raw.select(
    from_json(col("value").cast("string"), schema_debezium).alias("r"),
    col("value").cast("string").alias("raw_json")
)

# DLQ pour messages invalides (from_json nul / schéma incompatible)
valid   = parsed.filter(col("r.payload").isNotNull())
invalid = parsed.filter(col("r.payload").isNull()) \
               .withColumn("error_reason", trim(col("raw_json")))

dlq_query = (
    invalid.writeStream
    .format("delta").outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH + "_dlq")
    .start(DLQ_PATH)
)

# parse & cast des lignes valides
clean = (
    valid
    .filter(col("r.payload.op").isin("c", "u"))            # insert/update
    .select("r.payload.after.*")
    .filter(col("uid").isNotNull())
    .withColumn("uid", trim(col("uid")))
    .withColumn("id_salarie", col("id_salarie").cast("string"))
    .withColumn("date_debut", to_timestamp(col("date_debut")))  # ex: 2025-08-11T10:15:30.123Z
)

# Répare 'jour' manquant depuis date_debut (au cas où un producteur l'oublie)
enriched = clean.withColumn(
    "jour",
    when((col("jour").isNull()) | (col("jour") == ""), date_format(col("date_debut"), "yyyy-MM-dd"))
    .otherwise(col("jour"))
)

# Mois normalisé (YYYY-MM) : priorité à date_debut, sinon dérivé de 'jour'
monthly = enriched.withColumn(
    "mois",
    when(col("date_debut").isNotNull(), date_format(col("date_debut"), "yyyy-MM"))
    .otherwise(substring(regexp_replace(col("jour"), r"[^0-9\-]", ""), 1, 7))
)

# Dédoublonnage streaming : une activité unique par uid (watermark 1 jour)
dedup = monthly.withWatermark("date_debut", "1 day").dropDuplicates(["uid"])

# =============================================================================
# 5) Notifications ntfy (échantillonnées) — non bloquant
# =============================================================================

def notifier_lignes_sample(df: DataFrame, epoch_id: int) -> None:
    """Envoie au plus NTFY_MAX_PER_BATCH notifications (échantillon aléatoire) pour éviter le spam."""
    if not NTFY_ENABLED:
        logger.info(f"[{epoch_id}] 🔕 NTFY désactivées.")
        return

    sel = df.select(
        col("prenom").cast("string").alias("prenom"),
        col("type_activite").cast("string").alias("type_activite"),
        col("distance_km").cast("double").alias("distance_km"),
        col("temps_sec").cast("int").alias("temps_sec"),
    )

    try:
        total = sel.count()
        if total == 0:
            logger.info(f"[{epoch_id}] aucune ligne à notifier.")
            return

        sample_df = sel if total <= NTFY_MAX_PER_BATCH else sel.orderBy(rand()).limit(NTFY_MAX_PER_BATCH)

        ok = err = 0
        for r in sample_df.toLocalIterator():
            try:
                km = float(r.distance_km) if r.distance_km is not None else 0.0
                if km < 0: km = 0.0
                sec = int(r.temps_sec) if r.temps_sec is not None else 0
                minutes = max(0, sec // 60)
                envoyer_message_ntfy(r.prenom or "?", r.type_activite or "?", km, minutes)
                ok += 1
            except Exception as e:
                err += 1
                logger.warning(f"[{epoch_id}] ntfy erreur pour {getattr(r, 'prenom', '?')}: {e}")

        logger.info(f"[{epoch_id}] ntfy envoyées: {ok}/{min(total, NTFY_MAX_PER_BATCH)} (total batch={total}) | erreurs: {err}")
    except Exception as e:
        logger.warning(f"[{epoch_id}] notifications ignorées (non bloquant) : {e}")

# =============================================================================
# 6) foreachBatch : append Delta (partitionBy 'mois' YYYY-MM) → notifications
# =============================================================================

def ecrire_delta_et_notifier(batch_df: DataFrame, epoch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        logger.info(f"[{epoch_id}] batch vide, skip.")
        return

    out = batch_df.coalesce(COALESCE_N)

    # écriture partitionnée par mois (YYYY-MM)
    try:
        (
            out.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("maxRecordsPerFile", str(MAX_RECS_FILE))
            .partitionBy("mois")
            .save(DELTA_PATH)
        )
        logger.info(f"[{epoch_id}] batch écrit (partition='mois' YYYY-MM).")
    except Exception as e:
        msg = str(e)
        # fallback si la table Delta existante n'est pas partitionnée comme attendu
        if "Partition columns do not match" in msg or "partition columns of the table" in msg:
            logger.warning(
                f"[{epoch_id}] Table existante incompatible (partition). "
                f"Fallback sans partition. Conseil: changer DELTA_PATH ou recréer la table pour 'mois'. Détail: {msg}"
            )
            (
                out.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .option("maxRecordsPerFile", str(MAX_RECS_FILE))
                .save(DELTA_PATH)
            )
            logger.info(f"[{epoch_id}] batch écrit (sans partition).")
        else:
            raise

    # notifications non bloquantes (échantillonnées)
    notifier_lignes_sample(out, epoch_id)

# =============================================================================
# 7) Lancement du streaming
# =============================================================================

main_query = (
    dedup.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)     # placer sur S3A pour résilience
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .foreachBatch(ecrire_delta_et_notifier)
    .start()
)

logger.info("🟢 Streaming démarré (dedup uid → Delta partitionné 'mois' → ntfy).")
# Le flux DLQ tourne en parallèle; on attend la fin du flux principal.
main_query.awaitTermination()
