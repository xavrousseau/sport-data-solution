# ==========================================================================================
# Script      : bronze_streaming_activites.py (sans filtre date)
# Objectif    : Kafka (Debezium JSON) → Delta Lake (MinIO S3A),
#               dédoublonnage 'uid', notifications ntfy pour toutes les lignes,
#               fichiers Delta plus "lourds" (coalesce / maxRecordsPerFile), logs sobres.
# Auteur      : Xavier Rousseau | Août 2025
# ==========================================================================================

import os
import sys
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType
)

# =============================================================================
# 1) ENV & helpers
# =============================================================================

# Helper ntfy (exposé dans l'image Airflow)
sys.path.append("/opt/airflow/scripts")
from ntfy_helper import envoyer_message_ntfy  # type: ignore

# Chargement .env centralisé (même chemin que vos autres scripts)
load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

# --- Kafka (garde bien ce topic)
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "sportdata.sportdata.activites_sportives")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
MAX_OFFSETS     = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "20000"))

# --- Delta / MinIO
DELTA_PATH      = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH_DELTA", "/tmp/checkpoints/bronze_activites_sportives")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET    = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

# --- Tuning écriture/partitionnement
COALESCE_N      = int(os.getenv("DELTA_COALESCE_TARGET", "1"))          # 1 = un gros fichier par micro-batch
MAX_RECS_FILE   = int(os.getenv("DELTA_MAX_RECORDS_PER_FILE", "500000")) # cible d’enregistrements par fichier
SHUFFLE_PARTS   = int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "2"))    # dev: 2-4
TRIGGER_SECONDS = int(os.getenv("STREAM_TRIGGER_SECONDS", "30"))         # fréquence micro-batch (s)

# Logs sobres
logger.remove()
logger.add(sys.stdout, level="INFO")

# =============================================================================
# 2) Spark minimal (Delta + S3A)
# =============================================================================

spark = (
    SparkSession.builder
    .appName("StreamingActivitesSportives")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # MinIO (S3A)
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Tuning
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))
    .config("spark.sql.session.timeZone", "Europe/Paris")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

logger.info(f"Delta path  = {DELTA_PATH}")
logger.info(f"Kafka topic = {KAFKA_TOPIC}")
logger.info(f"Coalesce={COALESCE_N} | maxRecordsPerFile={MAX_RECS_FILE} | maxOffsetsPerTrigger={MAX_OFFSETS}")

# =============================================================================
# 3) Schémas (Debezium enveloppé → payload.after.*)
# =============================================================================

schema_after = StructType([
    StructField("uid",           StringType()),
    StructField("id_salarie",    LongType()),
    StructField("nom",           StringType()),
    StructField("prenom",        StringType()),
    StructField("date",          StringType()),
    StructField("jour",          StringType()),
    StructField("date_debut",    StringType()),
    StructField("type_activite", StringType()),
    StructField("distance_km",   DoubleType()),
    StructField("temps_sec",     IntegerType()),
    StructField("commentaire",   StringType()),
])

schema_debezium = StructType().add("payload", StructType([
    StructField("op",    StringType()),
    StructField("after", schema_after),
    StructField("ts_ms", LongType())
]))

# =============================================================================
# 4) Lecture Kafka → parse Debezium → nettoyer → dedup
#     ⚠️ Pas de filtre de dates ici.
# =============================================================================

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")     # évite la lecture d’offsets périmés
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .load()
)

parsed = raw.select(from_json(col("value").cast("string"), schema_debezium).alias("r"))

clean = (
    parsed
    .filter(col("r.payload").isNotNull())
    .filter(col("r.payload.op").isin("c", "u"))   # insert/update
    .select("r.payload.after.*")
    .withColumn("date_debut", to_timestamp("date_debut"))  # normalise timestamp
)

# Dédoublonnage streaming sur uid (watermark sur date_debut)
dedup = clean.withWatermark("date_debut", "1 hour").dropDuplicates(["uid"])

# =============================================================================
# 5) Notifications ntfy (toutes les lignes du batch)
# =============================================================================

def notifier_toutes_lignes(df: DataFrame, epoch_id: int) -> None:
    a_notifier = df.select("prenom", "type_activite", "distance_km", "temps_sec")
    rows = a_notifier.collect()
    if not rows:
        logger.info(f"[{epoch_id}] aucune notification à envoyer (batch vide).")
        return

    erreurs = 0
    for r in rows:
        try:
            duree_min = int(r.temps_sec or 0) // 60
            envoyer_message_ntfy(r.prenom, r.type_activite, r.distance_km, duree_min)
        except Exception as e:
            erreurs += 1
            logger.warning(f"[{epoch_id}] ntfy erreur pour {r.prenom}: {e}")

    ok = len(rows) - erreurs
    logger.info(f"[{epoch_id}] ntfy envoyées: {ok} | erreurs: {erreurs}")

# =============================================================================
# 6) foreachBatch : append Delta (coalesce + maxRecordsPerFile) → notifications
# =============================================================================

def ecrire_delta_et_notifier(batch_df: DataFrame, epoch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    out = batch_df.coalesce(COALESCE_N)  # moins de fichiers → plus gros

    (out.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("maxRecordsPerFile", str(MAX_RECS_FILE))
        .save(DELTA_PATH))

    logger.info(f"[{epoch_id}] batch écrit dans Delta (coalesce={COALESCE_N}, maxRecordsPerFile={MAX_RECS_FILE}).")
    notifier_toutes_lignes(out, epoch_id)

query = (
    dedup.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .foreachBatch(ecrire_delta_et_notifier)
    .start()
)

logger.info("Streaming démarré (dedup uid → Delta coalescé → ntfy toutes lignes).")
query.awaitTermination()
