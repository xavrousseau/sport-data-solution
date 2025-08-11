# ==========================================================================================
# Script      : bronze_streaming_activites.py (fonctionnel et commenté)
# Objectif    : Ingestion Kafka (Debezium JSON) → Delta Lake sur MinIO S3A,
#               dédoublonnage streaming sur 'uid', notification ntfy,
#               logs pédagogiques.
# Auteur      : Xavier Rousseau | Août 2025
# ==========================================================================================

import os
import sys
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, TimestampType

# ==========================================================================================
# 1. Configuration & helpers
# ==========================================================================================

# Ajout du chemin helper ntfy (à adapter selon ton infra)
sys.path.append("/opt/airflow/scripts")
from ntfy_helper import envoyer_message_ntfy

# Chargement des variables d'environnement (.env)
load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
DELTA_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/")
CHECKPOINT_PATH_DELTA = "/tmp/checkpoints/bronze_activites_sportives"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

logger.info(f"Chemin d'écriture Delta Lake : {DELTA_PATH}")
logger.info(f"Endpoint MinIO              : {MINIO_ENDPOINT}")

# ==========================================================================================
# 2. Schéma JSON Debezium attendu pour l'activité sportive
# ==========================================================================================

schema = StructType([
    StructField("uid", StringType()),
    StructField("id_salarie", LongType()),
    StructField("nom", StringType()),
    StructField("prenom", StringType()),
    StructField("date", StringType()),
    StructField("jour", StringType()),
    StructField("date_debut", StringType()),  # Conversion après
    StructField("type_activite", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("temps_sec", IntegerType()),
    StructField("commentaire", StringType())
])

schema_debezium = StructType().add("payload", StructType([
    StructField("op", StringType()),
    StructField("after", schema),
    StructField("ts_ms", LongType())
]))

# ==========================================================================================
# 3. Initialisation SparkSession (avec options Delta Lake & MinIO)
# ==========================================================================================

logger.info("Initialisation SparkSession...")
spark = (
    SparkSession.builder
    .appName("StreamingActivitesSportives")
    .config("spark.jars.packages", ",".join([
        "io.delta:delta-core_2.12:2.4.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        "org.apache.kafka:kafka-clients:3.4.0",
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    ]))
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 4. Lecture Kafka, parsing JSON Debezium, nettoyage & anti-doublon streaming
# ==========================================================================================

logger.info(f"Lecture du topic Kafka : {KAFKA_TOPIC}")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Extraction du JSON value
df_valeurs = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
df_parsed = df_valeurs.select(from_json(col("json_str"), schema_debezium).alias("record"))

# Nettoyage + conversion date_debut
df_clean = (
    df_parsed.filter(col("record.payload.op").isin("c", "u"))
    .select("record.payload.after.*")
    .withColumn("date_debut", to_timestamp("date_debut"))
)

# =======================
# 5. Anti-doublon streaming
# =======================
# Avec watermark sur date_debut : on ne garde qu'un event unique par 'uid' dans la fenêtre 1h
df_watermarked = df_clean.withWatermark("date_debut", "1 hour")
df_dedup = df_watermarked.dropDuplicates(["uid"])

# ==========================================================================================
# 6. Fonction de notification ntfy (exécutée après écriture Delta Lake)
# ==========================================================================================

def traiter_batch_ntfy(df: DataFrame, epoch_id: int):
    count = df.count()
    logger.info(f"📦 Batch {epoch_id} écrit dans Delta : {count} activité(s)")
    if count == 0:
        logger.warning(f"⚠️ Batch {epoch_id} vide (aucune notification ntfy envoyée).")
        return

    erreurs = 0
    lignes = df.select("prenom", "type_activite", "distance_km", "temps_sec").collect()
    for row in lignes:
        try:
            if row.temps_sec is None:
                logger.warning(f"⛔ temps_sec manquant pour {row.prenom} ({row.type_activite}) — notification ignorée")
                erreurs += 1
                continue
            duree_min = max(int(row.temps_sec) // 60, 0)
            envoyer_message_ntfy(row.prenom, row.type_activite, row.distance_km, duree_min)
        except Exception as e:
            logger.error(f"⚠️ Erreur notification pour {row} : {e}")
            erreurs += 1
    if erreurs > 0:
        logger.warning(f"⚠️ {erreurs} notification(s) ntfy n'ont pas pu être envoyées.")

# ==========================================================================================
# 7. Pipeline principal : foreachBatch pour garantir notification post-écriture
# ==========================================================================================

def ecriture_et_ntfy(batch_df, epoch_id):
    """
    Pour chaque micro-batch :
      1. Écriture append dans Delta Lake (sur MinIO/S3A)
      2. Notifications ntfy uniquement si l'écriture a réussi
    """
    if batch_df.count() == 0:
        logger.info(f"Batch {epoch_id} vide, rien à écrire ni notifier.")
        return
    try:
        batch_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(DELTA_PATH)
        logger.info(f"✅ Batch {epoch_id} écrit dans Delta Lake ({DELTA_PATH})")
        traiter_batch_ntfy(batch_df, epoch_id)
    except Exception as e:
        logger.error(f"⛔ Erreur lors de l'écriture du batch {epoch_id} dans Delta/MinIO : {e}")
        # Optionnel : tu pourrais envoyer ici une alerte critique ntfy/mail/Slack en cas d'échec majeur

# Démarrage du streaming
query_delta = (
    df_dedup.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH_DELTA)
    .trigger(processingTime="30 seconds")
    .queryName("delta_stream")
    .foreachBatch(ecriture_et_ntfy)
    .start()
)

logger.info("🟢 Pipeline streaming Delta Lake (anti-doublon + notification ntfy) démarré.")
logger.info("🟢 Tu dois voir apparaître les fichiers dans MinIO à l'adresse : " + DELTA_PATH)

# ==========================================================================================
# 8. Attente de terminaison du stream
# ==========================================================================================

query_delta.awaitTermination()
