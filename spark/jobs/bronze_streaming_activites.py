# ==========================================================================================
# Script      : bronze_streaming_activites.py
# Objectif    : Lire les activités sportives depuis Kafka (Debezium),
#               envoyer des messages ntfy enrichis pour chaque activité,
#               et stocker les activités dans Delta Lake (bronze).
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import sys
import time
from dotenv import load_dotenv
from loguru import logger
from random import choice
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

# ✅ Ajout du chemin contenant ntfy_helper.py
sys.path.append("/opt/airflow/scripts")
from ntfy_helper import envoyer_message_ntfy  # ✅ Appel centralisé depuis helper

# ==========================================================================================
# 1. Chargement des variables d’environnement (.env)
# ==========================================================================================

load_dotenv(dotenv_path=".env", override=True)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata_activites")
logger.info(f"📱 Kafka Bootstrap Servers : {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"📱 Topic Kafka configuré     : {KAFKA_TOPIC}")

NTFY_URL = os.getenv("NTFY_URL", "http://sport-ntfy")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata_activites")
logger.info(f"🔔 URL ntfy                  : {NTFY_URL}")
logger.info(f"🔔 Topic ntfy configuré      : {NTFY_TOPIC}")

DELTA_PATH_ACTIVITES = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/activites_sportives")
logger.info(f"📁 Chemin Delta Lake         : {DELTA_PATH_ACTIVITES}")

CHECKPOINT_PATH_DELTA = os.getenv("CHECKPOINT_PATH_DELTA", "/tmp/checkpoints/bronze_activites_sportives")
CHECKPOINT_PATH_NTFY = os.getenv("CHECKPOINT_PATH_NTFY", "/tmp/checkpoints/ntfy_activites")
logger.info(f"🗒️ Checkpoint Delta          : {CHECKPOINT_PATH_DELTA}")
logger.info(f"🗒️ Checkpoint NTFY           : {CHECKPOINT_PATH_NTFY}")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
logger.info(f"🪣 MinIO endpoint            : {MINIO_ENDPOINT}")
logger.info(f"🪣 MinIO access key          : {MINIO_ACCESS_KEY}")

if not KAFKA_BOOTSTRAP_SERVERS or not KAFKA_TOPIC:
    logger.warning("⚠️ Configuration Kafka incomplète. Vérifie les variables .env.")
if not NTFY_URL or not NTFY_TOPIC:
    logger.warning("⚠️ Configuration ntfy incomplète. Vérifie les variables .env.")
if not DELTA_PATH_ACTIVITES:
    logger.warning("⚠️ Chemin Delta Lake non défini.")
if not MINIO_ENDPOINT or not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    logger.warning("⚠️ Configuration MinIO incomplète. Vérifie les variables .env.")

# ==========================================================================================
# 2. Fonction de traitement par microbatch : notification pour chaque activité
# ==========================================================================================

def traiter_batch(df, epoch_id):
    logger.info(f"📦 Batch {epoch_id} reçu avec {df.count()} lignes")
    if df.isEmpty():
        logger.info(f"[Batch {epoch_id}] Aucun événement à traiter.")
        return

    lignes = df.select("prenom", "type_activite", "distance_km", "temps_sec").collect()
    for row in lignes:
        try:
            envoyer_message_ntfy(row.prenom, row.type_activite, row.distance_km, row.temps_sec // 60)
        except Exception as e:
            logger.warning(f"⚠️ Erreur traitement activité : {e}")
    df.select("prenom", "type_activite", "distance_km", "temps_sec").show(truncate=False)

# ==========================================================================================
# 3. Schéma JSON attendu (champ "after" de Debezium)
# ==========================================================================================

schema = StructType([
    StructField("uid", StringType()),
    StructField("id_salarie", LongType()),
    StructField("nom", StringType()),
    StructField("prenom", StringType()),
    StructField("date", StringType()),
    StructField("jour", StringType()),
    StructField("type_activite", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("temps_sec", IntegerType()),
    StructField("commentaire", StringType())
])

# ==========================================================================================
# 4. Initialisation Spark avec Delta + Kafka + S3A
# ==========================================================================================

logger.info("🚀 Initialisation SparkSession pour Kafka + Delta + S3A")

spark = SparkSession.builder \
    .appName("StreamingActivitesSportives") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("✅ SparkSession initialisée")

# ==========================================================================================
# 5. Lecture Kafka, parsing JSON, enrichissement
# ==========================================================================================

logger.info(f"📱 Lecture Kafka : topic = {KAFKA_TOPIC}")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

df_valeurs = kafka_df.selectExpr("CAST(value AS STRING)")

df_json = df_valeurs.select(
    from_json(col("value"), StructType().add("after", StringType())).alias("data")
).filter("data.after IS NOT NULL")

df_activites = df_json.select(
    from_json(col("data.after"), schema).alias("activite")
).select("activite.*")

df_activites = df_activites.withColumn("date_debut", col("date"))

df_activites.printSchema()
if df_activites.isStreaming:
    logger.info("✅ DataFrame en streaming actif")
else:
    logger.warning("❌ DataFrame n'est pas en streaming")

# ==========================================================================================
# 6. Notifications via foreachBatch
# ==========================================================================================

logger.info("🔔 Activation du flux NTFY")

query_ntfy = df_activites.writeStream \
    .foreachBatch(traiter_batch) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH_NTFY) \
    .start()

# ==========================================================================================
# 7. Écriture dans Delta Lake (bronze)
# ==========================================================================================

logger.info(f"📂 Écriture Delta Lake : {DELTA_PATH_ACTIVITES}")

query_delta = df_activites.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("path", DELTA_PATH_ACTIVITES) \
    .option("checkpointLocation", CHECKPOINT_PATH_DELTA) \
    .start()

for _ in range(10):
    if query_delta.lastProgress:
        logger.info(f"🔄 Progrès Delta : {query_delta.lastProgress}")
        break
    time.sleep(1)
else:
    logger.warning("⚠️ Aucun progrès d’écriture détecté après 10 secondes")

# ==========================================================================================
# 8. Attente de terminaison des streams
# ==========================================================================================

query_ntfy.awaitTermination()
query_delta.awaitTermination()
