# ==========================================================================================
# Script      : streaming_activites_sportives.py
# Objectif    : Lecture continue des activités sportives via Kafka Redpanda (Debezium),
#               extraction du champ 'after', filtre qualité, stockage Delta Lake MinIO.
# Auteur      : Xavier Rousseau |  Juillet 2025
# ==========================================================================================

import os
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata.sportdata.activites_sportives")

MINIO_ENDPOINT = os.getenv("MINIO_HOST", "sport-minio") + ":9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

DELTA_STREAM_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://datalake/bronze/activites_sportives/")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH_STREAMING", "/tmp/spark_checkpoints/activites_sportives")

# ==========================================================================================
# 2. Initialisation SparkSession (Delta Lake + S3A MinIO)
# ==========================================================================================
logger.info("🌀 Initialisation de la session Spark streaming...")

spark = SparkSession.builder \
    .appName("Streaming Activités Sportives") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 3. Définition du schéma de l'objet JSON 'after'
# ==========================================================================================
after_schema = StructType([
    StructField("id_salarie", StringType()),
    StructField("date_debut", StringType()),
    StructField("sport_type", StringType()),
    StructField("distance_m", IntegerType()),
    StructField("temps_s", IntegerType()),
    StructField("commentaire", StringType())
])

# ==========================================================================================
# 4. Lecture en streaming depuis Redpanda Kafka (Debezium)
# ==========================================================================================
try:
    logger.info(f"📡 Connexion au topic Kafka : {KAFKA_TOPIC}")
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_json = df_raw.selectExpr("CAST(value AS STRING) AS json_str")

    # ======================================================================================
    # 5. Extraction des événements INSERT (op = 'c') + parsing du champ 'after'
    # ======================================================================================
    df_parsed = df_json \
        .withColumn("json_data", from_json(col("json_str"),
                                           "STRUCT<before: STRING, after: STRING, op: STRING, ts_ms: BIGINT>")) \
        .filter("json_data.op = 'c' AND json_data.after IS NOT NULL") \
        .withColumn("after_struct", from_json(col("json_data.after"), after_schema)) \
        .select("after_struct.*")

    # ======================================================================================
    # 6. Filtres qualité (distance et temps positifs, sport renseigné)
    # ======================================================================================
    df_filtre = df_parsed \
        .filter("distance_m > 0 AND temps_s > 0 AND sport_type IS NOT NULL")

    logger.info("🔄 Streaming lancé : append vers Delta Lake + checkpointing")

    # ======================================================================================
    # 7. Écriture continue dans MinIO au format Delta
    # ======================================================================================
    query = df_filtre.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start(DELTA_STREAM_PATH)

    logger.success(f"✅ Flux actif vers Delta Lake : {DELTA_STREAM_PATH}")
    query.awaitTermination()

except Exception as e:
    logger.error(f"❌ Erreur Spark streaming : {e}")
    raise
