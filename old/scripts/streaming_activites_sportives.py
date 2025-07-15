# ==========================================================================================
# Script      : streaming_activites_sportives.py
# Objectif    : Lire les messages Kafka Redpanda en streaming (CDC Debezium),
#               extraire les insertions (op="c"), parser le champ "after"
#               et sauvegarder en continu dans MinIO (Delta Lake).
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================
load_dotenv(dotenv_path=".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC = "sportdata.sportdata.activites_sportives"

MINIO_ENDPOINT = os.getenv("MINIO_HOST", "sport-minio") + ":9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
DELTA_OUTPUT_PATH = "s3a://datalake/bronze/activites_sportives/"

# ==========================================================================================
# 2. Initialisation SparkSession
# ==========================================================================================
spark = SparkSession.builder \
    .appName("Streaming Activités Sportives") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 3. Définir le schéma du champ "after" (contenu inséré)
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
# 4. Lecture en streaming du topic Kafka Redpanda
# ==========================================================================================
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# ==========================================================================================
# 5. Extraction des messages 'op = c' (insertions uniquement)
# ==========================================================================================
df_parsed = df_json \
    .withColumn("json_data", from_json(col("json_str"), "STRUCT<before: STRING, after: STRING, op: STRING, ts_ms: BIGINT>")) \
    .filter("json_data.op = 'c' AND json_data.after IS NOT NULL") \
    .withColumn("after", from_json(col("json_data.after"), after_schema)) \
    .select("after.*")

# ==========================================================================================
# 6. Écriture continue dans MinIO (Delta Lake append)
# ==========================================================================================
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/activites_sportives") \
    .start(DELTA_OUTPUT_PATH)

query.awaitTermination()
