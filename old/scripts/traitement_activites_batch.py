v# ==========================================================================================
# Script      : traitement_activites_batch.py
# Objectif    : Lire les messages Debezium CDC depuis Redpanda,
#               extraire les insertions (op="c"), parser le champ "after"
#               et sauvegarder les données dans MinIO en Delta Lake (batch).
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
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
# 2. Initialisation SparkSession avec support Delta Lake & S3 (MinIO)
# ==========================================================================================
spark = SparkSession.builder \
    .appName("Traitement Activites Sportives (Batch)") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 3. Schéma attendu dans le champ 'after' des messages Debezium
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
# 4. Lecture des messages JSON depuis Redpanda (Kafka)
# ==========================================================================================
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# ==========================================================================================
# 5. Extraction des messages op = "c" (insert) et du champ "after"
# ==========================================================================================
df_parsed = df_json \
    .withColumn("json_data", from_json(col("json_str"), "STRUCT<before: STRING, after: STRING, op: STRING, ts_ms: BIGINT>")) \
    .filter("json_data.op = 'c' AND json_data.after IS NOT NULL") \
    .withColumn("after_json", from_json(col("json_data.after"), after_schema)) \
    .select("after_json.*")

# ==========================================================================================
# 6. Sauvegarde en Delta Lake dans MinIO
# ==========================================================================================
df_parsed.write \
    .format("delta") \
    .mode("overwrite") \
    .save(DELTA_OUTPUT_PATH)

print("✅ Export terminé dans MinIO (Delta Lake)")
spark.stop()
