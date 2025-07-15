# ==========================================================================================
# Script      : traitement_activites_batch.py
# Objectif    : Lire les messages Debezium (op="c") depuis Redpanda, parser le champ 'after',
#               appliquer un filtre qualité, et sauvegarder en Delta Lake sur MinIO.
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# ==========================================================================================
# 1. Chargement configuration depuis .env
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata.sportdata.activites_sportives")

MINIO_ENDPOINT = os.getenv("MINIO_HOST", "sport-minio") + ":9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
DELTA_OUTPUT_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://datalake/bronze/activites_sportives/")
DELTA_WRITE_MODE = os.getenv("DELTA_WRITE_MODE", "overwrite")

# ==========================================================================================
# 2. Initialisation Spark avec support Delta Lake + MinIO (S3A)
# ==========================================================================================
spark = SparkSession.builder \
    .appName("Batch | Traitement Activités Sportives") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 3. Schéma du champ 'after' (contenu réel d'une activité)
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
# 4. Pipeline principal
# ==========================================================================================
try:
    logger.info("🚀 Lecture des messages Kafka depuis Redpanda...")
    df_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    df_json = df_raw.selectExpr("CAST(value AS STRING) AS json_str")

    logger.info("🔍 Extraction des messages op='c' (insertions)...")
    df_parsed = df_json \
        .withColumn("json_data", from_json(col("json_str"),
                                           "STRUCT<before: STRING, after: STRING, op: STRING, ts_ms: BIGINT>")) \
        .filter("json_data.op = 'c' AND json_data.after IS NOT NULL") \
        .withColumn("after_json", from_json(col("json_data.after"), after_schema)) \
        .select("after_json.*")

    logger.info(f"🧼 Application des filtres qualité : distance_m > 0, temps_s > 0, sport_type non nul")
    df_filtre = df_parsed \
        .filter("distance_m > 0 AND temps_s > 0 AND sport_type IS NOT NULL")

    logger.info(f"💾 Sauvegarde Delta Lake dans : {DELTA_OUTPUT_PATH} (mode={DELTA_WRITE_MODE})")
    df_filtre.write \
        .format("delta") \
        .mode(DELTA_WRITE_MODE) \
        .save(DELTA_OUTPUT_PATH)

    logger.success(f"✅ Export Delta Lake terminé. Nombre d’activités : {df_filtre.count()}")

except Exception as e:
    logger.error(f"❌ Erreur Spark batch : {e}")
    raise

finally:
    spark.stop()
