# ==========================================================================================
# Script      : diagnostic_delta_minio_v2.py
# Objectif    : Lire directement le Delta Lake sur MinIO et afficher le contenu.
# ==========================================================================================

import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from loguru import logger

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================

load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

DELTA_PATH_ACTIVITES = os.getenv("DELTA_PATH_ACTIVITES").rstrip("/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

logger.info(f"Delta Path  : {DELTA_PATH_ACTIVITES}")

# ==========================================================================================
# 2. Initialisation SparkSession (S3A MinIO / Delta Lake)
# ==========================================================================================

spark = (
    SparkSession.builder
    .appName("DiagnosticDeltaMinIO")
    .config("spark.jars.packages", ",".join([
        "io.delta:delta-core_2.12:2.4.0",
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
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 3. Tentative de lecture Delta Lake directe
# ==========================================================================================

try:
    logger.info("Lecture du Delta Lake en cours...")
    df = spark.read.format("delta").load(DELTA_PATH_ACTIVITES)
    logger.success(f"✅ Lecture réussie — Nombre de lignes : {df.count()}")
    df.printSchema()
    df.show(10, truncate=False)
except Exception as e:
    logger.error(f"❌ Erreur lecture Delta : {e}")

# ==========================================================================================
# FIN DU SCRIPT
# ==========================================================================================
