# ==========================================================================================
# Script      : verifier_donnees_delta.py
# Objectif    : Vérifier le contenu d'un dossier Delta Lake dans MinIO :
#               structure, volume, stats par type d’activité, aperçu.
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

MINIO_ENDPOINT = os.getenv("MINIO_HOST", "sport-minio") + ":9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
DELTA_INPUT_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://datalake/bronze/activites_sportives/")

# ==========================================================================================
# 2. Initialisation SparkSession (Delta + MinIO S3A)
# ==========================================================================================
logger.info("🚀 Initialisation de la session Spark pour audit Delta Lake")

spark = SparkSession.builder \
    .appName("Audit Données Delta") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 3. Lecture Delta Lake
# ==========================================================================================
try:
    logger.info(f"📦 Lecture des données Delta depuis : {DELTA_INPUT_PATH}")
    df = spark.read.format("delta").load(DELTA_INPUT_PATH)
    logger.success("✅ Lecture Delta réussie")
except Exception as e:
    logger.error(f"❌ Erreur lecture Delta : {e}")
    spark.stop()
    exit(1)

# ==========================================================================================
# 4. Audit des données (structure + résumé)
# ==========================================================================================
logger.info("🔍 Schéma du DataFrame Delta :")
df.printSchema()

logger.info(f"📊 Nombre total de lignes : {df.count()}")

logger.info("🏅 Activités sportives par type :")
df.groupBy("sport_type").agg(count("*").alias("nb")).orderBy(col("nb").desc()).show(truncate=False)

logger.info("👥 Top 5 des salariés les plus actifs :")
df.groupBy("id_salarie").agg(count("*").alias("nb")).orderBy(col("nb").desc()).show(5, truncate=False)

logger.info("👀 Aperçu des premières lignes du DataFrame :")
df.show(10, truncate=False)

spark.stop()
