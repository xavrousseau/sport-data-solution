# ==========================================================================================
# Script      : verifier_donnees_delta.py
# Objectif    : Lire un dossier Delta Lake dans MinIO et vérifier son contenu :
#               schéma, volume, exemples, statistiques simples.
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
import os
from dotenv import load_dotenv

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================
load_dotenv(dotenv_path=".env")

MINIO_ENDPOINT = os.getenv("MINIO_HOST", "sport-minio") + ":9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

DELTA_INPUT_PATH = "s3a://datalake/bronze/activites_sportives/"

# ==========================================================================================
# 2. Initialisation SparkSession avec support Delta Lake & S3
# ==========================================================================================
spark = SparkSession.builder \
    .appName("Vérification Données Delta") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 3. Lecture du dossier Delta depuis MinIO
# ==========================================================================================
try:
    df = spark.read.format("delta").load(DELTA_INPUT_PATH)
    print("✅ Lecture Delta réussie")
except Exception as e:
    print(f"❌ Erreur lecture Delta : {e}")
    spark.stop()
    exit(1)

# ==========================================================================================
# 4. Audit rapide : schéma, volume, stats
# ==========================================================================================
print("\n📌 Schéma du DataFrame :")
df.printSchema()

print("\n📊 Nombre total de lignes :", df.count())

print("\n🏅 Activités par type :")
df.groupBy("sport_type").agg(count("*").alias("nb")).orderBy(col("nb").desc()).show(truncate=False)

print("\n👥 Top 5 salariés les plus actifs :")
df.groupBy("id_salarie").agg(count("*").alias("nb")).orderBy(col("nb").desc()).show(5, truncate=False)

print("\n👀 Aperçu des premières lignes :")
df.show(10, truncate=False)

spark.stop()
