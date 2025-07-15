# ==========================================================================================
# Script      : export_powerbi.py
# Objectif    : Agréger les données sportives par salarié pour export Power BI
# Auteur      : Xavier Rousseau | Version enrichie — Juillet 2025
# ==========================================================================================

import os
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, min, max, to_date, expr
import pandas as pd
from minio import Minio

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================
load_dotenv(dotenv_path=".env")

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_ENDPOINT = f"{MINIO_HOST}:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

DELTA_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://datalake/bronze/activites_sportives/")
EXPORT_PATH = "/tmp/avantages_sportifs_export.csv"
EXPORT_MINIO_KEY = os.getenv("POWERBI_EXPORT_TABLE", "avantages_sportifs_export")
EXPORT_MINIO_OBJECT = f"exports/{EXPORT_MINIO_KEY}.csv"

# ==========================================================================================
# 2. Initialisation Spark avec Delta Lake + MinIO
# ==========================================================================================
logger.info("🚀 Initialisation SparkSession")

spark = SparkSession.builder \
    .appName("Export Power BI - Avantages Sportifs") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 3. Lecture des activités sportives depuis Delta Lake
# ==========================================================================================
try:
    logger.info(f"📦 Lecture Delta Lake : {DELTA_PATH}")
    df = spark.read.format("delta").load(DELTA_PATH)
    df = df.withColumn("date", to_date(col("date_debut")))
except Exception as e:
    logger.error(f"❌ Erreur lecture Delta : {e}")
    spark.stop()
    raise

# ==========================================================================================
# 4. Agrégation des indicateurs par salarié
# ==========================================================================================
df_agg = df.groupBy("id_salarie").agg(
    count("*").alias("nb_activites"),
    countDistinct("date").alias("jours_bien_etre"),
    sum("distance_m").alias("distance_totale_m"),
    sum("temps_s").alias("temps_total_s"),
    min("date").alias("premiere_activite"),
    max("date").alias("derniere_activite")
)

df_agg = df_agg \
    .withColumn("distance_km", expr("ROUND(distance_totale_m / 1000.0, 2)")) \
    .withColumn("temps_minutes", expr("ROUND(temps_total_s / 60.0, 1)")) \
    .withColumn("prime_sportive_eur", expr("CASE WHEN jours_bien_etre >= 15 THEN 50 ELSE 0 END"))

df_final = df_agg.select(
    "id_salarie", "nb_activites", "jours_bien_etre",
    "distance_km", "temps_minutes", "premiere_activite",
    "derniere_activite", "prime_sportive_eur"
)

# ==========================================================================================
# 5. Export CSV local + MinIO
# ==========================================================================================
try:
    df_final_pd = df_final.toPandas()
    df_final_pd.to_csv(EXPORT_PATH, index=False)
    logger.success(f"✅ Export local CSV : {EXPORT_PATH}")

    minio = Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    minio.fput_object("datalake", EXPORT_MINIO_OBJECT, EXPORT_PATH)
    logger.success(f"📤 Export MinIO : {EXPORT_MINIO_OBJECT}")
except Exception as e:
    logger.error(f"❌ Erreur export CSV ou MinIO : {e}")

spark.stop()
