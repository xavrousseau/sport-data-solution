# ==========================================================================================
# Script      : export_powerbi.py
# Objectif    : Consolider les activités sportives (Delta Lake)
#               en calculant : nb d’activités, jours bien-être, distances, durée, primes,
#               puis exporter les résultats dans MinIO (CSV).
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count, sum, min, max, to_date, expr
import os
from dotenv import load_dotenv
import pandas as pd
from minio import Minio

# ==========================================================================================
# 1. Chargement configuration et accès MinIO
# ==========================================================================================
load_dotenv(dotenv_path=".env")

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_ENDPOINT = MINIO_HOST + ":9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
DELTA_PATH = "s3a://datalake/bronze/activites_sportives/"
EXPORT_CSV_PATH = "/tmp/avantages_sportifs_export.csv"
EXPORT_MINIO_KEY = "exports/avantages_sportifs_export.csv"

# ==========================================================================================
# 2. Initialisation SparkSession
# ==========================================================================================
spark = SparkSession.builder \
    .appName("Export Avantages Sportifs - Power BI") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 3. Lecture des activités sportives
# ==========================================================================================
df = spark.read.format("delta").load(DELTA_PATH)
df = df.withColumn("date", to_date(col("date_debut")))

# ==========================================================================================
# 4. Agrégation par salarié
# ==========================================================================================
df_agg = df.groupBy("id_salarie") \
    .agg(
        count("*").alias("nb_activites"),
        countDistinct("date").alias("jours_bien_etre"),
        sum("distance_m").alias("distance_totale_m"),
        sum("temps_s").alias("temps_total_s"),
        min("date").alias("premiere_activite"),
        max("date").alias("derniere_activite")
    )

# Conversion des unités
df_agg = df_agg.withColumn("distance_km", expr("ROUND(distance_totale_m / 1000.0, 2)")) \
               .withColumn("temps_minutes", expr("ROUND(temps_total_s / 60.0, 1)"))

# Règle de prime (exemple : 50€ si +15 jours bien-être)
df_agg = df_agg.withColumn("prime_sportive_eur", expr("CASE WHEN jours_bien_etre >= 15 THEN 50 ELSE 0 END"))

# ==========================================================================================
# 5. Export en CSV local puis upload dans MinIO
# ==========================================================================================
df_final = df_agg.select(
    "id_salarie", "nb_activites", "jours_bien_etre",
    "distance_km", "temps_minutes", "premiere_activite",
    "derniere_activite", "prime_sportive_eur"
)

df_final_pd = df_final.toPandas()
df_final_pd.to_csv(EXPORT_CSV_PATH, index=False)
print(f"✅ Fichier exporté localement : {EXPORT_CSV_PATH}")

client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

client.fput_object(
    bucket_name="datalake",
    object_name=EXPORT_MINIO_KEY,
    file_path=EXPORT_CSV_PATH
)

print(f"📤 Fichier exporté dans MinIO : {EXPORT_MINIO_KEY}")
spark.stop()
