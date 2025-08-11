# ==========================================================================================
# Script      : etape_02_init_delta_table.py
# Objectif    : Initialiser la table Delta Lake (bronze/activites_sportives)
#               avec le schéma correct, notamment le champ 'date_debut' en TimestampType.
# Auteur      : Xavier Rousseau | Août 2025
# ==========================================================================================

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, TimestampType
from loguru import logger

# ==========================================================================================
# 1. Chargement des variables d'environnement (.env)
# ==========================================================================================

load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

DELTA_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

logger.info(f"Delta Path : {DELTA_PATH}")

# ==========================================================================================
# 2. Initialisation de la SparkSession avec accès MinIO (S3A)
# ==========================================================================================

spark = (
    SparkSession.builder
    .appName("InitDeltaTableActivitesSportives")
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
# 3. Schéma de la table Delta 
# ==========================================================================================

schema = StructType([
    StructField("uid", StringType()),
    StructField("id_salarie", LongType()),
    StructField("nom", StringType()),
    StructField("prenom", StringType()),
    StructField("date", StringType()),
    StructField("jour", StringType()),
    StructField("date_debut", TimestampType()),  
    StructField("type_activite", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("temps_sec", IntegerType()),
    StructField("commentaire", StringType())
])

# ==========================================================================================
# 4. Création d'un DataFrame vide avec ce schéma
# ==========================================================================================

df_vide = spark.createDataFrame([], schema)

# ==========================================================================================
# 5. Sauvegarde dans Delta Lake (mode overwrite pour forcer la structure)
# ==========================================================================================

logger.warning("📁 Initialisation du dossier Delta Lake (vide mais avec schéma correct)...")
df_vide.write.format("delta").mode("overwrite").save(DELTA_PATH)
logger.success("✅ Table Delta Lake initialisée avec succès.")

spark.stop()
