# ==========================================================================================
# Script      : controle_qualite_activites.py
# Objectif    : Contrôle qualité sur les activités sportives (Delta Lake sur MinIO),
#               avec export des erreurs dans MinIO et notification ntfy.
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from minio import Minio

# ==========================================================================================
# 1. Chargement configuration .env
# ==========================================================================================
load_dotenv(dotenv_path=".env")

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_ENDPOINT = f"{MINIO_HOST}:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
DELTA_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://datalake/bronze/activites_sportives/")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata.qualite")
NTFY_URL = os.getenv("NTFY_URL", f"http://localhost:8080/{NTFY_TOPIC}")

EXPORT_KEY = "exports/erreurs_qualite_activites.xlsx"
EXPORT_PATH = "/tmp/erreurs_qualite_activites.xlsx"

# ==========================================================================================
# 2. Initialisation Spark (Delta + MinIO)
# ==========================================================================================
logger.info("🚀 Initialisation SparkSession pour contrôle qualité")

spark = SparkSession.builder \
    .appName("Contrôle Qualité Activités Sportives") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 3. Lecture des données Delta
# ==========================================================================================
try:
    df = spark.read.format("delta").load(DELTA_PATH)
    logger.success(f"✅ Données chargées depuis : {DELTA_PATH}")
except Exception as e:
    logger.error(f"❌ Erreur lecture Delta : {e}")
    spark.stop()
    raise

# ==========================================================================================
# 4. Définition des règles qualité (distance > 0, temps > 0, champs non nuls)
# ==========================================================================================
regles = [
    ("distance_m > 0", col("distance_m") > 0),
    ("temps_s > 0", col("temps_s") > 0),
    ("sport_type NOT NULL", col("sport_type").isNotNull()),
    ("id_salarie NOT NULL", col("id_salarie").isNotNull())
]

df_valide = df
df_erreurs = spark.createDataFrame([], df.schema)

# ==========================================================================================
# 5. Application des règles
# ==========================================================================================
for nom_regle, condition in regles:
    violations = df_valide.filter(~condition)
    nb = violations.count()
    if nb > 0:
        logger.warning(f"❌ {nb} violation(s) : {nom_regle}")
        df_erreurs = df_erreurs.union(violations)
        df_valide = df_valide.filter(condition)
    else:
        logger.info(f"✅ Règle OK : {nom_regle}")

nb_valides = df_valide.count()
nb_erreurs = df_erreurs.count()

# ==========================================================================================
# 6. Résumé et export des erreurs
# ==========================================================================================
logger.info("📊 Résumé des contrôles qualité :")
logger.info(f"✔️  Lignes valides   : {nb_valides}")
logger.info(f"❌ Lignes en erreur : {nb_erreurs}")

if nb_erreurs > 0:
    try:
        df_erreurs.toPandas().to_excel(EXPORT_PATH, index=False)
        minio = Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        minio.fput_object("datalake", EXPORT_KEY, EXPORT_PATH)
        logger.success(f"📤 Erreurs exportées dans MinIO : {EXPORT_KEY}")
    except Exception as e:
        logger.error(f"Erreur export vers MinIO : {e}")

# ==========================================================================================
# 7. Notification ntfy
# ==========================================================================================
try:
    msg = (
        f"❌ Contrôle qualité : {nb_erreurs} erreur(s) détectée(s)."
        if nb_erreurs > 0
        else f"✅ Contrôle qualité OK : {nb_valides} lignes valides."
    )
    requests.post(NTFY_URL, data=msg.encode("utf-8"))
    logger.info(f"🔔 Notification ntfy envoyée : {msg}")
except Exception as e:
    logger.warning(f"⚠️ Erreur envoi ntfy : {e}")

# ==========================================================================================
# 8. Aperçu des erreurs
# ==========================================================================================
if nb_erreurs > 0:
    logger.info("🔍 Exemples d'erreurs détectées :")
    df_erreurs.show(10, truncate=False)

spark.stop()
