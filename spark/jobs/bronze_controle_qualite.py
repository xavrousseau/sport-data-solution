# ======================================================================================
# Script      : bronze_controle_qualite.py
# Objectif    : Contrôle qualité des activités sportives (bronze / Delta Lake)
#               - Vérifie plusieurs règles de cohérence métier
#               - Exporte les erreurs dans MinIO (Excel)
#               - Envoie un message de notification ntfy
# Auteur      : Xavier Rousseau | Juillet 2025
# ======================================================================================

import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from minio import Minio
from delta import configure_spark_with_delta_pip

# ======================================================================================
# 1. Chargement configuration depuis le fichier .env
# ======================================================================================

load_dotenv(dotenv_path=".env")

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_ENDPOINT = f"{MINIO_HOST}:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
DELTA_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/resultats/")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata_activites")
NTFY_URL = os.getenv("NTFY_URL", f"http://localhost:8080/{NTFY_TOPIC}")

EXPORT_KEY = "exports/erreurs_qualite_activites.xlsx"
EXPORT_PATH = "/tmp/erreurs_qualite_activites.xlsx"

# ======================================================================================
# 2. Fonction principale du contrôle qualité
# ======================================================================================

def controle_qualite():
    logger.info("🚀 Initialisation de SparkSession avec support Delta + S3A")

    builder = SparkSession.builder \
        .appName("Contrôle Qualité Activités Sportives") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("✅ SparkSession instanciée avec succès")
    except Exception as e:
        logger.error(f"❌ Échec d’instanciation Spark : {e}")
        raise

    spark.sparkContext.setLogLevel("WARN")

    # Lecture des données Delta depuis MinIO
    try:
        logger.info(f"📂 Lecture des données Delta : {DELTA_PATH}")
        df = spark.read.format("delta").load(DELTA_PATH)
        logger.success(f"✅ Données chargées depuis : {DELTA_PATH}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la lecture Delta : {e}")
        spark.stop()
        raise

    # Règles métier à appliquer
    regles = [
        ("distance_km > 0", col("distance_km") > 0),
        ("temps_sec > 0", col("temps_sec") > 0),
        ("type_activite NOT NULL", col("type_activite").isNotNull()),
        ("id_salarie NOT NULL", col("id_salarie").isNotNull())
    ]

    df_valide = df
    df_erreurs = spark.createDataFrame([], df.schema)

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

    logger.info("📊 Résumé du contrôle qualité :")
    logger.info(f"✔️ Lignes valides   : {nb_valides}")
    logger.info(f"❌ Lignes en erreur : {nb_erreurs}")

    # Export des erreurs en Excel vers MinIO
    if nb_erreurs > 0:
        try:
            logger.info("📤 Export des erreurs au format Excel...")
            df_erreurs.toPandas().to_excel(EXPORT_PATH, index=False)

            minio = Minio(
                MINIO_HOST,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
            minio.fput_object("datalake", EXPORT_KEY, EXPORT_PATH)
            logger.success(f"✅ Erreurs exportées dans MinIO : {EXPORT_KEY}")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l’export Excel vers MinIO : {e}")

    # Notification via ntfy
    try:
        message = (
            f"❌ Contrôle qualité : {nb_erreurs} erreur(s) détectée(s)."
            if nb_erreurs > 0
            else f"✅ Contrôle qualité OK : {nb_valides} lignes valides."
        )
        requests.post(NTFY_URL, data=message.encode("utf-8"))
        logger.info(f"🔔 Notification ntfy envoyée : {message}")
    except Exception as e:
        logger.warning(f"⚠️ Échec d’envoi de notification ntfy : {e}")

    if nb_erreurs > 0:
        logger.info("🔍 Exemples d’erreurs détectées :")
        df_erreurs.show(10, truncate=False)

    spark.stop()

# ======================================================================================
# 3. Lancement direct (CLI ou Airflow)
# ======================================================================================

def main():
    controle_qualite()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
