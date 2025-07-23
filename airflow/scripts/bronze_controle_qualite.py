# ======================================================================================
# Script      : bronze_controle_qualite.py
# Objectif    : Contrôle qualité des activités (bronze/Delta), export des erreurs et ntfy
# Auteur      : Xavier Rousseau | Version robuste corrigée – Juillet 2025
# ======================================================================================

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from minio import Minio

# ======================================================================================
# 1. Chargement configuration .env
# ======================================================================================

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

# ======================================================================================
# 2. Pipeline principal compatible Airflow
# ======================================================================================

def controle_qualite():
    logger.info("🚀 Initialisation SparkSession pour contrôle qualité")

    try:
        spark = SparkSession.builder \
            .appName("Contrôle Qualité Activités Sportives") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        logger.info("✅ SparkSession instanciée avec succès")
    except Exception as e:
        logger.error(f"❌ Échec d’instanciation Spark : {e}")
        raise

    spark.sparkContext.setLogLevel("WARN")

    # Vérification existence du dossier Delta
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(DELTA_PATH)
    if not fs.exists(path):
        logger.error(f"❌ Le répertoire Delta '{DELTA_PATH}' n'existe pas dans MinIO.")
        spark.stop()
        raise FileNotFoundError(f"Delta path not found: {DELTA_PATH}")

    # Lecture Delta
    try:
        df = spark.read.format("delta").load(DELTA_PATH)
        logger.success(f"✅ Données chargées depuis : {DELTA_PATH}")
    except Exception as e:
        logger.error(f"❌ Erreur lecture Delta : {e}")
        spark.stop()
        raise

    # Règles qualité
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

    # Export Excel dans MinIO
    if nb_erreurs > 0:
        try:
            df_erreurs.toPandas().to_excel(EXPORT_PATH, index=False)
            minio = Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
            minio.fput_object("datalake", EXPORT_KEY, EXPORT_PATH)
            logger.success(f"📤 Erreurs exportées dans MinIO : {EXPORT_KEY}")
        except Exception as e:
            logger.error(f"❌ Erreur export MinIO : {e}")

    # Notification ntfy
    try:
        msg = (
            f"❌ Contrôle qualité : {nb_erreurs} erreur(s) détectée(s)."
            if nb_erreurs > 0
            else f"✅ Contrôle qualité OK : {nb_valides} lignes valides."
        )
        requests.post(NTFY_URL, data=msg.encode("utf-8"))
        logger.info(f"🔔 Notification ntfy envoyée : {msg}")
    except Exception as e:
        logger.warning(f"⚠️ Erreur envoi notification ntfy : {e}")

    # Aperçu console
    if nb_erreurs > 0:
        logger.info("🔍 Exemples d'erreurs détectées :")
        df_erreurs.show(10, truncate=False)

    spark.stop()

# ======================================================================================
# 3. Lancement DAG ou CLI
# ======================================================================================

def main():
    controle_qualite()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        import sys
        sys.exit(1)
