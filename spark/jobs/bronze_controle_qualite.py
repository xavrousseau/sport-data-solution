# ======================================================================================
# Script      : bronze_controle_qualite.py
# Objectif    : Contrôle qualité des activités sportives dans la zone bronze (Delta Lake)
#               - Vérifie les règles métier sur les colonnes clés
#               - Génère un export des erreurs en Excel dans MinIO
#               - Envoie une notification via ntfy
# Auteur      : Xavier Rousseau | Modifié par ChatGPT, juillet 2025
# ======================================================================================

import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType
from minio import Minio
from delta import configure_spark_with_delta_pip

# ======================================================================================
# 1. Chargement des variables d’environnement depuis .env
# ======================================================================================

load_dotenv(dotenv_path=".env")

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "sportdata")
MINIO_ENDPOINT = f"{MINIO_HOST}:9000"

DELTA_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/resultats/")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata_activites")
NTFY_URL = os.getenv("NTFY_URL", f"http://localhost:80/{NTFY_TOPIC}")

EXPORT_KEY = "exports/erreurs_qualite_activites.xlsx"
EXPORT_PATH = "/tmp/erreurs_qualite_activites.xlsx"

# ======================================================================================
# 2. Fonction principale : contrôle qualité des données Delta Lake
# ======================================================================================

def controle_qualite():
    logger.info("🚀 Initialisation de SparkSession avec support Delta Lake + S3A")

    builder = SparkSession.builder \
        .appName("Controle Qualite Activites") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)

    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("✅ SparkSession créée avec succès")
    except Exception as e:
        logger.error(f"❌ Erreur création SparkSession : {e}")
        sys.exit(1)

    spark.sparkContext.setLogLevel("WARN")

    # Schéma par défaut (si Delta vide)
    schema_activites = StructType([
        StructField("id_salarie", LongType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("temps_sec", DoubleType(), True),
        StructField("type_activite", StringType(), True),
        StructField("date", TimestampType(), True)
    ])

    try:
        logger.info(f"📥 Lecture Delta : {DELTA_PATH}")
        df = spark.read.format("delta").load(DELTA_PATH)
        logger.success(f"✅ Données chargées : {DELTA_PATH}")
    except Exception as e:
        logger.warning(f"⚠️ Données absentes. Création d’une table vide : {e}")
        df = spark.createDataFrame([], schema_activites)
        df.write.format("delta").mode("overwrite").save(DELTA_PATH)
        logger.success(f"✅ Table Delta vide initialisée dans : {DELTA_PATH}")

    # Règles de validation
    regles = [
        ("distance_km > 0", col("distance_km") > 0),
        ("temps_sec > 0", col("temps_sec") > 0),
        ("type_activite NOT NULL", col("type_activite").isNotNull()),
        ("id_salarie NOT NULL", col("id_salarie").isNotNull()),
    ]

    df_valide = df
    df_erreurs = spark.createDataFrame([], df.schema)

    for nom, condition in regles:
        violations = df_valide.filter(~condition)
        nb = violations.count()
        if nb > 0:
            logger.warning(f"❌ {nb} violation(s) : {nom}")
            df_erreurs = df_erreurs.union(violations)
            df_valide = df_valide.filter(condition)
        else:
            logger.info(f"✅ Règle OK : {nom}")

    nb_valides = df_valide.count()
    nb_erreurs = df_erreurs.count()

    logger.info("📊 Résumé du contrôle qualité :")
    logger.info(f"✔️ Valides : {nb_valides} ligne(s)")
    logger.info(f"❌ Erreurs : {nb_erreurs} ligne(s)")

    # ======================================================================================
    # 3. Export des erreurs vers Excel et MinIO (si erreurs détectées)
    # ======================================================================================
    if nb_erreurs > 0:
        try:
            df_erreurs.toPandas().to_excel(EXPORT_PATH, index=False)

            minio = Minio(
                MINIO_HOST,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
            minio.fput_object(MINIO_BUCKET, EXPORT_KEY, EXPORT_PATH)
            logger.success(f"📁 Fichier d’erreurs exporté dans MinIO : {EXPORT_KEY}")
        except Exception as e:
            logger.error(f"❌ Export erreurs MinIO échoué : {e}")

    # ======================================================================================
    # 4. Notification via ntfy
    # ======================================================================================
    try:
        message = (
            f"❌ Qualité NOK : {nb_erreurs} erreur(s) détectée(s)."
            if nb_erreurs > 0
            else f"✅ Qualité OK : {nb_valides} ligne(s) valides."
        )
        requests.post(NTFY_URL, data=message.encode("utf-8"))
        logger.info(f"🔔 Notification envoyée : {message}")
    except Exception as e:
        logger.warning(f"⚠️ Erreur envoi notification ntfy : {e}")

    if nb_erreurs > 0:
        logger.info("🔍 Exemple d’erreurs :")
        df_erreurs.show(10, truncate=False)

    spark.stop()

# ======================================================================================
# 5. Point d’entrée
# ======================================================================================

def main():
    controle_qualite()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
