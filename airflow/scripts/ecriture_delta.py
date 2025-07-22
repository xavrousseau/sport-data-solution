# ======================================================================================
# Script      : ecriture_delta.py
# Objectif    : Écriture des activités sportives de PostgreSQL vers Delta Lake (MinIO)
# Auteur      : Xavier Rousseau | Juillet 2025
# ======================================================================================

import os
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession

# ======================================================================================
# 1. Variables d'environnement
# ======================================================================================
load_dotenv(dotenv_path=".env")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_ENDPOINT = f"{MINIO_HOST}:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
DELTA_OUTPUT_PATH = "s3a://datalake/bronze/activites_sportives/"

# ======================================================================================
# 2. Initialisation Spark + lecture PostgreSQL + écriture Delta
# ======================================================================================
def ecriture_delta():
    logger.info("🚀 Initialisation Spark pour écriture Delta Lake")

    spark = SparkSession.builder \
        .appName("Écriture Delta depuis PostgreSQL") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", "sportdata.activites_sportives") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        logger.success(f"✅ {df.count()} lignes lues depuis PostgreSQL")
        df.write.format("delta").mode("overwrite").save(DELTA_OUTPUT_PATH)
        logger.success(f"📤 Données écrites dans Delta Lake : {DELTA_OUTPUT_PATH}")
    except Exception as e:
        logger.error(f"❌ Erreur d’écriture Delta : {e}")
        raise
    finally:
        spark.stop()

# ======================================================================================
# 3. Lancement CLI
# ======================================================================================
if __name__ == "__main__":
    ecriture_delta()
