# ==========================================================================================
# Script      : diagnostic_pipeline_sportdata.py
# Objectif    : Diagnostic automatique de l’état du pipeline Sport Data Solution
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import sys
from dotenv import load_dotenv
from loguru import logger
import boto3
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pathlib import Path

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

MINIO_HOST = os.getenv("MINIO_HOST", "localhost")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "sportdata")

POSTGRES_CONN = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "dbname": os.getenv("POSTGRES_DB")
}

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata_activites")

# ==========================================================================================
# 2. Vérification MinIO
# ==========================================================================================

def verifier_minio():
    logger.info("🔍 Vérification MinIO...")
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_HOST}:{MINIO_PORT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name="us-east-1"
        )

        prefixes = [
            "referentiels/", "raw/", "simulation/", "exports/",
            "resultats/prime_sportive/", "resultats/jours_bien_etre/"
        ]
        for prefix in prefixes:
            result = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
            nb = result.get("KeyCount", 0)
            if nb == 0:
                logger.warning(f"📁 Dossier vide ou absent : {prefix}")
            else:
                logger.success(f"📁 {prefix} contient {nb} objet(s)")
    except Exception as e:
        logger.error(f"❌ Erreur MinIO : {e}")

# ==========================================================================================
# 3. Vérification PostgreSQL
# ==========================================================================================

def verifier_postgres():
    logger.info("🔍 Vérification PostgreSQL...")
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()
        tables = ["sportdata.employes", "sportdata.activites_sportives", "sportdata.avantages_sportifs"]
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.success(f"📊 Table {table} : {count} lignes")
            except Exception as e:
                logger.warning(f"⚠️ Table inaccessible : {table} ({e})")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Connexion PostgreSQL échouée : {e}")

# ==========================================================================================
# 4. Vérification Kafka
# ==========================================================================================

def verifier_kafka():
    logger.info("🔍 Vérification Kafka/Redpanda...")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            group_id="diagnostic-check",
            auto_offset_reset="earliest",
            consumer_timeout_ms=2000
        )
        topics = consumer.topics()
        if KAFKA_TOPIC in topics:
            logger.success(f"📡 Topic Kafka présent : {KAFKA_TOPIC}")
        else:
            logger.warning(f"⚠️ Topic Kafka absent : {KAFKA_TOPIC}")
        consumer.close()
    except KafkaError as e:
        logger.error(f"❌ Erreur Kafka : {e}")

# ==========================================================================================
# 5. Vérification logs Airflow (si lancés en local)
# ==========================================================================================

def verifier_logs_airflow():
    logs_path = Path("/opt/airflow/logs")
    if not logs_path.exists():
        logger.info("📁 Logs Airflow non accessibles (local uniquement)")
        return
    logger.info("📜 Lecture logs Airflow (10 lignes max)")
    for file in logs_path.rglob("*.log"):
        try:
            lines = file.read_text(encoding="utf-8").splitlines()
            if lines:
                logger.info(f"🧾 {file.name} → Dernière ligne : {lines[-1]}")
        except Exception:
            pass

# ==========================================================================================
# 6. Lancement global
# ==========================================================================================

if __name__ == "__main__":
    logger.info("🔧 Lancement du diagnostic complet Sport Data Solution")
    verifier_minio()
    verifier_postgres()
    verifier_kafka()
    verifier_logs_airflow()
    logger.info("✅ Diagnostic terminé.")
