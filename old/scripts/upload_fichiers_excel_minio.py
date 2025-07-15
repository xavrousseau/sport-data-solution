# === Script : Upload des fichiers Excel vers MinIO dans /referentiels/ ===
# Ce script détecte automatiquement les fichiers .xlsx dans /opt/airflow/data/
# et les envoie dans MinIO, dossier 'referentiels/', bucket défini dans .env.

import os
import sys
from pathlib import Path
from loguru import logger
import boto3
from botocore.exceptions import ClientError

# ==============================================================================
# 🔧 Initialisation des logs
# ==============================================================================
LOGS_PATH = Path(os.getenv("AIRFLOW_LOG_PATH", "logs"))
LOGS_PATH.mkdir(parents=True, exist_ok=True)

logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOGS_PATH / "upload_fichiers_excel_minio.log", level="INFO", rotation="1 MB")

# ==============================================================================
# ☁️ Configuration MinIO depuis .env
# ==============================================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin1234")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "sport-data")
MINIO_DESTINATION_PREFIX = os.getenv("MINIO_DESTINATION_PREFIX", "referentiels/")

# 📁 Dossier des fichiers Excel
DATA_DIR = Path("/opt/airflow/data")
EXTENSIONS = [".xlsx", ".xls"]

# ==============================================================================
# 📤 Fonction d’upload
# ==============================================================================
def upload_excel_files_to_minio():
    logger.info("📦 Recherche des fichiers Excel à uploader...")
    excel_files = [f for f in DATA_DIR.glob("*.xlsx") if f.is_file()]

    if not excel_files:
        logger.warning("⚠️ Aucun fichier Excel trouvé dans le dossier.")
        return

    # Connexion MinIO
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name="us-east-1",
        )
        logger.success("✅ Connexion à MinIO réussie.")
    except Exception as e:
        logger.error(f"❌ Connexion échouée : {e}")
        sys.exit(1)

    # Vérification ou création du bucket
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET_NAME)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            logger.info(f"📁 Bucket '{MINIO_BUCKET_NAME}' introuvable. Création en cours...")
            try:
                s3.create_bucket(Bucket=MINIO_BUCKET_NAME)
                logger.success(f"✅ Bucket '{MINIO_BUCKET_NAME}' créé.")
            except Exception as err:
                logger.error(f"❌ Erreur création bucket : {err}")
                sys.exit(1)
        else:
            logger.error(f"❌ Accès au bucket impossible : {e}")
            sys.exit(1)

    # Upload des fichiers
    for file in excel_files:
        key = f"{MINIO_DESTINATION_PREFIX}{file.name}"
        try:
            s3.upload_file(str(file), MINIO_BUCKET_NAME, key)
            logger.success(f"📤 {file.name} ➔ MinIO → {key}")
        except Exception as e:
            logger.error(f"❌ Upload échoué pour {file.name} : {e}")

    logger.success("🎯 Tous les fichiers Excel ont été traités.")

# ==============================================================================
# 🚀 Lancement
# ==============================================================================
if __name__ == "__main__":
    try:
        upload_excel_files_to_minio()
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
