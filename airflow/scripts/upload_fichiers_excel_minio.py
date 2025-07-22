# ==========================================================================================
# Script      : upload_fichiers_excel_minio.py
# Objectif    : Uploader tous les fichiers Excel (RH / sport) vers MinIO dans /referentiels/
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
import boto3
from botocore.exceptions import ClientError

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")
MINIO_ENDPOINT = f"http://{MINIO_HOST}:{MINIO_PORT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET", "datalake")
DESTINATION_PREFIX = "referentiels/"

DATA_DIR = Path("/opt/airflow/data")
EXTENSIONS = [".xlsx", ".xls"]

# ==========================================================================================
# 2. Configuration des logs
# ==========================================================================================
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(exist_ok=True)

logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOGS_PATH / "upload_fichiers_excel_minio.log", level="INFO", rotation="1 MB")

# ==========================================================================================
# 3. Fonction d’upload
# ==========================================================================================
def upload_excel_files_to_minio():
    logger.info(f"🔍 Recherche de fichiers Excel dans : {DATA_DIR}")
    fichiers_excel = [f for f in DATA_DIR.glob("*") if f.suffix in EXTENSIONS and f.is_file()]

    if not fichiers_excel:
        logger.warning("⚠️ Aucun fichier Excel trouvé.")
        return

    # Connexion MinIO via boto3
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name="us-east-1"
        )
        logger.success("✅ Connexion à MinIO réussie.")
    except Exception as e:
        logger.error(f"❌ Connexion MinIO échouée : {e}")
        sys.exit(1)

    # Vérifie ou crée le bucket
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET_NAME)
        logger.info(f"📦 Bucket MinIO trouvé : {MINIO_BUCKET_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info(f"📁 Bucket inexistant, création : {MINIO_BUCKET_NAME}")
            try:
                s3.create_bucket(Bucket=MINIO_BUCKET_NAME)
                logger.success("✅ Bucket créé avec succès.")
            except Exception as err:
                logger.error(f"❌ Erreur création bucket : {err}")
                sys.exit(1)
        else:
            logger.error(f"❌ Accès bucket échoué : {e}")
            sys.exit(1)

    # Upload des fichiers
    for fichier in fichiers_excel:
        key = f"{DESTINATION_PREFIX}{fichier.name}"
        try:
            s3.upload_file(str(fichier), MINIO_BUCKET_NAME, key)
            logger.success(f"📤 Fichier uploadé : {fichier.name} ➔ {key}")
        except Exception as e:
            logger.error(f"❌ Upload échoué pour {fichier.name} : {e}")

    logger.success("🎯 Tous les fichiers Excel ont été traités.")

# ==========================================================================================
# 4. Lancement
# ==========================================================================================
# ==========================================================================================
# 4. Lancement
# ==========================================================================================
def main():
    upload_excel_files_to_minio()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
