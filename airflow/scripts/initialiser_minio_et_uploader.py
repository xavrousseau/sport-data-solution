# ==========================================================================================
# Script      : minio_initialisation_et_upload.py
# Objectif    : Initialiser la structure MinIO + uploader les fichiers Excel de référence
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
# 1. Chargement du .env et configuration
# ==========================================================================================
load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")
MINIO_ENDPOINT = f"http://{MINIO_HOST}:{MINIO_PORT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "sportdata")

DATA_DIR = Path("/opt/airflow/data/inputs")
EXTENSIONS = [".xlsx", ".xls"]

PREFIXES = [
    "referentiels/",
    "simulation/",
    "raw/",
    "resultats/prime_sportive/",
    "resultats/jours_bien_etre/",
    "exports/"
]

# ==========================================================================================
# 2. Configuration des logs
# ==========================================================================================
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(exist_ok=True)

logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOGS_PATH / "minio_initialisation_et_upload.log", level="INFO", rotation="1 MB")

# ==========================================================================================
# 3. Fonction principale
# ==========================================================================================
def initialiser_et_uploader():
    logger.info("🚀 Début initialisation MinIO...")

    # Connexion MinIO
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
        logger.error(f"❌ Connexion échouée : {e}")
        sys.exit(1)

    # Vérification/création du bucket
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        logger.info(f"📦 Bucket existant : {MINIO_BUCKET}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            try:
                s3.create_bucket(Bucket=MINIO_BUCKET)
                logger.success(f"📦 Bucket créé : {MINIO_BUCKET}")
            except Exception as err:
                logger.error(f"❌ Création bucket échouée : {err}")
                sys.exit(1)
        else:
            logger.error(f"❌ Erreur accès bucket : {e}")
            sys.exit(1)

    # Création des préfixes via .keep
    for prefix in PREFIXES:
        key = prefix + ".keep"
        try:
            s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=b"")
            logger.success(f"📁 Préfixe créé : {prefix}")
        except Exception as e:
            logger.warning(f"⚠️ Erreur création préfixe {prefix} : {e}")

    # Upload des fichiers Excel vers referentiels/
    fichiers_excel = [f for f in DATA_DIR.glob("*") if f.suffix in EXTENSIONS and f.is_file()]
    if not fichiers_excel:
        logger.warning("⚠️ Aucun fichier Excel trouvé dans /data/inputs.")
    else:
        for fichier in fichiers_excel:
            key = f"referentiels/{fichier.name}"
            try:
                s3.upload_file(str(fichier), MINIO_BUCKET, key)
                logger.success(f"📤 Fichier uploadé : {fichier.name} ➜ {key}")
            except Exception as e:
                logger.error(f"❌ Upload échoué pour {fichier.name} : {e}")

    logger.success("🎯 Structure MinIO prête + fichiers Excel transférés.")

# ==========================================================================================
# 4. Exécution directe
# ==========================================================================================
if __name__ == "__main__":
    try:
        initialiser_et_uploader()
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
