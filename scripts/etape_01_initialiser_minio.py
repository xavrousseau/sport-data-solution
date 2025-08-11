# ==========================================================================================
# Script      : etape_01_initialiser_minio.py
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
# 1. Chargement des variables d’environnement (.env global utilisé dans toute la stack)
# ==========================================================================================

load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")
MINIO_ENDPOINT = f"http://{MINIO_HOST}:{MINIO_PORT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "sportdata")

# Répertoire contenant les fichiers Excel de référence
DATA_DIR = Path("/opt/airflow/data/inputs")
EXTENSIONS = [".xlsx", ".xls"]

# Dossiers logiques à créer dans MinIO (préfixes S3)
PREFIXES = [
    "inputs/",        # fichiers Excel initiaux (RH + sport)
    "clean/",         # Données nettoyées prêtes à l’usage
    "bronze/",        # Flux brut (Kafka → Delta)
    "silver/",        # Après contrôle qualité
    "gold/",          # Résultats finaux (primes, JBE, agrégats)
    "exports/",       # Fichiers Excel pour Power BI, audits, etc.
    "validation/"     # Rapports Great Expectations
]

# ==========================================================================================
# 2. Configuration des logs
# ==========================================================================================

LOGS_PATH = Path("/opt/airflow/logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)

logger.remove()
logger.add(sys.stdout, level="INFO")  # Console
logger.add(LOGS_PATH / "minio_initialisation_et_upload.log", level="INFO", rotation="1 MB")  # Fichier

# ==========================================================================================
# 3. Fonction principale : Initialisation + Upload
# ==========================================================================================

def initialiser_et_uploader():
    """
    Initialise la structure MinIO :
    - Crée le bucket s’il n’existe pas
    - Crée les dossiers logiques avec un fichier .keep
    - Upload les fichiers Excel initiaux dans `inputs/`
    """
    logger.info("🚀 Début de l'initialisation MinIO...")

    # --- Connexion à MinIO avec boto3 ---
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name="us-east-1"  # obligatoire même si non utilisé
        )
        logger.success("✅ Connexion à MinIO réussie.")
    except Exception as e:
        logger.error(f"❌ Connexion échouée : {e}")
        raise

    # --- Création du bucket s’il n’existe pas ---
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        logger.info(f"📦 Bucket déjà présent : {MINIO_BUCKET}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            try:
                s3.create_bucket(Bucket=MINIO_BUCKET)
                logger.success(f"📦 Bucket créé : {MINIO_BUCKET}")
            except Exception as err:
                logger.error(f"❌ Création échouée : {err}")
                raise
        else:
            logger.error(f"❌ Erreur accès bucket : {e}")
            raise

    # --- Création des dossiers logiques avec un .keep ---
    for prefix in PREFIXES:
        key = prefix + ".keep"
        try:
            s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=b"")
            logger.success(f"📁 Dossier logique créé : {prefix}")
        except Exception as e:
            logger.warning(f"⚠️ Erreur création dossier {prefix} : {e}")

    # --- Upload des fichiers Excel de référence dans "inputs/" ---
    fichiers_excel = [f for f in DATA_DIR.glob("*") if f.suffix in EXTENSIONS and f.is_file()]
    if not fichiers_excel:
        logger.warning("⚠️ Aucun fichier Excel trouvé dans /data/inputs.")
    else:
        for fichier in fichiers_excel:
            key = f"inputs/{fichier.name}"
            try:
                s3.upload_file(str(fichier), MINIO_BUCKET, key)
                logger.success(f"📤 Fichier uploadé : {fichier.name} ➜ {key}")
            except Exception as e:
                logger.error(f"❌ Upload échoué pour {fichier.name} : {e}")

    logger.success("🎯 Initialisation MinIO terminée avec succès.")

# ==========================================================================================
# 4. Point d’entrée CLI
# ==========================================================================================

def main():
    initialiser_et_uploader()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
