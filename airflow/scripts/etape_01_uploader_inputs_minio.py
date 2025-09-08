# ==========================================================================================
# Script      : etape_01_uploader_inputs_minio.py
# Objectif    : Vérifier MinIO et téléverser (uploader) les fichiers Excel d'inputs
# Auteur      : Xavier Rousseau | Août 2025
#
# Fonctionnement :
#   1) Charge la config depuis .env
#   2) Vérifie que le bucket MinIO existe (créé par init_minio.sh)
#   3) Parcourt INPUT_DATA_PATH et envoie les .xls/.xlsx dans MINIO_PATH_INPUTS
#   4) Ignore les fichiers déjà présents à l'identique (même taille ⇒ pas de ré-upload)
# ==========================================================================================

import os
import sys
import mimetypes
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ------------------------------
# Chargement configuration (.env)
# ------------------------------
load_dotenv("/opt/airflow/.env", override=True)
load_dotenv(".env", override=True)

MINIO_HOTE = os.getenv("MINIO_HOST", "sport-minio")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", f"http://{MINIO_HOTE}:{MINIO_PORT}")
MINIO_CLE = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", ""))
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", ""))
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "sportdata")

CHEMIN_SOURCE = Path(os.getenv("INPUT_DATA_PATH", "/opt/airflow/data/inputs"))
PREFIXE_DEST = os.getenv("MINIO_PATH_INPUTS", "inputs/")
if not PREFIXE_DEST.endswith("/"):
    PREFIXE_DEST += "/"

EXTENSIONS = {".xlsx", ".xls"}

# ------------------------------
# Journalisation
# ------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO")

# ------------------------------
# Fonctions utilitaires (simples)
# ------------------------------
def client_minio():
    """Retourne un client S3 (MinIO) avec timeouts et retries raisonnables."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_CLE,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
        config=Config(retries={"max_attempts": 3, "mode": "standard"},
                      connect_timeout=5, read_timeout=30),
    )

def bucket_disponible(s3) -> bool:
    """True si le bucket existe et est accessible (on ne le crée jamais ici)."""
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        return True
    except ClientError:
        return False

def meme_taille(s3, cle_objet: str, fichier: Path) -> bool:
    """True si un objet de même taille existe déjà (suffisant pour éviter le doublon)."""
    try:
        meta = s3.head_object(Bucket=MINIO_BUCKET, Key=cle_objet)
        return meta.get("ContentLength") == fichier.stat().st_size
    except ClientError:
        return False

# ------------------------------
# Traitement principal
# ------------------------------
def televerser_inputs():
    logger.info("🚀 Téléversement des fichiers d'inputs vers MinIO")

    # Garde-fous
    if not MINIO_CLE or not MINIO_SECRET:
        logger.warning("Clés MinIO manquantes. Vérifie MINIO_ROOT_USER / MINIO_ROOT_PASSWORD.")

    s3 = client_minio()
    if not bucket_disponible(s3):
        logger.error("❌ Bucket MinIO indisponible. Exécute d’abord 'init_minio.sh'.")
        sys.exit(2)

    if not CHEMIN_SOURCE.exists():
        logger.warning(f"⚠️ Dossier source introuvable : {CHEMIN_SOURCE}")
        return

    fichiers = [p for p in CHEMIN_SOURCE.iterdir() if p.is_file() and p.suffix.lower() in EXTENSIONS]
    if not fichiers:
        logger.info("Aucun fichier Excel à téléverser.")
        return

    envoyes, ignores, erreurs = 0, 0, 0
    for f in fichiers:
        cle = f"{PREFIXE_DEST}{f.name}"

        # Si le même fichier (taille) existe déjà, on évite un ré-upload inutile
        if meme_taille(s3, cle, f):
            logger.info(f"↪︎ Ignoré (identique) : {f.name}")
            ignores += 1
            continue

        try:
            type_contenu, _ = mimetypes.guess_type(str(f))
            args = {"ContentType": type_contenu} if type_contenu else {}
            s3.upload_file(str(f), MINIO_BUCKET, cle, ExtraArgs=args)
            logger.info(f"📤 Envoyé : {f.name} → {cle}")
            envoyes += 1
        except Exception as e:
            logger.error(f"❌ Échec pour {f.name} : {e}")
            erreurs += 1

    logger.info(f"Résumé : envoyés={envoyes}, ignorés={ignores}, erreurs={erreurs}")
    if erreurs:
        sys.exit(3)
    logger.info("🎯 Terminé.")

# ------------------------------
# Exécution directe (CLI)
# ------------------------------
if __name__ == "__main__":
    try:
        televerser_inputs()
    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
