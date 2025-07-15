# ==========================================================================================
# Fichier     : scripts/minio_helper.py
# Objet       : Classe utilitaire pour interagir avec MinIO (S3-compatible)
# Auteur      : Xavier Rousseau | Version enrichie — Juillet 2025
# ==========================================================================================

import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from loguru import logger
from pathlib import Path

# ------------------------------------------------------------------------------------------
# Chargement du .env global (dans container Airflow)
# ------------------------------------------------------------------------------------------
load_dotenv("/opt/airflow/.env", override=True)

class MinIOHelper:
    def __init__(self):
        self.endpoint_url = f"http://{os.getenv('MINIO_HOST')}:{os.getenv('MINIO_PORT')}"
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        self.bucket = os.getenv("MINIO_BUCKET_NAME", "sportdata")

        if not self.access_key or not self.secret_key:
            raise EnvironmentError("❌ Clés MinIO manquantes dans le .env")

        logger.info(f"📡 Connexion MinIO → {self.endpoint_url} | Bucket : {self.bucket}")

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1"
        )

    # --------------------------------------------------------------------------------------
    # Vérifie la présence du bucket et le crée si besoin
    # --------------------------------------------------------------------------------------
    def create_bucket_if_missing(self):
        try:
            self.client.head_bucket(Bucket=self.bucket)
            logger.info(f"✅ Bucket '{self.bucket}' déjà existant.")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ["404", "NoSuchBucket"]:
                self.client.create_bucket(Bucket=self.bucket)
                logger.success(f"📦 Bucket '{self.bucket}' créé.")
            else:
                logger.error(f"❌ Erreur accès bucket : {e}")
                raise

    # --------------------------------------------------------------------------------------
    # Upload d’un fichier local vers MinIO
    # --------------------------------------------------------------------------------------
    def upload(self, local_path, object_key):
        try:
            self.client.upload_file(local_path, self.bucket, object_key)
            logger.success(f"📤 Upload : {local_path} → {self.bucket}/{object_key}")
        except Exception as e:
            logger.error(f"❌ Erreur upload : {e}")
            raise

    # --------------------------------------------------------------------------------------
    # Téléchargement depuis MinIO vers fichier local
    # --------------------------------------------------------------------------------------
    def download(self, object_key, local_path):
        try:
            self.client.download_file(self.bucket, object_key, local_path)
            logger.success(f"📥 Téléchargé : {self.bucket}/{object_key} → {local_path}")
        except Exception as e:
            logger.error(f"❌ Téléchargement échoué : {e}")
            raise

    # --------------------------------------------------------------------------------------
    # Liste tous les objets dans le bucket (avec prefix facultatif)
    # --------------------------------------------------------------------------------------
    def list_objects(self, prefix=""):
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except Exception as e:
            logger.error(f"❌ Erreur list_objects : {e}")
            return []

    # --------------------------------------------------------------------------------------
    # Supprime un objet donné
    # --------------------------------------------------------------------------------------
    def delete(self, object_key):
        try:
            self.client.delete_object(Bucket=self.bucket, Key=object_key)
            logger.success(f"🗑️ Supprimé : {self.bucket}/{object_key}")
        except Exception as e:
            logger.error(f"❌ Erreur suppression : {object_key} — {e}")

    # --------------------------------------------------------------------------------------
    # Supprime tous les objets puis le bucket
    # --------------------------------------------------------------------------------------
    def delete_bucket(self):
        try:
            objets = self.list_objects()
            if objets:
                logger.info(f"🧺 Suppression de {len(objets)} fichiers...")
                for obj in objets:
                    self.delete(obj)
            else:
                logger.info(f"🧼 Bucket '{self.bucket}' déjà vide.")
            self.client.delete_bucket(Bucket=self.bucket)
            logger.success(f"💥 Bucket '{self.bucket}' supprimé.")
        except Exception as e:
            logger.error(f"❌ Erreur suppression bucket : {e}")
