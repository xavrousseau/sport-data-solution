# ==========================================================================================
# Fichier     : tests/minio_helper.py
# Objet       : Classe utilitaire pour interagir avec MinIO (S3-compatible)
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from loguru import logger

# Chargement du .env global
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

    def list_objects(self, prefix=""):
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except Exception as e:
            logger.error(f"❌ Erreur list_objects : {e}")
            return []

    def delete(self, object_key):
        try:
            self.client.delete_object(Bucket=self.bucket, Key=object_key)
            print(f"🗑️ Supprimé : {self.bucket}/{object_key}")
        except Exception as e:
            print(f"❌ Erreur suppression : {object_key} — {e}")

    def delete_bucket(self):
        """
        Supprime tous les objets du bucket puis le bucket lui-même.
        """
        try:
            # Supprimer tous les objets
            objets = self.list_objects()
            if objets:
                print(f"🧺 Suppression de {len(objets)} fichiers dans le bucket '{self.bucket}'")
                for obj in objets:
                    self.delete(obj)
            else:
                print(f"🧼 Bucket '{self.bucket}' déjà vide.")

            # Supprimer le bucket
            self.client.delete_bucket(Bucket=self.bucket)
            print(f"💥 Bucket '{self.bucket}' supprimé avec succès.")
        except Exception as e:
            print(f"❌ Erreur lors de la suppression du bucket : {e}")
