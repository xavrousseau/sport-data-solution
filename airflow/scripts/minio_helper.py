# ==========================================================================================
# Fichier     : scripts/minio_helper.py
# Objet       : Classe utilitaire pour interagir facilement avec MinIO (S3-compatible)
#
# Description :
#   Permet de centraliser les opérations courantes sur un bucket MinIO depuis un pipeline :
#     - Connexion simple via boto3 en utilisant les variables d'environnement
#     - Liste des objets d'un bucket
#     - Suppression d'un objet
#     - Suppression complète d'un bucket (avec purge)
#
# Prérequis :
#   - Les variables d'environnement suivantes doivent être définies dans le .env :
#       MINIO_HOST, MINIO_PORT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_BUCKET_NAME
#   - Le fichier .env doit être présent dans /opt/airflow/.env (dans le container)
#
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from loguru import logger

# ------------------------------------------------------------------------------------------
# 1. Chargement du .env pour récupérer les variables MinIO (appelé au chargement du module)
# ------------------------------------------------------------------------------------------
load_dotenv("/opt/airflow/.env", override=True)

class MinIOHelper:
    """
    Classe utilitaire pour manipuler MinIO (S3) : connexion, listing, suppression objets/bucket.
    """

    def __init__(self):
        """
        Initialise le client boto3 avec les variables d'environnement et vérifie leur présence.
        """
        self.endpoint_url = f"http://{os.getenv('MINIO_HOST')}:{os.getenv('MINIO_PORT')}"
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        self.bucket = os.getenv("MINIO_BUCKET_NAME", "sportdata")  # Valeur par défaut si non précisé

        # Sécurisation : vérification des credentials
        if not self.access_key or not self.secret_key:
            raise EnvironmentError("❌ Clés MinIO manquantes dans le .env")

        logger.info(f"📡 Connexion MinIO → {self.endpoint_url} | Bucket : {self.bucket}")

        # Initialisation du client boto3 S3
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1"  # MinIO ignore la région, mais boto3 l'exige
        )

    # --------------------------------------------------------------------------------------
    # 2. Lister les objets présents dans le bucket (option : préfixe de chemin)
    # --------------------------------------------------------------------------------------
    def list_objects(self, prefix=""):
        """
        Liste tous les objets du bucket MinIO correspondant à un préfixe donné.

        Args:
            prefix (str) : Filtre sur le début du nom de fichier (ex : 'raw/' ou '')

        Returns:
            list : Liste des clés objets (paths) présents dans le bucket.
        """
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except Exception as e:
            logger.error(f"❌ Erreur list_objects : {e}")
            return []

    # --------------------------------------------------------------------------------------
    # 3. Supprimer un objet spécifique dans le bucket
    # --------------------------------------------------------------------------------------
    def delete(self, object_key):
        """
        Supprime un objet identifié par sa clé dans le bucket MinIO.

        Args:
            object_key (str): Clé de l'objet (chemin complet dans le bucket)
        """
        try:
            self.client.delete_object(Bucket=self.bucket, Key=object_key)
            print(f"🗑️ Supprimé : {self.bucket}/{object_key}")
        except Exception as e:
            print(f"❌ Erreur suppression : {object_key} — {e}")

    # --------------------------------------------------------------------------------------
    # 4. Supprimer l'intégralité du bucket (purge objets + bucket)
    # --------------------------------------------------------------------------------------
    def delete_bucket(self):
        """
        Supprime tous les objets du bucket puis le bucket lui-même.

        Attention : opération destructive ! Assure-toi de ne pas supprimer un bucket important par erreur.
        """
        try:
            # Suppression de tous les objets du bucket
            objets = self.list_objects()
            if objets:
                print(f"🧺 Suppression de {len(objets)} fichiers dans le bucket '{self.bucket}'")
                for obj in objets:
                    self.delete(obj)
            else:
                print(f"🧼 Bucket '{self.bucket}' déjà vide.")

            # Suppression du bucket lui-même
            self.client.delete_bucket(Bucket=self.bucket)
            print(f"💥 Bucket '{self.bucket}' supprimé avec succès.")
        except Exception as e:
            print(f"❌ Erreur lors de la suppression du bucket : {e}")

# ------------------------------------------------------------------------------------------
# Fin du fichier (aucun code d'exécution directe ici : importer la classe pour utilisation)
# ------------------------------------------------------------------------------------------
