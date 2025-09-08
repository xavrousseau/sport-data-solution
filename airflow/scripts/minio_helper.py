# ==========================================================================================
# Fichier     : scripts/minio_helper.py
# Objet       : Classe utilitaire pour interagir facilement avec MinIO (S3-compatible)
#
# Compatibilité :
#   - API inchangée vs version précédente :
#       class MinIOHelper:
#         - list_objects(prefix="")
#         - delete(object_key)
#         - delete_bucket()
#         - read_excel(key)
#         - upload_file(local_path, key)
#         - upload_excel(df, key, label="Fichier Excel")
#
# Améliorations :
#   - .env harmonisé (/opt/airflow/.env puis .env)
#   - Timeouts + retries réseau via botocore.config.Config
#   - ContentType explicite pour Excel
#   - Pagination list_objects (gestion >1000 objets)
# ==========================================================================================

import os
import io
import mimetypes

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from loguru import logger

# ------------------------------------------------------------------------------------------
# 1) Chargement .env : priorité Airflow, fallback local
# ------------------------------------------------------------------------------------------
try:
    load_dotenv("/opt/airflow/.env", override=True)
except Exception:
    pass
load_dotenv(".env", override=True)


class MinIOHelper:
    """
    Classe utilitaire pour manipuler MinIO (S3).
    Compatibilité totale avec la version existante.
    """

    def __init__(self):
        """
        Initialise le client boto3 avec les variables d'environnement et vérifie leur présence.
        """
        self.endpoint_url = f"http://{os.getenv('MINIO_HOST')}:{os.getenv('MINIO_PORT')}"
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        self.bucket = os.getenv("MINIO_BUCKET_NAME", "sportdata")  # défaut inchangé

        if not self.access_key or not self.secret_key:
            raise EnvironmentError("❌ Clés MinIO manquantes dans le .env")

        logger.info(f"📡 Connexion MinIO → {self.endpoint_url} | Bucket : {self.bucket}")

        # Config plus robuste : retries & timeouts (évite les blocages réseau)
        cfg = Config(
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=5,
            read_timeout=30,
        )

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1",  # exigé par boto3 même si MinIO ignore
            config=cfg,
        )

    # --------------------------------------------------------------------------------------
    # 2) Lister les objets du bucket (avec pagination)
    # --------------------------------------------------------------------------------------
    def list_objects(self, prefix: str = ""):
        """
        Liste tous les objets du bucket correspondant à un préfixe donné.

        Args:
            prefix (str): ex. 'raw/' ou '' (tout le bucket)
        Returns:
            list[str]: clés S3 (paths) présentes dans le bucket
        """
        keys = []
        try:
            continuation_token = None
            while True:
                kwargs = {"Bucket": self.bucket, "Prefix": prefix}
                if continuation_token:
                    kwargs["ContinuationToken"] = continuation_token
                resp = self.client.list_objects_v2(**kwargs)
                contents = resp.get("Contents", [])
                keys.extend(obj["Key"] for obj in contents)
                if resp.get("IsTruncated"):
                    continuation_token = resp.get("NextContinuationToken")
                else:
                    break
            return keys
        except Exception as e:
            logger.error(f"❌ Erreur list_objects(prefix='{prefix}') : {e}")
            return []

    # --------------------------------------------------------------------------------------
    # 3) Supprimer un objet
    # --------------------------------------------------------------------------------------
    def delete(self, object_key: str):
        """
        Supprime un objet identifié par sa clé dans le bucket.

        Args:
            object_key (str): Clé de l'objet (chemin complet dans le bucket)
        """
        try:
            self.client.delete_object(Bucket=self.bucket, Key=object_key)
            logger.success(f"🗑️ Supprimé : {self.bucket}/{object_key}")
        except Exception as e:
            logger.error(f"❌ Erreur suppression : {object_key} — {e}")

    # --------------------------------------------------------------------------------------
    # 4) Supprimer l'intégralité du bucket (purge objets + bucket)
    # --------------------------------------------------------------------------------------
    def delete_bucket(self):
        """
        Supprime tous les objets puis le bucket lui-même.
        ATTENTION : opération destructive.
        """
        try:
            objets = self.list_objects()
            if objets:
                logger.info(f"🧺 Suppression de {len(objets)} fichiers dans '{self.bucket}'")
                # suppression par paquets (S3 DeleteObjects accepte jusqu'à 1000 keys)
                for i in range(0, len(objets), 1000):
                    chunk = objets[i : i + 1000]
                    self.client.delete_objects(
                        Bucket=self.bucket,
                        Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
                    )
            else:
                logger.info(f"🧼 Bucket '{self.bucket}' déjà vide.")

            self.client.delete_bucket(Bucket=self.bucket)
            logger.success(f"💥 Bucket '{self.bucket}' supprimé avec succès.")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la suppression du bucket : {e}")

    # --------------------------------------------------------------------------------------
    # 5) Lire un Excel depuis MinIO dans un DataFrame Pandas
    # --------------------------------------------------------------------------------------
    def read_excel(self, key: str) -> pd.DataFrame:
        """
        Télécharge un fichier Excel depuis MinIO et retourne un DataFrame Pandas.
        """
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=key)
            return pd.read_excel(io.BytesIO(obj["Body"].read()))
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                logger.error(f"❌ Fichier {key} non trouvé dans le bucket {self.bucket}")
            else:
                logger.error(f"❌ Erreur lors du téléchargement de {key} : {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Erreur lecture Excel {key} : {e}")
            raise

    # --------------------------------------------------------------------------------------
    # 6) Uploader un fichier local vers MinIO
    # --------------------------------------------------------------------------------------
    def upload_file(self, local_path: str, key: str):
        """
        Upload un fichier local vers MinIO à la clé spécifiée.
        """
        try:
            # On essaye de deviner le Content-Type pour une meilleure UX côté outils aval
            content_type, _ = mimetypes.guess_type(local_path)
            extra_args = {"ContentType": content_type} if content_type else None
            if extra_args:
                self.client.upload_file(local_path, self.bucket, key, ExtraArgs=extra_args)
            else:
                self.client.upload_file(local_path, self.bucket, key)
            logger.success(f"📤 Fichier uploadé : {local_path} ➜ {key}")
        except Exception as e:
            logger.error(f"❌ Erreur upload {local_path} : {e}")
            raise

    # --------------------------------------------------------------------------------------
    # 7) Uploader un DataFrame Pandas en Excel directement (en mémoire)
    # --------------------------------------------------------------------------------------
    def upload_excel(self, df: pd.DataFrame, key: str, label: str = "Fichier Excel"):
        """
        Upload un DataFrame Pandas sous format Excel vers MinIO (sans fichier temporaire).
        """
        try:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False)
            buffer.seek(0)
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=buffer,
                ContentLength=buffer.getbuffer().nbytes,
                ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            logger.success(f"📤 {label} uploadé sur MinIO : {key}")
        except Exception as e:
            logger.error(f"❌ Erreur upload Excel {label} : {e}")
            raise

# ------------------------------------------------------------------------------------------
# Fin du fichier
# ------------------------------------------------------------------------------------------
