# ==========================================================================================
# Fichier     : scripts/init_minio_structure.py
# Objet       : Initialise la structure du bucket MinIO pour le projet Sport Data Solution
#
# Description :
#   - Vérifie l'existence du bucket principal, le crée s'il est absent
#   - Crée les dossiers/prefixes nécessaires (simulant des répertoires) en ajoutant un
#     fichier vide '.keep' dans chacun pour forcer la création dans MinIO/S3
#
# Prérequis :
#   - Le bucket cible (ex : 'sportdata') est renseigné dans les variables du script/.env
#   - Le module minio_helper.py doit être disponible et correctement configuré
#   - Les variables d'environnement MinIO doivent être bien renseignées dans le .env
#
# Utilisation :
#   python scripts/init_minio_structure.py
#
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from minio_helper import MinIOHelper
from botocore.exceptions import ClientError

# ------------------------------------------------------------------------------------------
# Paramètres de structure : nom du bucket et "dossiers" à créer
# ------------------------------------------------------------------------------------------
BUCKET = "sportdata"  # Nom du bucket principal (adapter si besoin)
PREFIXES = [
    "referentiels/",
    "simulation/",
    "raw/",
    "resultats/prime_sportive/",
    "resultats/jours_bien_etre/",
    "exports/"
]

# ------------------------------------------------------------------------------------------
# Fonction : Créer le bucket s'il n'existe pas déjà
# ------------------------------------------------------------------------------------------
def create_bucket_if_missing():
    """
    Vérifie l'existence du bucket MinIO, et le crée si absent.

    Le test se fait via un head_bucket. Si le bucket n'existe pas (erreur 404/NoSuchBucket),
    il est créé immédiatement. En cas d'autre erreur, celle-ci est affichée et remontée.
    """
    print("🛠 Démarrage de create_bucket_if_missing()")
    helper = MinIOHelper()
    print(f"📡 Tentative de connexion à : {helper.endpoint_url}")

    try:
        helper.client.head_bucket(Bucket=BUCKET)
        print(f"✅ Bucket '{BUCKET}' déjà existant.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        # Codes typiques : "404" ou "NoSuchBucket" (MinIO/S3)
        if error_code in ["404", "NoSuchBucket"]:
            print(f"📦 Création du bucket '{BUCKET}'...")
            helper.client.create_bucket(Bucket=BUCKET)
            print(f"✅ Bucket '{BUCKET}' créé.")
        else:
            print(f"🧨 Erreur head_bucket : {e}")
            raise

# ------------------------------------------------------------------------------------------
# Fonction : Créer la structure de préfixes/dossiers dans le bucket
# ------------------------------------------------------------------------------------------
def create_prefix_structure():
    """
    Crée les "dossiers" (préfixes S3) dans le bucket en y déposant un fichier vide .keep.

    Ceci permet d’avoir une structure apparente de répertoires dans l’interface MinIO ou S3.
    """
    helper = MinIOHelper()
    for prefix in PREFIXES:
        key = prefix + ".keep"  # Convention : un fichier vide par dossier
        try:
            helper.client.put_object(Bucket=BUCKET, Key=key, Body=b"")
            print(f"📂 Préfixe forcé : {prefix}")
        except Exception as e:
            print(f"❌ Erreur put_object pour {key} : {e}")

# ------------------------------------------------------------------------------------------
# Point d'entrée du script (exécution directe uniquement)
# ------------------------------------------------------------------------------------------
if __name__ == "__main__":
    create_bucket_if_missing()
    create_prefix_structure()

# ------------------------------------------------------------------------------------------
# Fin du fichier : ce script peut être lancé seul pour initialiser la structure MinIO
# ------------------------------------------------------------------------------------------
