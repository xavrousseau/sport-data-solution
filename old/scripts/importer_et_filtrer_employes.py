# ==========================================================================================
# Fichier     : scripts/import_employes_tempfile.py
# Objet       : Pipeline ETL d’import, nettoyage, filtrage d’éligibilité et injection
#               des employés dans PostgreSQL, en utilisant un téléchargement MinIO via
#               fichier temporaire (tempfile).
#
# Description :
#   - Télécharge le fichier RH source depuis MinIO dans un fichier temporaire local
#   - Lit et nettoie les données sous pandas
#   - Calcule la distance domicile-travail pour chaque salarié (API Google Maps)
#   - Filtre les salariés éligibles au forfait mobilité (mode & distance)
#   - Valide la qualité des données (Great Expectations)
#   - Injecte les données éligibles dans PostgreSQL
#   - Sauvegarde la version nettoyée/enrichie sur MinIO (raw/)
#
# Prérequis :
#   - Variables d’environnement (.env) correctement paramétrées (Google API, MinIO, PostgreSQL)
#   - Le module minio_helper.py dans le PYTHONPATH
#   - Accès réseau à MinIO, PostgreSQL, Google API
#
# Utilisation :
#   python scripts/import_employes_tempfile.py
#
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import tempfile
import pandas as pd
import requests
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine
import great_expectations as ge
from minio_helper import MinIOHelper

# ------------------------------------------------------------------------------------------
# 1. Chargement des variables d’environnement (.env global, à adapter selon l’environnement)
# ------------------------------------------------------------------------------------------
load_dotenv("/opt/airflow/.env", override=True)

# ------------------------------------------------------------------------------------------
# 2. Configuration MinIO (chemins objets à traiter)
# ------------------------------------------------------------------------------------------
MINIO_SOURCE_KEY = "referentiels/donnees_rh.xlsx"
MINIO_CLEANED_KEY = "raw/donnees_rh_cleaned.xlsx"

# ------------------------------------------------------------------------------------------
# 3. Adresse du lieu de travail et clé API Google Maps
# ------------------------------------------------------------------------------------------
ADRESSE_TRAVAIL = "1362 Avenue des Platanes, 34970 Lattes, France"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# ------------------------------------------------------------------------------------------
# 4. Connexion PostgreSQL (paramètres via .env)
# ------------------------------------------------------------------------------------------
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ------------------------------------------------------------------------------------------
# 5. Modes de transport et seuils d’éligibilité (km)
# ------------------------------------------------------------------------------------------
MODES_TRANSPORT = {
    "marche": 15,
    "course à pied": 15,
    "vélo": 25,
    "trottinette": 25,
    "roller": 25,
    "skateboard": 25
}

# ------------------------------------------------------------------------------------------
# 6. Calcul de la distance domicile-travail via Google Maps API
# ------------------------------------------------------------------------------------------
def calculer_distance_km(adresse_depart):
    """
    Retourne la distance en kilomètres entre l’adresse de départ et le lieu de travail,
    via l’API Google Distance Matrix.
    """
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": adresse_depart,
        "destinations": ADRESSE_TRAVAIL,
        "key": GOOGLE_API_KEY,
        "mode": "walking"
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        distance_m = data["rows"][0]["elements"][0]["distance"]["value"]  # en mètres
        return distance_m / 1000
    except Exception as e:
        logger.warning(f"Erreur API Google Maps : {e}")
        return None

# ------------------------------------------------------------------------------------------
# 7. Vérification de l’éligibilité au forfait mobilité
# ------------------------------------------------------------------------------------------
def verifier_eligibilite(adresse, mode):
    """
    Retourne True si le salarié est éligible selon le mode et la distance,
    sinon False.
    """
    if not adresse or not mode:
        return False
    mode = mode.lower().strip()
    if mode not in MODES_TRANSPORT:
        logger.info(f"Mode non éligible : {mode}")
        return False
    distance = calculer_distance_km(adresse)
    if distance is None:
        return False
    if distance > MODES_TRANSPORT[mode]:
        logger.info(f"Distance {distance:.1f} km > limite pour {mode}")
        return False
    return True

# ------------------------------------------------------------------------------------------
# 8. Fonction utilitaire : normalisation des noms de colonnes
# ------------------------------------------------------------------------------------------
def normaliser_colonnes(df):
    """
    Nettoie et uniformise les noms de colonnes pour l’intégration SQL/ETL.
    """
    df.columns = [
        col.strip().lower().replace(" ", "_").replace("'", "_").replace("’", "_")
        .replace("é", "e").replace("è", "e").replace("ê", "e")
        .replace("à", "a").replace("â", "a")
        for col in df.columns
    ]
    return df

# ------------------------------------------------------------------------------------------
# 9. Pipeline principal (lecture, enrichissement, validation, export)
# ------------------------------------------------------------------------------------------
def main():
    helper = MinIOHelper()

    # -- 1. Téléchargement du fichier RH depuis MinIO dans un fichier temporaire --
    logger.info(f"⬇️ Téléchargement {MINIO_SOURCE_KEY} depuis MinIO")
    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmpfile:
        try:
            helper.client.download_file(
                Bucket=helper.bucket,
                Key=MINIO_SOURCE_KEY,
                Filename=tmpfile.name
            )
            logger.success(f"✅ Téléchargement réussi dans {tmpfile.name}")
        except Exception as e:
            logger.error(f"❌ Échec téléchargement fichier source : {e}")
            return

        # -- 2. Lecture du fichier Excel téléchargé --
        df = pd.read_excel(tmpfile.name)

    # -- 3. Nettoyage et normalisation des colonnes --
    df = normaliser_colonnes(df)
    logger.info(f"Chargement de {len(df)} salariés depuis le fichier RH.")

    # -- 4. Calcul des distances et filtrage éligibilité --
    logger.info("Calcul des distances domicile-travail et filtrage éligibilité...")
    df["distance_km"] = df.apply(lambda row: calculer_distance_km(row["adresse_du_domicile"]), axis=1)
    df["eligible"] = df.apply(lambda row: verifier_eligibilite(row["adresse_du_domicile"], row["moyen_de_deplacement"]), axis=1)
    logger.info(f"{df['eligible'].sum()} salariés éligibles sur {len(df)}")

    df = df[df["eligible"] == True]

    # -- 5. Validation de la qualité avec Great Expectations --
    ge_df = ge.from_pandas(df)
    ge_df.expect_column_values_to_not_be_null("id_salarie")
    ge_df.expect_column_values_to_not_be_null("nom")
    ge_df.expect_column_values_to_not_be_null("prenom")
    ge_df.expect_column_values_to_be_of_type("distance_km", "float64")
    results = ge_df.validate()
    if not results["success"]:
        logger.error("Validation Great Expectations échouée, pipeline interrompu.")
        return

    # -- 6. Insertion des données dans PostgreSQL --
    try:
        engine = create_engine(DB_CONN_STRING)
        df.to_sql("employes", engine, if_exists="replace", index=False)
        logger.success("✅ Données insérées dans PostgreSQL (table 'employes')")
    except Exception as e:
        logger.error(f"Erreur insertion PostgreSQL : {e}")
        return

    # -- 7. Export du fichier nettoyé/enrichi vers MinIO (upload direct via buffer mémoire) --
    import io
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    try:
        helper.client.put_object(
            Bucket=helper.bucket,
            Key=MINIO_CLEANED_KEY,
            Body=output,
            ContentLength=output.getbuffer().nbytes,
        )
        logger.success(f"✅ Fichier nettoyé uploadé vers {MINIO_CLEANED_KEY}")
    except Exception as e:
        logger.error(f"Erreur upload fichier nettoyé MinIO : {e}")

# ------------------------------------------------------------------------------------------
# 10. Point d’entrée du script (exécution directe)
# ------------------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        logger.info("🚀 Démarrage pipeline import et filtrage employés")
        main()
        logger.info("🎉 Pipeline terminé avec succès")
    except Exception as e:
        logger.error(f"❌ Pipeline interrompu : {e}")
        raise

# ------------------------------------------------------------------------------------------
# Fin du fichier – Pipeline ETL pour traitement RH/éligibilité mobilité (mode tempfile)
# ------------------------------------------------------------------------------------------
