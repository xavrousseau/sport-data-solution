# ==========================================================================================
# Script      : croiser_rh_sport_et_calculer_prime.py
# Objectif    : Croisement RH/Sport, calcul des bénéficiaires et montant de la prime
#               Export table finale (MinIO/PostgreSQL)
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import pandas as pd
import tempfile
from loguru import logger
from minio_helper import MinIOHelper
from sqlalchemy import create_engine
import os

# --- Fichiers cleaned sur MinIO ---
MINIO_RH_KEY = "raw/donnees_rh_cleaned.xlsx"
MINIO_SPORT_KEY = "raw/donnees_sportives_cleaned.xlsx"
MINIO_EXPORT_KEY = "final/beneficiaires_primes_sportives.xlsx"
TMP_DIR = "/tmp"

# --- Connexion PostgreSQL ---
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env", override=True)
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# --- Paramètres prime ---
PRIME_MONTANT = 100  # Prime forfaitaire en euros (exemple)

# ==========================================================================================
# 1. Téléchargement et chargement des fichiers cleaned depuis MinIO
# ==========================================================================================
def charger_fichier_minio(helper, key):
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        helper.client.download_file(Bucket=helper.bucket, Key=key, Filename=tmpfile.name)
        logger.success(f"✅ Fichier {key} téléchargé depuis MinIO")
        return pd.read_excel(tmpfile.name)

# ==========================================================================================
# 2. Pipeline de croisement RH/Sport et calcul de prime
# ==========================================================================================
def pipeline_croisement_prime():
    logger.info("=== Démarrage du croisement RH/Sport et calcul de prime ===")
    helper = MinIOHelper()

    # --- Chargement des fichiers nettoyés ---
    df_rh = charger_fichier_minio(helper, MINIO_RH_KEY)
    df_sport = charger_fichier_minio(helper, MINIO_SPORT_KEY)

    logger.info(f"Salariés RH éligibles : {len(df_rh)}")
    logger.info(f"Déclarations sportives valides : {len(df_sport)}")

    # --- Normalisation des noms de colonnes pour jointure ---
    col_id_rh = next((c for c in df_rh.columns if "id" in c.lower() and "salarie" in c.lower()), "id_salarie")
    col_id_sport = next((c for c in df_sport.columns if "id" in c.lower() and "salarie" in c.lower()), "id_salarie")

    # --- Jointure : on ne garde que les salariés RH éligibles ayant au moins une activité reconnue ---
    df_joint = pd.merge(
        df_rh,
        df_sport,
        how="inner",
        left_on=col_id_rh,
        right_on=col_id_sport,
        suffixes=('_rh', '_sport')
    )
    logger.info(f"Bénéficiaires RH ayant une activité sportive reconnue : {len(df_joint)}")

    # --- Calcul de la prime : simple (forfaitaire par salarié avec activité) ---
    df_joint["prime_eligible"] = True
    df_joint["prime_montant_eur"] = PRIME_MONTANT

    # --- Reporting synthétique ---
    logger.info("\n--- Reporting synthétique ---")
    logger.info(f"Nombre de bénéficiaires de la prime : {df_joint[col_id_rh].nunique()}")
    logger.info("Montant total des primes à verser : {:.2f} €".format(df_joint["prime_montant_eur"].sum()))
    logger.info(f"Répartition par activité sportive :\n{df_joint['activite_clean'].value_counts().to_string()}")
    if "bu" in df_joint.columns:
        logger.info(f"Répartition par BU :\n{df_joint['bu'].value_counts().to_string()}")

    # --- Export du tableau final vers MinIO ---
    import io
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_joint.to_excel(writer, index=False)
    output.seek(0)
    try:
        helper.client.put_object(
            Bucket=helper.bucket,
            Key=MINIO_EXPORT_KEY,
            Body=output,
            ContentLength=output.getbuffer().nbytes,
        )
        logger.success(f"✅ Tableau des bénéficiaires/primes uploadé sur MinIO : {MINIO_EXPORT_KEY}")
    except Exception as e:
        logger.error(f"Erreur upload MinIO : {e}")

    # --- Optionnel : insertion PostgreSQL ---
    try:
        engine = create_engine(DB_CONN_STRING)
        df_joint.to_sql("beneficiaires_primes_sport", engine, if_exists="replace", index=False, schema="sportdata")
        logger.success("✅ Tableau des bénéficiaires/primes inséré dans PostgreSQL ('beneficiaires_primes_sport')")
    except Exception as e:
        logger.error(f"Erreur insertion PostgreSQL : {e}")

    logger.info("=== Pipeline croisement RH/Sport et calcul prime terminé ===")

# ==========================================================================================
# 3. Point d’entrée
# ==========================================================================================
if __name__ == "__main__":
    try:
        pipeline_croisement_prime()
    except Exception as e:
        logger.error(f"❌ Pipeline interrompu : {e}")
        raise

# ==========================================================================================
# Fin du fichier – Croisement RH/Sport, calcul et reporting primes, export MinIO/SQL
# ==========================================================================================
