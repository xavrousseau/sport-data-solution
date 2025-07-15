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
from datetime import datetime
import os
import io
from dotenv import load_dotenv

# ==========================================================================================
# 1. Paramètres globaux
# ==========================================================================================

# Chargement .env
load_dotenv(dotenv_path=".env", override=True)

# Accès MinIO (fichiers nettoyés)
MINIO_RH_KEY = "raw/donnees_rh_cleaned.xlsx"
MINIO_SPORT_KEY = "raw/donnees_sportives_cleaned.xlsx"
MINIO_EXPORT_KEY = "final/beneficiaires_primes_sportives.xlsx"
TMP_DIR = "/tmp"

# Connexion PostgreSQL
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Montant de la prime forfaitaire (euros)
PRIME_MONTANT = 100

# ==========================================================================================
# 2. Chargement des fichiers depuis MinIO
# ==========================================================================================

def charger_fichier_minio(helper, key):
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        helper.client.download_file(Bucket=helper.bucket, Key=key, Filename=tmpfile.name)
        logger.success(f"✅ Fichier {key} téléchargé depuis MinIO")
        return pd.read_excel(tmpfile.name)

# ==========================================================================================
# 3. Pipeline principal de croisement et calcul des primes
# ==========================================================================================

def pipeline_croisement_prime():
    logger.info("=== Démarrage du croisement RH/Sport et calcul de prime ===")
    helper = MinIOHelper()

    # --- Chargement des deux jeux de données
    df_rh = charger_fichier_minio(helper, MINIO_RH_KEY)
    df_sport = charger_fichier_minio(helper, MINIO_SPORT_KEY)

    logger.info(f"Salariés RH éligibles         : {len(df_rh)}")
    logger.info(f"Activités sportives valides  : {len(df_sport)}")

    # --- Identification des colonnes ID salarié
    col_id_rh = next((c for c in df_rh.columns if "id" in c.lower() and "salarie" in c.lower()), "id_salarie")
    col_id_sport = next((c for c in df_sport.columns if "id" in c.lower() and "salarie" in c.lower()), "id_salarie")

    # --- Jointure inner : salariés RH qui ont une activité reconnue
    df_joint = pd.merge(
        df_rh,
        df_sport,
        how="inner",
        left_on=col_id_rh,
        right_on=col_id_sport,
        suffixes=('_rh', '_sport')
    )

    logger.info(f"Bénéficiaires potentiels      : {len(df_joint)}")

    # --- Agrégation par salarié (1 ligne = 1 prime)
    grouped = df_joint.groupby([col_id_rh, "nom_rh", "prenom_rh"]).agg({
        "type_activite": "count"
    }).reset_index().rename(columns={"type_activite": "nb_activites"})

    grouped["prime_eligible"] = True
    grouped["prime_montant_eur"] = PRIME_MONTANT
    grouped["date_prime"] = datetime.today().strftime("%Y-%m-%d")

    logger.info("\n--- Reporting synthétique ---")
    logger.info(f"Nombre de bénéficiaires       : {len(grouped)}")
    logger.info(f"Montant total des primes      : {grouped['prime_montant_eur'].sum():.2f} €")
    logger.info(f"Moyenne d'activités par salarié : {grouped['nb_activites'].mean():.2f}")

    # --- Export MinIO (.xlsx)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        grouped.to_excel(writer, index=False)
    output.seek(0)
    try:
        helper.client.put_object(
            Bucket=helper.bucket,
            Key=MINIO_EXPORT_KEY,
            Body=output,
            ContentLength=output.getbuffer().nbytes,
        )
        logger.success(f"✅ Résultat exporté dans MinIO : {MINIO_EXPORT_KEY}")
    except Exception as e:
        logger.error(f"Erreur export MinIO : {e}")

    # --- Insertion PostgreSQL
    try:
        engine = create_engine(DB_CONN_STRING)
        grouped.to_sql("beneficiaires_primes_sport", engine, if_exists="replace", index=False, schema="sportdata")
        logger.success("✅ Résultat inséré dans PostgreSQL (table sportdata.beneficiaires_primes_sport)")
    except Exception as e:
        logger.error(f"Erreur PostgreSQL : {e}")

    logger.info("=== Pipeline terminé avec succès ===")

    # --- Vérification qualité (Great Expectations)
    from great_expectations.dataset import PandasDataset
    from great_expectations.render.renderer import ValidationResultsPageRenderer
    from great_expectations.render.view import DefaultJinjaPageView

    ge_df = PandasDataset(grouped)

    expectations = [
        ("expect_column_values_to_not_be_null", {"column": "id_salarie"}),
        ("expect_column_values_to_not_be_null", {"column": "nom_rh"}),
        ("expect_column_values_to_not_be_null", {"column": "prenom_rh"}),
        ("expect_column_values_to_be_between", {"column": "nb_activites", "min_value": 1}),
        ("expect_column_values_to_be_of_type", {"column": "prime_montant_eur", "type_": "int64"}),
        ("expect_column_values_to_be_between", {"column": "prime_montant_eur", "min_value": 1, "max_value": 1000}),
    ]

    all_ok = True
    for exp, kwargs in expectations:
        result = getattr(ge_df, exp)(**kwargs)
        if not result.success:
            all_ok = False
            logger.error(f"❌ Échec GE : {exp} -- {kwargs}")
        else:
            logger.info(f"✅ OK GE : {exp} -- {kwargs}")

    if not all_ok:
        raise Exception("Échec de validation Great Expectations – pipeline interrompu.")
    else:
        logger.success("✅ Toutes les validations Great Expectations sont passées.")

    # --- Génération du rapport HTML
    rendered = ValidationResultsPageRenderer().render(result)
    html = DefaultJinjaPageView().render(rendered)

    report_name = f"validation_reports/rapport_GE_primes_{datetime.now().strftime('%Y%m%d')}.html"
    report_path = os.path.join(TMP_DIR, "rapport_GE.html")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    try:
        helper.client.fput_object(
            Bucket=helper.bucket,
            Key=report_name,
            Filename=report_path
        )
        logger.success(f"📄 Rapport GE HTML exporté dans MinIO : {report_name}")
    except Exception as e:
        logger.error(f"Erreur upload rapport GE : {e}")

# ==========================================================================================
# 4. Point d’entrée
# ==========================================================================================

if __name__ == "__main__":
    try:
        pipeline_croisement_prime()
    except Exception as e:
        logger.error(f"❌ Pipeline interrompu : {e}")
        raise
