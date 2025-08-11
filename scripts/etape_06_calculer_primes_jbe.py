# ==========================================================================================
# Script      : etape_04_calculer_primes_jbe.py
# Objectif    : Calculer les primes sportives et journées bien-être à partir des activités simulées.
#               Stockage des résultats (MinIO Excel + PostgreSQL) + Contrôle qualité GE.
# Auteur      : Xavier Rousseau |  Août 2025
# ==========================================================================================

import os
import io
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger
import great_expectations as ge
from minio_helper import MinIOHelper

# ==========================================================================================
# 1. Chargement des variables d’environnement (.env)
# ==========================================================================================

load_dotenv(dotenv_path=".env", override=True)

# PostgreSQL
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# MinIO
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "sportdata")
MINIO_RH_KEY = "clean/donnees_rh_cleaned.xlsx"
MINIO_SPORT_KEY = "clean/activites_sportives_simulees.xlsx"
MINIO_EXPORT_PRIMES = "gold/beneficiaires_primes_sportives.xlsx"
MINIO_EXPORT_BE = "gold/beneficiaires_journees_bien_etre.xlsx"

# Paramètres RH (configurables)
SEUIL_ACTIVITES_PRIME = int(os.getenv("SEUIL_ACTIVITES_PRIME", 10))
POURCENTAGE_PRIME = float(os.getenv("POURCENTAGE_PRIME", 0.05))
JOURNEES_BIEN_ETRE = int(os.getenv("NB_JOURNEES_BIEN_ETRE", 5))

# ==========================================================================================
# 2. Chargement des données sources (RH et activités)
# ==========================================================================================

def charger_donnees():
    helper = MinIOHelper()
    logger.info("📥 Chargement des fichiers depuis MinIO...")

    df_rh = helper.read_excel(MINIO_RH_KEY)
    df_activites = helper.read_excel(MINIO_SPORT_KEY)

    logger.success(f"✅ Données RH : {len(df_rh)} lignes chargées.")
    logger.success(f"✅ Activités sportives : {len(df_activites)} lignes chargées.")
    return df_rh, df_activites

# ==========================================================================================
# 3. Calcul des bénéficiaires (primes sportives et bien-être)
# ==========================================================================================

def calculer_beneficiaires(df_rh, df_activites):
    logger.info("📊 Calcul des bénéficiaires (primes + JBE)...")

    # Agréger le nombre d'activités par salarié
    df_agg = df_activites.groupby(["id_salarie", "nom", "prenom"]).agg(
        nb_activites=pd.NamedAgg(column="uid", aggfunc="count")
    ).reset_index()

    logger.info(f"ℹ️ {len(df_agg)} salariés ayant des activités sportives enregistrées.")

    # Bénéficiaires primes sportives (seuil + % sur salaire)
    df_agg = df_agg.merge(df_rh[["id_salarie", "salaire_brut_annuel"]], on="id_salarie", how="left")
    df_primes = df_agg[df_agg["nb_activites"] >= SEUIL_ACTIVITES_PRIME].copy()
    df_primes["prime_montant_eur"] = (df_primes["salaire_brut_annuel"] * POURCENTAGE_PRIME).round(2)

    # Bénéficiaires JBE (seuil fixe sur nombre d'activités)
    df_bien_etre = df_agg[df_agg["nb_activites"] >= SEUIL_ACTIVITES_PRIME].copy()
    df_bien_etre["nb_journees_bien_etre"] = JOURNEES_BIEN_ETRE

    logger.success(f"✅ {len(df_primes)} salariés bénéficieront d'une prime sportive.")
    logger.success(f"✅ {len(df_bien_etre)} salariés bénéficieront de journées bien-être.")

    return df_primes, df_bien_etre

# ==========================================================================================
# 4. Export des résultats vers MinIO (Excel)
# ==========================================================================================

def exporter_minio(df, key, label):
    helper = MinIOHelper()
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    helper.client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=buffer,
        ContentLength=buffer.getbuffer().nbytes
    )
    logger.success(f"📤 {label} exporté dans MinIO : {key}")

# ==========================================================================================
# 5. Insertion dans PostgreSQL (append, sans doublons sur uid)
# ==========================================================================================

def inserer_postgresql(df, table_name, unique_field="uid"):
    engine = create_engine(DB_CONN_STRING)
    with engine.begin() as conn:
        existing_uids = pd.read_sql(f"SELECT {unique_field} FROM sportdata.{table_name}", conn)
    df_to_insert = df[~df[unique_field].isin(existing_uids[unique_field])]
    if df_to_insert.empty:
        logger.info(f"ℹ️ Aucune nouvelle donnée à insérer dans sportdata.{table_name}")
        return
    df_to_insert.to_sql(table_name, engine, if_exists="append", index=False, schema="sportdata")
    logger.success(f"✅ {len(df_to_insert)} lignes insérées dans sportdata.{table_name}")

# ==========================================================================================
# 6. Contrôle qualité avec Great Expectations
# ==========================================================================================

def controle_qualite_ge(df):
    ge_df = ge.from_pandas(df)
    expectations = [
        ("expect_column_values_to_not_be_null", {"column": "id_salarie"}),
        ("expect_column_values_to_be_between", {"column": "prime_montant_eur", "min_value": 0}),
        ("expect_column_values_to_be_between", {"column": "nb_activites", "min_value": SEUIL_ACTIVITES_PRIME}),
    ]
    for exp_type, kwargs in expectations:
        getattr(ge_df, exp_type)(**kwargs)
    result = ge_df.validate(result_format="SUMMARY")

    # Rapport HTML
    report_name = f"validation/rapport_GE_primes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    html = ge.render.renderer.ValidationResultsPageRenderer().render(result)
    page_html = ge.render.view.DefaultJinjaPageView().render(html)
    with open("/tmp/rapport_GE.html", "w", encoding="utf-8") as f:
        f.write(page_html)

    helper = MinIOHelper()
    helper.upload_file("/tmp/rapport_GE.html", report_name)
    logger.success(f"📄 Rapport GE exporté dans MinIO : {report_name}")
# ==========================================================================================
# 7. Pipeline principal (corrigé pour insertion bien-être)
# ==========================================================================================

def pipeline_primes_jbe():
    df_rh, df_activites = charger_donnees()
    df_primes, df_bien_etre = calculer_beneficiaires(df_rh, df_activites)

    if df_primes.empty:
        logger.warning("⚠️ Aucun salarié éligible à une prime sportive.")
    else:
        exporter_minio(df_primes, MINIO_EXPORT_PRIMES, "Primes sportives")
        inserer_postgresql(df_primes, "beneficiaires_primes_sport", unique_field="id_salarie")
        controle_qualite_ge(df_primes)

    if df_bien_etre.empty:
        logger.warning("⚠️ Aucun salarié éligible aux journées bien-être.")
    else:
        # *** Correction ici : sélectionner seulement les colonnes SQL ***
        colonnes_sql = ["id_salarie", "nb_activites", "nb_journees_bien_etre"]
        df_be_sql = df_bien_etre[colonnes_sql].copy()
        exporter_minio(df_be_sql, MINIO_EXPORT_BE, "Journées bien-être")
        inserer_postgresql(df_be_sql, "beneficiaires_journees_bien_etre", unique_field="id_salarie")

    logger.success("🎯 Pipeline primes et bien-être terminé avec succès.")

# ==========================================================================================
# 8. Main
# ==========================================================================================

if __name__ == "__main__":
    try:
        pipeline_primes_jbe()
    except Exception as e:
        logger.error(f"❌ Erreur pipeline : {e}")
        raise
