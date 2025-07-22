# ==========================================================================================
# Script      : croiser_rh_sport_et_calculer_prime.py
# Objectif    : Croisement RH/Sport, calcul des bénéficiaires et montant de la prime
#               + attribution des journées bien-être (≥15 activités)
#               + export (MinIO/PostgreSQL), validation Great Expectations
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import io
import tempfile
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine
from minio_helper import MinIOHelper
import requests
# ==========================================================================================
# 1. Paramètres globaux et environnement
# ==========================================================================================

load_dotenv(dotenv_path=".env", override=True)

MINIO_RH_KEY = "raw/donnees_rh_cleaned.xlsx"
MINIO_SPORT_KEY = "raw/donnees_sportives_cleaned.xlsx"
MINIO_EXPORT_KEY = "final/beneficiaires_primes_sportives.xlsx"
MINIO_EXPORT_BE_KEY = "final/beneficiaires_journees_bien_etre.xlsx"
TMP_DIR = "/tmp"

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

PRIME_MONTANT = 100

# ==========================================================================================
# 2. Fonction utilitaire : chargement depuis MinIO
# ==========================================================================================

def charger_fichier_minio(helper, key):
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        helper.client.download_file(Bucket=helper.bucket, Key=key, Filename=tmpfile.name)
        logger.success(f"📥 Fichier téléchargé depuis MinIO : {key}")
        return pd.read_excel(tmpfile.name)

# ==========================================================================================
# 3. Pipeline principal : croisement, calcul, export, validation
# ==========================================================================================

def pipeline_croisement_prime():
    logger.info("=== DÉMARRAGE DU PIPELINE : Prime + Journées Bien-Être ===")
    helper = MinIOHelper()

    df_rh = charger_fichier_minio(helper, MINIO_RH_KEY)
    df_sport = charger_fichier_minio(helper, MINIO_SPORT_KEY)

    logger.info(f"📊 Salariés RH éligibles        : {len(df_rh)}")
    logger.info(f"📊 Activités sportives valides : {len(df_sport)}")

    # --- Jointure RH + Activités sportives
    df_joint = pd.merge(df_rh, df_sport, on="id_salarie", how="inner")
    logger.info(f"🔗 Bénéficiaires potentiels     : {len(df_joint)}")

    # --- Agrégation pour la prime
    grouped = df_joint.groupby(["id_salarie", "nom", "prenom"]).agg({
        "activite_clean": "count"
    }).reset_index().rename(columns={"activite_clean": "nb_activites"})

    grouped["prime_eligible"] = True
    grouped["prime_montant_eur"] = PRIME_MONTANT
    grouped["date_prime"] = datetime.today().strftime("%Y-%m-%d")

    logger.info("--- Synthèse Primes Sportives ---")
    logger.info(f"🎯 Nb bénéficiaires     : {len(grouped)}")
    logger.info(f"💶 Montant total primes : {grouped['prime_montant_eur'].sum():.2f} €")
    logger.info(f"📈 Activités moyennes   : {grouped['nb_activites'].mean():.2f}")

    # ======================================================================================
    # 4. Export primes vers MinIO (.xlsx)
    # ======================================================================================

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
        logger.success(f"📤 Exporté vers MinIO : {MINIO_EXPORT_KEY}")
    except Exception as e:
        logger.error(f"❌ Erreur export MinIO : {e}")

    # ======================================================================================
    # 5. Insertion PostgreSQL (prime)
    # ======================================================================================

    try:
        engine = create_engine(DB_CONN_STRING)
        grouped.to_sql("beneficiaires_primes_sport", engine, if_exists="replace", index=False, schema="sportdata")
        logger.success("🗃️ Table PostgreSQL : sportdata.beneficiaires_primes_sport")
    except Exception as e:
        logger.error(f"❌ Erreur PostgreSQL (prime) : {e}")

    # ======================================================================================
    # 6. Calcul des journées bien-être (≥15 activités/an)
    # ======================================================================================

    logger.info("=== Calcul des bénéficiaires des journées bien-être ===")

    df_nb_activites = df_sport.groupby("id_salarie").agg(
        nb_activites=('date_debut', 'count')
    ).reset_index()

    df_bien_etre = df_nb_activites[df_nb_activites["nb_activites"] >= 15].copy()
    df_bien_etre["nb_journees_bien_etre"] = 5

    logger.info(f"✅ {len(df_bien_etre)} salarié(s) éligible(s) aux journées bien-être.")

    # --- Export PostgreSQL (bien-être)
    try:
        df_bien_etre.to_sql("beneficiaires_journees_bien_etre", engine, if_exists="replace", index=False, schema="sportdata")
        logger.success("🗃️ Table PostgreSQL : sportdata.beneficiaires_journees_bien_etre")
    except Exception as e:
        logger.error(f"❌ Erreur PostgreSQL (bien-être) : {e}")

    # --- Export MinIO (bien-être)
    output_be = io.BytesIO()
    with pd.ExcelWriter(output_be, engine="xlsxwriter") as writer:
        df_bien_etre.to_excel(writer, index=False)
    output_be.seek(0)

    try:
        helper.client.put_object(
            Bucket=helper.bucket,
            Key=MINIO_EXPORT_BE_KEY,
            Body=output_be,
            ContentLength=output_be.getbuffer().nbytes,
        )
        logger.success(f"📤 Export MinIO : {MINIO_EXPORT_BE_KEY}")
    except Exception as e:
        logger.error(f"❌ Erreur export MinIO (bien-être) : {e}")

    # ======================================================================================
    # 7. Validation Great Expectations sur les primes
    # ======================================================================================

    from great_expectations.dataset import PandasDataset
    from great_expectations.render.renderer import ValidationResultsPageRenderer
    from great_expectations.render.view import DefaultJinjaPageView

    ge_df = PandasDataset(grouped)
    ge_df.expect_column_values_to_not_be_null("id_salarie")
    ge_df.expect_column_values_to_not_be_null("nom")
    ge_df.expect_column_values_to_not_be_null("prenom")
    ge_df.expect_column_values_to_be_between("nb_activites", min_value=1)
    ge_df.expect_column_values_to_be_of_type("prime_montant_eur", "int64")
    ge_df.expect_column_values_to_be_between("prime_montant_eur", 1, 1000)

    checkpoint_result = ge_df.validate(result_format="SUMMARY")
    if not checkpoint_result.success:
        logger.error("❌ Certaines expectations ont échoué.")
        raise Exception("Échec de validation Great Expectations – pipeline interrompu.")
    else:
        logger.success("✅ Toutes les validations Great Expectations sont passées.")

    # --- Génération rapport HTML
    rendered = ValidationResultsPageRenderer().render(checkpoint_result)
    html = DefaultJinjaPageView().render(rendered)

    report_name = f"validation_reports/rapport_GE_primes_{datetime.now().strftime('%Y%m%d')}.html"
    report_path = os.path.join(TMP_DIR, "rapport_GE.html")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    try:
        helper.client.upload_file(
            Filename=report_path,
            Bucket=helper.bucket,
            Key=report_name
        )
        logger.success(f"📄 Rapport GE HTML exporté dans MinIO : {report_name}")
    except Exception as e:
        logger.error(f"❌ Erreur upload rapport GE : {e}")



    message = (
        f"🎯 Pipeline terminé avec succès !\n"
        f"💶 {len(grouped)} primes sportives attribuées\n"
        f"🧘 {len(df_bien_etre)} journées bien-être accordées\n"
        f"🗃️ Données disponibles dans MinIO et PostgreSQL"
    )

    try:
        ntfy_url = "http://ntfy:8080/avantages-sportifs"
        requests.post(ntfy_url, data=message.encode("utf-8"))
        logger.success("🔔 Notification ntfy envoyée.")
    except Exception as e:
        logger.error(f"❌ Erreur envoi notification ntfy : {e}")

    logger.info("✅ Pipeline terminé avec succès.")

# ==========================================================================================
# 8. Exécution principale
# ==========================================================================================

if __name__ == "__main__":
    try:
        pipeline_croisement_prime()
    except Exception as e:
        logger.error(f"❌ Pipeline interrompu : {e}")

        # 🔔 Notification ntfy en cas d'échec
        import requests
        try:
            message = (
                "🚨 Échec du pipeline avantages RH ❌\n"
                f"⛔ Erreur : {str(e)}\n"
                "📋 Consultez les logs Airflow pour plus de détails."
            )
            ntfy_url = "http://ntfy:8080/avantages-sportifs"
            requests.post(ntfy_url, data=message.encode("utf-8"))
            logger.warning("🔔 Notification ntfy (échec) envoyée.")
        except Exception as ne:
            logger.error(f"⚠ Erreur envoi notification ntfy (échec) : {ne}")

        raise
