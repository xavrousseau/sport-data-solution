# ==========================================================================================
# Script      : etape_04_calculer_primes_jbe.py
# Objectif    : Croiser RH + activités sportives pour :
#               - calculer les primes sportives (5% du salaire annuel brut si déplacement sportif)
#               - identifier les bénéficiaires des journées bien-être (≥15 activités)
#               - exporter vers MinIO, PostgreSQL, notifier via ntfy, valider via GE
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import io
import tempfile
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
from minio_helper import MinIOHelper
from ntfy_helper import envoyer_resume_pipeline, envoyer_message_erreur
from sqlalchemy import create_engine, text

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================

load_dotenv(dotenv_path=".env", override=True)

MINIO_RH_KEY = "raw/donnees_rh_cleaned.xlsx"
MINIO_SPORT_KEY = "raw/activites_sportives_simulees.xlsx"
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

# ==========================================================================================
# 2. Fonction : téléchargement d’un fichier depuis MinIO
# ==========================================================================================

def charger_fichier_minio(helper, key):
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        helper.client.download_file(Bucket=helper.bucket, Key=key, Filename=tmpfile.name)
        logger.success(f"📥 Fichier téléchargé depuis MinIO : {key}")
        return pd.read_excel(tmpfile.name)

# ==========================================================================================
# 3. Pipeline principal : calcul des primes et journées bien-être
# ==========================================================================================

def pipeline_croisement_prime():
    logger.info("=== DÉMARRAGE DU PIPELINE : Prime + Journées Bien-Être ===")
    helper = MinIOHelper()

    df_rh = charger_fichier_minio(helper, MINIO_RH_KEY)
    df_sport = charger_fichier_minio(helper, MINIO_SPORT_KEY)

    logger.info(f"📊 Salariés RH éligibles        : {len(df_rh)}")
    logger.info(f"📊 Activités sportives valides : {len(df_sport)}")

    if 'date_debut' not in df_sport.columns:
        logger.warning("⚠️ Colonne 'date_debut' absente. Utilisation de 'date' comme fallback.")
        df_sport['date_debut'] = df_sport['date']
    else:
        logger.info("✅ Colonne 'date_debut' disponible dans les données sportives.")

    # Jointure RH + Sport
    df_joint = pd.merge(df_rh, df_sport, on="id_salarie", how="inner")
    logger.info(f"🔗 Bénéficiaires potentiels     : {len(df_joint)}")

    df_joint = df_joint.rename(columns={"nom_x": "nom", "prenom_x": "prenom"})

    if "deplacement_sportif" not in df_joint.columns:
        raise KeyError("❌ La colonne 'deplacement_sportif' est absente. Vérifiez le fichier RH nettoyé.")

    # Filtrage : ne garder que les trajets domicile-bureau déclarés comme sportifs
    df_joint = df_joint[df_joint["deplacement_sportif"] == True]
    logger.info(f"🚲 Déplacements sportifs retenus : {len(df_joint)}")

    # Calcul de la prime : 5% du salaire annuel brut
    grouped = df_joint.groupby(["id_salarie", "nom", "prenom", "salaire_brut_annuel"]).agg(
        nb_activites=("date_debut", "count")
    ).reset_index()

    grouped["prime_eligible"] = True
    grouped["prime_montant_eur"] = (grouped["salaire_brut_annuel"] * 0.05).round(2)
    grouped["date_prime"] = datetime.today().strftime("%Y-%m-%d")

    logger.info("--- Synthèse Primes Sportives ---")
    logger.info(f"🎯 Nb bénéficiaires     : {len(grouped)}")
    logger.info(f"💶 Montant total primes : {grouped['prime_montant_eur'].sum():.2f} €")
    logger.info(f"📈 Activités moyennes   : {grouped['nb_activites'].mean():.2f}")

    # Export MinIO
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        grouped.to_excel(writer, index=False)
    output.seek(0)

    helper.client.put_object(
        Bucket=helper.bucket,
        Key=MINIO_EXPORT_KEY,
        Body=output,
        ContentLength=output.getbuffer().nbytes,
    )
    logger.success(f"📤 Exporté vers MinIO : {MINIO_EXPORT_KEY}")

    # Export PostgreSQL
    engine = create_engine(DB_CONN_STRING)
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM sportdata.beneficiaires_primes_sport"))

    grouped.to_sql("beneficiaires_primes_sport", engine, if_exists="append", index=False, schema="sportdata")
    logger.success("🗃️ Table PostgreSQL : sportdata.beneficiaires_primes_sport (append)")


    # Journées bien-être
    logger.info("=== Calcul des bénéficiaires des journées bien-être ===")
    df_nb_activites = df_sport.groupby("id_salarie").agg(nb_activites=('date_debut', 'count')).reset_index()
    df_bien_etre = df_nb_activites[df_nb_activites["nb_activites"] >= 15].copy()
    df_bien_etre["nb_journees_bien_etre"] = 5

    logger.info(f"✅ {len(df_bien_etre)} salarié(s) éligible(s) aux journées bien-être.")

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM sportdata.beneficiaires_journees_bien_etre"))

    df_bien_etre.to_sql("beneficiaires_journees_bien_etre", engine, if_exists="append", index=False, schema="sportdata")
    logger.success("🗃️ Table PostgreSQL : sportdata.beneficiaires_journees_bien_etre (append)")


    output_be = io.BytesIO()
    with pd.ExcelWriter(output_be, engine="xlsxwriter") as writer:
        df_bien_etre.to_excel(writer, index=False)
    output_be.seek(0)

    helper.client.put_object(
        Bucket=helper.bucket,
        Key=MINIO_EXPORT_BE_KEY,
        Body=output_be,
        ContentLength=output_be.getbuffer().nbytes,
    )
    logger.success(f"📤 Export MinIO : {MINIO_EXPORT_BE_KEY}")

    # Validation GE + rapport
    from great_expectations.dataset import PandasDataset
    from great_expectations.render.renderer import ValidationResultsPageRenderer
    from great_expectations.render.view import DefaultJinjaPageView

    ge_df = PandasDataset(grouped)
    ge_df.expect_column_values_to_not_be_null("id_salarie")
    ge_df.expect_column_values_to_not_be_null("nom")
    ge_df.expect_column_values_to_not_be_null("prenom")
    ge_df.expect_column_values_to_be_between("nb_activites", min_value=1)
    ge_df.expect_column_values_to_be_of_type("prime_montant_eur", "float")
    ge_df.expect_column_values_to_be_between("prime_montant_eur", 1, 10000)

    checkpoint_result = ge_df.validate(result_format="SUMMARY")
    if not checkpoint_result.success:
        raise Exception("Échec de validation Great Expectations – pipeline interrompu.")
    logger.success("✅ Toutes les validations Great Expectations sont passées.")

    # Génération du rapport GE HTML directement en mémoire (sans fichier local)
    rendered = ValidationResultsPageRenderer().render(checkpoint_result)
    html = DefaultJinjaPageView().render(rendered)

    # Construction du nom de fichier MinIO
    report_name = f"validation_reports/rapport_GE_primes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

    # Encodage en bytes et upload dans MinIO
    html_bytes = html.encode("utf-8")
    helper.client.put_object(
        Bucket=helper.bucket,
        Key=report_name,
        Body=io.BytesIO(html_bytes),
        ContentLength=len(html_bytes),
        ContentType="text/html"
    )
    logger.success(f"📄 Rapport GE HTML exporté dans MinIO : {report_name}")

    # Construction de l’URL web pour accès via navigateur
    rapport_ge_url = f"http://localhost:9001/browser/sportdata/{report_name.replace('/', '%2F')}"
    logger.info(f"🔗 Rapport accessible ici : {rapport_ge_url}")

    # Envoi de la notification finale avec lien réel
    envoyer_resume_pipeline(grouped, df_bien_etre, rapport_ge_url)

# ==========================================================================================
# 4. Point d’entrée principal avec gestion d’erreurs
# ==========================================================================================

if __name__ == "__main__":
    try:
        pipeline_croisement_prime()
    except Exception as e:
        logger.error(f"❌ Pipeline interrompu : {e}")
        envoyer_message_erreur("avantages_sportifs", f"🚨 Échec du pipeline avantages RH ❌\n⛔ Erreur : {str(e)}\n📋 Consultez les logs Airflow pour plus de détails.")
        raise
