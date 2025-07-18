# ==========================================================================================
# Script      : croiser_rh_sport_et_calculer_prime.py
# Objectif    : Croisement RH/Sport, calcul des bénéficiaires et montant de la prime
#               Export table finale (MinIO/PostgreSQL), validation Great Expectations
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
# 1. Paramètres globaux et environnement
# ==========================================================================================

# Chargement du fichier .env
load_dotenv(dotenv_path=".env", override=True)

# Clés MinIO des fichiers nettoyés
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

# Montant forfaitaire attribué par prime
PRIME_MONTANT = 100

# ==========================================================================================
# 2. Fonction utilitaire : charger un fichier Excel depuis MinIO
# ==========================================================================================

def charger_fichier_minio(helper, key):
    """
    Télécharge un fichier depuis MinIO et le charge en DataFrame.
    """
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        helper.client.download_file(Bucket=helper.bucket, Key=key, Filename=tmpfile.name)
        logger.success(f"✅ Fichier {key} téléchargé depuis MinIO")
        return pd.read_excel(tmpfile.name)

# ==========================================================================================
# 3. Pipeline principal : croisement, calcul de prime, export, validation
# ==========================================================================================

def pipeline_croisement_prime():
    logger.info("=== Démarrage du croisement RH/Sport et calcul de prime ===")
    helper = MinIOHelper()

    # --- Chargement des fichiers RH et sport
    df_rh = charger_fichier_minio(helper, MINIO_RH_KEY)
    df_sport = charger_fichier_minio(helper, MINIO_SPORT_KEY)

    logger.info(f"Salariés RH éligibles         : {len(df_rh)}")
    logger.info(f"Activités sportives valides  : {len(df_sport)}")

    # --- Identification dynamique de la colonne ID salarié
    col_id_rh = next((c for c in df_rh.columns if "id" in c.lower() and "salarie" in c.lower()), "id_salarie")
    col_id_sport = next((c for c in df_sport.columns if "id" in c.lower() and "salarie" in c.lower()), "id_salarie")

    # --- Jointure entre RH et activités sportives
    df_joint = pd.merge(
        df_rh,
        df_sport,
        how="inner",
        left_on=col_id_rh,
        right_on=col_id_sport
    )

    logger.info(f"Bénéficiaires potentiels      : {len(df_joint)}")

    # --- Agrégation : 1 ligne = 1 salarié avec nombre d'activités
    grouped = df_joint.groupby([col_id_rh, "nom", "prenom"]).agg({
        "activite_clean": "count"
    }).reset_index().rename(columns={"activite_clean": "nb_activites"})

    # --- Ajout des colonnes liées à la prime
    grouped["prime_eligible"] = True
    grouped["prime_montant_eur"] = PRIME_MONTANT
    grouped["date_prime"] = datetime.today().strftime("%Y-%m-%d")

    # --- Affichage synthétique des résultats
    logger.info("\n--- Reporting synthétique ---")
    logger.info(f"Nombre de bénéficiaires       : {len(grouped)}")
    logger.info(f"Montant total des primes      : {grouped['prime_montant_eur'].sum():.2f} €")
    logger.info(f"Moyenne d'activités par salarié : {grouped['nb_activites'].mean():.2f}")

    # ======================================================================================
    # 4. Export vers MinIO (.xlsx)
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
        logger.success(f"✅ Résultat exporté dans MinIO : {MINIO_EXPORT_KEY}")
    except Exception as e:
        logger.error(f"Erreur export MinIO : {e}")

    # ======================================================================================
    # 5. Insertion dans PostgreSQL
    # ======================================================================================

    try:
        engine = create_engine(DB_CONN_STRING)
        grouped.to_sql("beneficiaires_primes_sport", engine, if_exists="replace", index=False, schema="sportdata")
        logger.success("✅ Résultat inséré dans PostgreSQL (table sportdata.beneficiaires_primes_sport)")
    except Exception as e:
        logger.error(f"Erreur PostgreSQL : {e}")

    logger.info("=== Pipeline terminé avec succès ===")

    # ======================================================================================
    # 6. Validation qualité avec Great Expectations
    # ======================================================================================

    from great_expectations.dataset import PandasDataset
    from great_expectations.render.renderer import ValidationResultsPageRenderer
    from great_expectations.render.view import DefaultJinjaPageView

    # Création d’un dataset GE à partir du DataFrame
    ge_df = PandasDataset(grouped)

    # Définition des tests à exécuter
    ge_df.expect_column_values_to_not_be_null(column="id_salarie")
    ge_df.expect_column_values_to_not_be_null(column="nom")
    ge_df.expect_column_values_to_not_be_null(column="prenom")
    ge_df.expect_column_values_to_be_between(column="nb_activites", min_value=1)
    ge_df.expect_column_values_to_be_of_type(column="prime_montant_eur", type_="int64")
    ge_df.expect_column_values_to_be_between(column="prime_montant_eur", min_value=1, max_value=1000)

    # Exécution globale des tests et génération de résultats enrichis
    checkpoint_result = ge_df.validate(result_format="SUMMARY")

    if not checkpoint_result.success:
        logger.error("❌ Certaines expectations ont échoué.")
        raise Exception("Échec de validation Great Expectations – pipeline interrompu.")
    else:
        logger.success("✅ Toutes les validations Great Expectations sont passées.")

    # ======================================================================================
    # 7. Génération du rapport HTML + upload MinIO
    # ======================================================================================

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
        logger.error(f"Erreur upload rapport GE : {e}")

# ==========================================================================================
# 8. Exécution
# ==========================================================================================

if __name__ == "__main__":
    try:
        pipeline_croisement_prime()
    except Exception as e:
        logger.error(f"❌ Pipeline interrompu : {e}")
        raise
