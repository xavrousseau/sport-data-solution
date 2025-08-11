# ==========================================================================================
# Script      : 02_nettoyer_donnees_rh.py
# Objectif    : Audit exploratoire, nettoyage RH, filtrage éligibilité via Google Maps API,
#               validation qualité Great Expectations, export vers MinIO & PostgreSQL.
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import sys
import io
import re
import uuid
import requests
import unicodedata
import tempfile
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine
from minio_helper import MinIOHelper

# Great Expectations
import great_expectations as ge
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================

load_dotenv(dotenv_path=".env", override=True)

# MinIO (chemins unifiés)
MINIO_SOURCE_KEY = "inputs/donnees_rh.xlsx"
MINIO_CLEANED_KEY = "clean/donnees_rh_cleaned.xlsx"
MINIO_EXCLUS_KEY = "clean/donnees_rh_exclus.xlsx"
TMP_DIR = "/tmp"

# Google Maps
ADRESSE_TRAVAIL = "1362 Avenue des Platanes, 34970 Lattes, France"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("❌ GOOGLE_API_KEY manquante dans .env")

# PostgreSQL
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Seuils d’éligibilité RH
DISTANCE_MAX_MARCHE_KM = 15
DISTANCE_MAX_VELO_KM = 25
MODES_TRANSPORT = {"marche": DISTANCE_MAX_MARCHE_KM, "vélo": DISTANCE_MAX_VELO_KM}
MAPPING_GOOGLE_API = {"marche": "walking", "vélo": "bicycling"}

# ==========================================================================================
# 2. Mapping modes de transport autorisés
# ==========================================================================================

MAPPING_TRANSPORT = {
    # Modes "marche"
    "marche": "marche",
    "marcher": "marche",
    "marche a pied": "marche",
    "marche à pied": "marche",
    "marche running": "marche",
    "marche/running": "marche",
    "running": "marche",
    "run": "marche",
    "course a pied": "marche",
    "course à pied": "marche",
    "courir": "marche",
    "walk": "marche",
    "foot": "marche",
    "a pied": "marche",
    "à pied": "marche",

    # Modes "vélo"
    "velo": "vélo",
    "vélo": "vélo",
    "velo electrique": "vélo",
    "vélo electrique": "vélo",
    "vélo pliant": "vélo",
    "velo pliant": "vélo",
    "velo assistance electrique": "vélo",
    "vélo assistance electrique": "vélo",
    "bicycle": "vélo",
    "bike": "vélo",
    "vélo tout terrain": "vélo",
    "vtt": "vélo",

    # Moyens doux (assimilés vélo)
    "trottinette": "vélo",
    "trottinette electrique": "vélo",
    "roller": "vélo",
    "rollers": "vélo",
    "skateboard": "vélo",
    "skate": "vélo",
    "gyroroue": "vélo",
    "hoverboard": "vélo",
    "monoroue": "vélo",
    "segway": "vélo",
    "patinette": "vélo",

    # Variantes projet/réelles multi-modes
    "velo trottinette autres": "vélo",
    "vélo trottinette autres": "vélo",
    "velo trottinette": "vélo",
    "vélo trottinette": "vélo",

    # Synonymes internationaux/anglais
    "scooter": "vélo",       # usage trottinette seulement !
    "e scooter": "vélo",
    "e bike": "vélo",
    "e-bike": "vélo",
    "kick scooter": "vélo",
    "push scooter": "vélo"
}

# ==========================================================================================
# 3. Fonctions de normalisation et eligibility
# ==========================================================================================

def normaliser_mode_transport(mode):
    """Normalise une chaîne : accents, ponctuation, casse, etc."""
    if not isinstance(mode, str):
        return None
    txt = unicodedata.normalize('NFD', mode).encode('ascii', 'ignore').decode('utf-8').lower()
    txt = re.sub(r"[-_/]", " ", txt)
    txt = re.sub(r"[^\w\s]", "", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return MAPPING_TRANSPORT.get(txt)

def normaliser_colonnes(df):
    """Nettoie les noms de colonnes pour ETL"""
    return df.rename(columns=lambda col: unicodedata.normalize('NFD', col)
                     .encode('ascii', 'ignore').decode()
                     .lower().strip()
                     .replace(" ", "_")
                     .replace("-", "_")
                     .replace("'", "_")
                     .replace("’", "_"))

def get_mode_api(mode_projet):
    return MAPPING_GOOGLE_API.get(mode_projet)

def calculer_distance_km(adresse_depart, mode_projet):
    """Appel Google API pour calculer la distance à pied ou vélo"""
    if not adresse_depart or not mode_projet:
        return None
    mode_api = get_mode_api(mode_projet)
    if not mode_api:
        return None
    params = {
        "origins": adresse_depart,
        "destinations": ADRESSE_TRAVAIL,
        "key": GOOGLE_API_KEY,
        "mode": mode_api
    }
    try:
        response = requests.get("https://maps.googleapis.com/maps/api/distancematrix/json", params=params, timeout=10)
        data = response.json()
        if data["rows"][0]["elements"][0]["status"] != "OK":
            return None
        distance_m = data["rows"][0]["elements"][0]["distance"]["value"]
        return distance_m / 1000
    except Exception as e:
        logger.warning(f"⚠️ Erreur Google Maps : {e}")
        return None

def verifier_eligibilite(adresse, mode):
    """Retourne (bool, motif, distance_km, mode_normalisé)"""
    if not adresse:
        return False, "Adresse manquante", None, None
    if not mode:
        return False, "Mode manquant", None, None
    mode_proj = normaliser_mode_transport(mode)
    if not mode_proj:
        return False, f"Mode non reconnu ({mode})", None, None
    distance = calculer_distance_km(adresse, mode_proj)
    if distance is None:
        return False, "Distance inconnue", None, mode_proj
    if distance > MODES_TRANSPORT[mode_proj]:
        return False, f"{distance:.1f} km > seuil", distance, mode_proj
    return True, "", distance, mode_proj

# ==========================================================================================
# 4. Analyse exploratoire RH
# ==========================================================================================

def analyse_exploratoire_avancee(df, description="Analyse exploratoire RH"):
    """Audit complet du DataFrame + mapping dynamique modes de transport"""
    logger.info(f"\n{'='*80}\n{description}\n{'='*80}")
    logger.info(f"Nombre de lignes        : {len(df)}")
    logger.info(f"Nombre de colonnes      : {len(df.columns)}")
    logger.info(f"Colonnes disponibles    : {list(df.columns)}")
    logger.info("\n--- Valeurs manquantes par colonne ---")
    logger.info(df.isnull().sum().to_string())
    logger.info("\n--- Unicité par colonne (nb valeurs uniques) ---")
    logger.info(df.nunique().sort_values(ascending=False).to_string())

    for col in df.columns:
        logger.info(f"\n==> Colonne : {col}")
        logger.info(f"Type : {df[col].dtype}")
        logger.info(f"Valeurs uniques (5 premières) : {df[col].dropna().unique()[:5]}")
        if df[col].dtype in ['object', 'string']:
            logger.info(f"Top 5 valeurs fréquentes :\n{df[col].value_counts(dropna=False).head(5)}")
        if pd.api.types.is_numeric_dtype(df[col]):
            logger.info(f"Stats numériques :\n{df[col].describe().to_string()}")
        if "date" in col:
            logger.info(f"Exemples de dates : {df[col].dropna().astype(str).unique()[:5]}")
    logger.info("\n--- Aperçu des 10 premières lignes ---")
    logger.info("\n" + df.head(10).to_string())
    logger.info("\n--- Aperçu des 10 dernières lignes ---")
    logger.info("\n" + df.tail(10).to_string())

    # Audit dynamique mapping des modes de transport
    if "moyen_de_deplacement" in df.columns:
        logger.info("\n== Audit mapping modes de transport ==")
        modes_uniques = sorted(df["moyen_de_deplacement"].dropna().unique())
        modes_non_reconnus = []
        for mode in modes_uniques:
            mapped = normaliser_mode_transport(mode)
            logger.info(f"Mode trouvé: '{mode}' --> mapping projet: '{mapped}'")
            if mapped is None:
                modes_non_reconnus.append(mode)
        if modes_non_reconnus:
            logger.warning(f"Modes non reconnus à compléter dans le mapping: {modes_non_reconnus}")
        logger.info(f"Répartition des modes de transport:\n{df['moyen_de_deplacement'].value_counts(dropna=False).to_string()}")


# ==========================================================================================
# 5. Pipeline complet
# ==========================================================================================

def pipeline_nettoyage_rh():
    logger.info("🚀 Début pipeline RH")
    helper = MinIOHelper()

    # Téléchargement Excel RH
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        helper.client.download_file(helper.bucket, MINIO_SOURCE_KEY, tmpfile.name)
        df = pd.read_excel(tmpfile.name)
    df = normaliser_colonnes(df)
    analyse_exploratoire_avancee(df)

    champs_requis = ["id_salarie", "nom", "prenom", "adresse_du_domicile", "moyen_de_deplacement"]
    if not all(col in df.columns for col in champs_requis):
        raise ValueError("❌ Colonnes essentielles manquantes")

    # Eligibility
    eligibles, exclus = [], []
    for _, row in df.iterrows():
        is_ok, motif, dist, mode = verifier_eligibilite(row["adresse_du_domicile"], row["moyen_de_deplacement"])
        ligne = {
            "uid": str(uuid.uuid4()),
            "id_salarie": row.get("id_salarie"),
            "nom": row.get("nom"),
            "prenom": row.get("prenom"),
            "adresse_du_domicile": row.get("adresse_du_domicile"),
            "moyen_de_deplacement": row.get("moyen_de_deplacement"),
            "mode_normalise": mode,
            "distance_km": dist,
            "eligible": is_ok,
            "motif_exclusion": motif,
            "salaire_brut_annuel": int(row.get("salaire_brut", 0) or 0)
        }
        (eligibles if is_ok else exclus).append(ligne)

    df_ok = pd.DataFrame(eligibles)
    df_ko = pd.DataFrame(exclus)

    df_ok["deplacement_sportif"] = df_ok["mode_normalise"].isin(["marche", "vélo"])

    logger.info(f"✅ {len(df_ok)} éligibles / {len(df_ko)} exclus")

    # Great Expectations
    if not df_ok.empty:
        ge_df = ge.from_pandas(df_ok)
        ge_df.expect_column_values_to_not_be_null(column="id_salarie")
        ge_df.expect_column_values_to_be_between(column="distance_km", min_value=0, max_value=100)
        ge_df.expect_column_values_to_be_of_type(column="deplacement_sportif", type_="bool")

        result = ge_df.validate(result_format="SUMMARY")
        if not result.success:
            raise Exception("❌ Échec validation Great Expectations")

        html = DefaultJinjaPageView().render(ValidationResultsPageRenderer().render(result))
        report_key = f"validation/rapport_GE_RH_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        report_path = os.path.join(TMP_DIR, "rapport_ge.html")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html)
        helper.client.upload_file(report_path, helper.bucket, report_key)
        logger.success(f"📄 Rapport GE uploadé : {report_key}")

    # Upload Excel MinIO
    def upload_excel(df, key, label):
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False)
        buffer.seek(0)
        helper.client.put_object(Bucket=helper.bucket, Key=key, Body=buffer,
                                 ContentLength=buffer.getbuffer().nbytes)
        logger.success(f"📤 Upload {label} : {key}")

    if not df_ok.empty:
        upload_excel(df_ok, MINIO_CLEANED_KEY, "RH éligibles")
    if not df_ko.empty:
        upload_excel(df_ko, MINIO_EXCLUS_KEY, "RH exclus")

    # Export vers PostgreSQL
    if not df_ok.empty:
        engine = create_engine(DB_CONN_STRING)
        df_ok.to_sql("employes", engine, if_exists="replace", index=False, schema="sportdata")
        logger.success("🗃️ Données RH insérées dans PostgreSQL")
        engine.dispose()

    logger.info("🏁 Fin du pipeline RH")

# ==========================================================================================
# Point d’entrée CLI
# ==========================================================================================

if __name__ == "__main__":
    try:
        pipeline_nettoyage_rh()
    except Exception as e:
        logger.error(f"💥 Pipeline échoué : {e}")
        sys.exit(1)
