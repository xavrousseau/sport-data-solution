# ==========================================================================================
# Script      : 02_nettoyer_donnees_rh.py
# Objectif    : Audit exploratoire, nettoyage RH, filtrage éligibilité via Google Maps API,
#               validation qualité Great Expectations, export vers MinIO & PostgreSQL.
# Auteur      : Xavier Rousseau | Juillet 2025
# Modifs      : Robustesse API (batch + retry), Content-Type Excel, UPSERT Postgres, .env harmonisé
# ==========================================================================================

import os
import sys
import io
import re
import uuid
import time
import json
import requests
import unicodedata
import tempfile
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import psycopg2
from psycopg2.extras import execute_values

# Great Expectations
import great_expectations as ge
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView

# Helpers
from minio_helper import MinIOHelper

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================

# Priorité à /opt/airflow/.env (en exécution conteneur), fallback .env (dev)
try:
    load_dotenv(dotenv_path="/opt/airflow/.env", override=True)
except Exception:
    pass
load_dotenv(dotenv_path=".env", override=True)

# MinIO (chemins unifiés)
MINIO_SOURCE_KEY  = "inputs/donnees_rh.xlsx"
MINIO_CLEANED_KEY = "clean/donnees_rh_cleaned.xlsx"
MINIO_EXCLUS_KEY  = "clean/donnees_rh_exclus.xlsx"
TMP_DIR = "/tmp"

# Google Maps
ADRESSE_TRAVAIL = "1362 Avenue des Platanes, 34970 Lattes, France"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("❌ GOOGLE_API_KEY manquante dans .env")

# PostgreSQL (pas de valeurs par défaut sensibles)
POSTGRES_USER   = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_HOST   = os.getenv("POSTGRES_HOST", "sport-postgres")
POSTGRES_PORT   = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB     = os.getenv("POSTGRES_DB", "sportdata")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "sportdata")

DB_CONN_STRING = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Seuils d’éligibilité RH
DISTANCE_MAX_MARCHE_KM = 15
DISTANCE_MAX_VELO_KM   = 25
MODES_TRANSPORT = {"marche": DISTANCE_MAX_MARCHE_KM, "vélo": DISTANCE_MAX_VELO_KM}
MAPPING_GOOGLE_API = {"marche": "walking", "vélo": "bicycling"}


# ==========================================================================================
# 2. Mapping modes de transport autorisés
# ==========================================================================================

MAPPING_TRANSPORT = {
    # Modes "marche"
    "marche": "marche", "marcher": "marche", "marche a pied": "marche",
    "marche à pied": "marche", "marche running": "marche", "marche/running": "marche",
    "running": "marche", "run": "marche", "course a pied": "marche", "course à pied": "marche",
    "courir": "marche", "walk": "marche", "foot": "marche", "a pied": "marche", "à pied": "marche",

    # Modes "vélo"
    "velo": "vélo", "vélo": "vélo", "velo electrique": "vélo", "vélo electrique": "vélo",
    "vélo pliant": "vélo", "velo pliant": "vélo", "velo assistance electrique": "vélo",
    "vélo assistance electrique": "vélo", "bicycle": "vélo", "bike": "vélo",
    "vélo tout terrain": "vélo", "vtt": "vélo",

    # Moyens doux (assimilés vélo)
    "trottinette": "vélo", "trottinette electrique": "vélo", "roller": "vélo", "rollers": "vélo",
    "skateboard": "vélo", "skate": "vélo", "gyroroue": "vélo", "hoverboard": "vélo",
    "monoroue": "vélo", "segway": "vélo", "patinette": "vélo",

    # Variantes projet/réelles multi-modes
    "velo trottinette autres": "vélo", "vélo trottinette autres": "vélo",
    "velo trottinette": "vélo", "vélo trottinette": "vélo",

    # Synonymes internationaux/anglais
    "scooter": "vélo", "e scooter": "vélo", "e bike": "vélo", "e-bike": "vélo",
    "kick scooter": "vélo", "push scooter": "vélo"
}

# ==========================================================================================
# 3. Fonctions de normalisation et eligibility (logique conservée)
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

# ---- VERSION ORIGINALE (conservée pour compat) ----
def calculer_distance_km(adresse_depart, mode_projet):
    """Appel Google API (1 par adresse). Gardé pour compatibilité mais non utilisé en boucle."""
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
        r = requests.get(
            "https://maps.googleapis.com/maps/api/distancematrix/json",
            params=params, timeout=10
        )
        data = r.json()
        if data["rows"][0]["elements"][0]["status"] != "OK":
            return None
        distance_m = data["rows"][0]["elements"][0]["distance"]["value"]
        return distance_m / 1000
    except Exception as e:
        logger.warning(f"⚠️ Erreur Google Maps (single): {e}")
        return None

# ---- NOUVEAU : appels batchés + retries ----
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_DM_SESSION = requests.Session()
_DM_SESSION.mount("https://", HTTPAdapter(max_retries=Retry(
    total=3, backoff_factor=0.6, status_forcelist=(500, 502, 503, 504), raise_on_status=False
)))
_DM_TIMEOUT = (5, 20)
_DM_URL = "https://maps.googleapis.com/maps/api/distancematrix/json"

def _distance_matrix_batch(origins, destination, mode_api):
    """Retourne la distance (km) pour chaque origine (<=25), -1.0 si inconnue."""
    if not origins:
        return []
    params = {
        "origins": "|".join(origins),
        "destinations": destination,
        "mode": mode_api,
        "units": "metric",
        "key": GOOGLE_API_KEY
    }
    try:
        resp = _DM_SESSION.get(_DM_URL, params=params, timeout=_DM_TIMEOUT)
        if resp.status_code != 200:
            logger.warning(f"[GDM] HTTP {resp.status_code} — {resp.text[:160]}")
            return [-1.0] * len(origins)
        data = resp.json()
        if data.get("status") != "OK":
            logger.warning(f"[GDM] status != OK — {json.dumps(data)[:160]}")
            return [-1.0] * len(origins)
        distances = []
        for row in data.get("rows", []):
            el = (row.get("elements") or [{}])[0]
            if el.get("status") != "OK":
                distances.append(-1.0)
            else:
                meters = (el.get("distance") or {}).get("value")
                distances.append(float(meters) / 1000.0 if meters is not None else -1.0)
        if len(distances) != len(origins):
            distances += [-1.0] * (len(origins) - len(distances))
        return distances
    except Exception as e:
        logger.error(f"[GDM] Exception batch: {e!r}")
        return [-1.0] * len(origins)

def _compute_distances_batched(df, col_adresse, col_mode):
    """Batch par mode (walking/bicycling), lots de 25 origines."""
    distances = pd.Series([-1.0] * len(df), index=df.index, dtype=float)
    if not GOOGLE_API_KEY:
        logger.warning("⚠️ GOOGLE_API_KEY absent : distances à -1.0")
        return distances
    # walking puis bicycling
    for mproj, mapi in MAPPING_GOOGLE_API.items():
        idx = df.index[df[col_mode].map(normaliser_mode_transport) == mproj].tolist()
        j = 0
        while j < len(idx):
            batch_idx = idx[j:j+25]
            origins = [df.at[i, col_adresse] for i in batch_idx]
            res = _distance_matrix_batch(origins, ADRESSE_TRAVAIL, mapi)
            for bi, d in zip(batch_idx, res):
                distances.at[bi] = d
            j += 25
            time.sleep(0.1)
    return distances

def verifier_eligibilite(adresse, mode):
    """Retourne (bool, motif, distance_km, mode_normalisé) — logique conservée"""
    if not adresse:
        return False, "Adresse manquante", None, None
    if not mode:
        return False, "Mode manquant", None, None
    mode_proj = normaliser_mode_transport(mode)
    if not mode_proj:
        return False, f"Mode non reconnu ({mode})", None, None
    # on garde la version single pour compat d’appel au besoin
    distance = calculer_distance_km(adresse, mode_proj)
    if distance is None:
        return False, "Distance inconnue", None, mode_proj
    if distance > MODES_TRANSPORT[mode_proj]:
        return False, f"{distance:.1f} km > seuil", distance, mode_proj
    return True, "", distance, mode_proj

# ==========================================================================================
# 4. Analyse exploratoire RH (inchangé)
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
# 5. PostgreSQL — création table & UPSERT (au lieu de replace)
# ==========================================================================================

def _build_engine() -> Engine:
    return create_engine(DB_CONN_STRING, pool_pre_ping=True, future=True)

def _ensure_table(engine: Engine):
    with engine.begin() as conn:
        if POSTGRES_SCHEMA:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA};"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.employes (
                id_salarie           VARCHAR PRIMARY KEY,
                nom                  VARCHAR,
                prenom               VARCHAR,
                adresse_du_domicile  VARCHAR,
                moyen_de_deplacement VARCHAR,
                mode_normalise       VARCHAR,
                distance_km          DOUBLE PRECISION,
                eligible             BOOLEAN,
                motif_exclusion      VARCHAR,
                salaire_brut_annuel  INTEGER,
                deplacement_sportif  BOOLEAN,
                date_mise_a_jour     TIMESTAMP DEFAULT NOW()
            );
        """))

def _upsert_employes(engine: Engine, df: pd.DataFrame):
    if df.empty:
        return
    records = list(df[[
        "id_salarie", "nom", "prenom", "adresse_du_domicile",
        "moyen_de_deplacement", "mode_normalise", "distance_km",
        "eligible", "motif_exclusion", "salaire_brut_annuel",
        "deplacement_sportif"
    ]].itertuples(index=False, name=None))
    with engine.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            query = f"""
            INSERT INTO {POSTGRES_SCHEMA}.employes
            (id_salarie, nom, prenom, adresse_du_domicile, moyen_de_deplacement,
             mode_normalise, distance_km, eligible, motif_exclusion,
             salaire_brut_annuel, deplacement_sportif)
            VALUES %s
            ON CONFLICT (id_salarie) DO UPDATE SET
                nom = EXCLUDED.nom,
                prenom = EXCLUDED.prenom,
                adresse_du_domicile = EXCLUDED.adresse_du_domicile,
                moyen_de_deplacement = EXCLUDED.moyen_de_deplacement,
                mode_normalise = EXCLUDED.mode_normalise,
                distance_km = EXCLUDED.distance_km,
                eligible = EXCLUDED.eligible,
                motif_exclusion = EXCLUDED.motif_exclusion,
                salaire_brut_annuel = EXCLUDED.salaire_brut_annuel,
                deplacement_sportif = EXCLUDED.deplacement_sportif,
                date_mise_a_jour = NOW();
            """
            execute_values(cur, query, records, page_size=1000)

# ==========================================================================================
# 6. Pipeline complet (avec appels Google batchés)
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

    # Distances en LOTS (beaucoup plus rapide et robuste), puis eligibility
    df["mode_normalise"] = df["moyen_de_deplacement"].apply(normaliser_mode_transport)
    df["distance_km"] = _compute_distances_batched(df, "adresse_du_domicile", "moyen_de_deplacement")

    eligibles, exclus = [], []
    for _, row in df.iterrows():
        mode_proj = row["mode_normalise"]
        dist = row["distance_km"]
        if mode_proj is None:
            is_ok, motif = False, f"Mode non reconnu ({row['moyen_de_deplacement']})"
        elif dist < 0:
            is_ok, motif = False, "Distance inconnue"
        elif dist > MODES_TRANSPORT[mode_proj]:
            is_ok, motif = False, f"{dist:.1f} km > seuil"
        else:
            is_ok, motif = True, ""

        ligne = {
            "uid": str(uuid.uuid4()),
            "id_salarie": row.get("id_salarie"),
            "nom": row.get("nom"),
            "prenom": row.get("prenom"),
            "adresse_du_domicile": row.get("adresse_du_domicile"),
            "moyen_de_deplacement": row.get("moyen_de_deplacement"),
            "mode_normalise": mode_proj,
            "distance_km": (None if dist < 0 else float(dist)),
            "eligible": is_ok,
            "motif_exclusion": motif,
            "salaire_brut_annuel": int(row.get("salaire_brut", 0) or 0)
        }
        (eligibles if is_ok else exclus).append(ligne)

    df_ok = pd.DataFrame(eligibles)
    df_ko = pd.DataFrame(exclus)
    if not df_ok.empty:
        df_ok["deplacement_sportif"] = df_ok["mode_normalise"].isin(["marche", "vélo"])

    logger.info(f"✅ {len(df_ok)} éligibles / {len(df_ko)} exclus")

    # Great Expectations (inchangé)
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
        # ContentType explicite
        with open(report_path, "rb") as fh:
            helper.client.put_object(
                Bucket=helper.bucket, Key=report_key, Body=fh,
                ContentLength=os.path.getsize(report_path),
                ContentType="text/html",
            )
        logger.success(f"📄 Rapport GE uploadé : {report_key}")

    # Upload Excel MinIO (avec ContentType explicite)
    def upload_excel(df, key, label):
        if df.empty:
            return
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False)
        buffer.seek(0)
        helper.client.put_object(
            Bucket=helper.bucket,
            Key=key,
            Body=buffer,
            ContentLength=buffer.getbuffer().nbytes,
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        logger.success(f"📤 Upload {label} : {key}")

    upload_excel(df_ok, MINIO_CLEANED_KEY, "RH éligibles")
    upload_excel(df_ko, MINIO_EXCLUS_KEY, "RH exclus")

    # Export vers PostgreSQL (UPSERT au lieu de replace)
    if not df_ok.empty:
        engine = _build_engine()
        _ensure_table(engine)
        _upsert_employes(engine, df_ok)
        logger.success("🗃️ Données RH upsert dans PostgreSQL (sportdata.employes)")
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
