# ==========================================================================================
# Script      : 02_nettoyer_donnees_rh.py
# Objectif    : Audit exploratoire, nettoyage et filtrage d’éligibilité RH avec mapping dynamique.
# Auteur      : Xavier Rousseau | Juillet 2025 
# ==========================================================================================

import os
import tempfile
import pandas as pd
import requests
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine

# Import des composants pour générer un rapport HTML lisible avec Great Expectations
import great_expectations as ge
try:
    from great_expectations.render.renderer import ValidationResultsPageRenderer
    from great_expectations.render.view import DefaultJinjaPageView
except ImportError:
    raise ImportError("❌ Les modules pour générer le rapport HTML Great Expectations ne sont pas disponibles. Installe-les avec `pip install great_expectations`.")


# Import utilisé pour horodater le rapport (nom de fichier)
from datetime import datetime
from minio_helper import MinIOHelper
import unicodedata
import re
import io 
import uuid
# ==========================================================================================
# 1. Chargement des variables d’environnement (.env global)
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

# ==========================================================================================
# 2. Variables globales et connexions
# ==========================================================================================
MINIO_SOURCE_KEY = "referentiels/donnees_rh.xlsx"           # RH brut
MINIO_CLEANED_KEY = "raw/donnees_rh_cleaned.xlsx"           # RH éligibles
MINIO_EXCLUS_KEY = "raw/donnees_rh_exclus.xlsx"             # RH exclus

TMP_DIR = "/tmp"

ADRESSE_TRAVAIL = "1362 Avenue des Platanes, 34970 Lattes, France"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

DISTANCE_MAX_MARCHE_KM = 15
DISTANCE_MAX_VELO_KM = 25

MODES_TRANSPORT = {"marche": DISTANCE_MAX_MARCHE_KM, "vélo": DISTANCE_MAX_VELO_KM}
MAPPING_GOOGLE_API = {"marche": "walking", "vélo": "bicycling"}

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


# ==========================================================================================
# Mapping des modes de transport (le plus exhaustif possible, toutes variantes acceptées)
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
    "velo trottinette autres": "vélo",
    "vélo trottinette autres": "vélo",
    "velo trottinette autres": "vélo",

    # Synonymes internationaux/anglais
    "scooter": "vélo",       # usage trottinette seulement !
    "e scooter": "vélo",
    "e bike": "vélo",
    "e-bike": "vélo",
    "kick scooter": "vélo",
    "push scooter": "vélo"
    # tout le reste : non éligible
    # Les modes suivants sont non éligibles et NE DOIVENT PAS figurer dans le mapping :
    # - "Transports en commun" (tram, bus, métro…)
    # - "véhicule thermique/électrique" (voiture, moto…)
}


# ==========================================================================================
# 3. Analyse exploratoire avancée + log mapping dynamique
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
# 4. Fonction de normalisation robuste des modes de transport
# ==========================================================================================

def normaliser_mode_transport(mode):
    """
    Nettoie et uniformise un mode de transport pour le mapping.
    - Accents supprimés, tout en minuscules
    - Tous séparateurs / - _ deviennent des espaces
    - Ponctuation supprimée
    - Plusieurs espaces réduits à un seul
    - Mapping exact dans MAPPING_TRANSPORT
    """
    if not isinstance(mode, str):
        return None
    mode_clean = (
        unicodedata.normalize('NFD', mode)
        .encode('ascii', 'ignore')
        .decode('utf-8')
        .lower()
    )
    mode_clean = re.sub(r"[-_/]", " ", mode_clean)
    mode_clean = re.sub(r"\s+", " ", mode_clean)
    mode_clean = re.sub(r"[^\w\s]", "", mode_clean).strip()
    return MAPPING_TRANSPORT.get(mode_clean)


def normaliser_colonnes(df):
    """Harmonise tous les noms de colonnes pour traitement ETL"""
    return df.rename(columns=lambda col: (
        col.strip()                        # Suppression des espaces autour
           .lower()                        # Minuscule
           .replace(" ", "_")              # Espaces => underscore
           .replace("'", "_")              # Apostrophes classiques
           .replace("’", "_")              # Apostrophes typographiques
           .replace("é", "e")              # Accents
           .replace("è", "e")
           .replace("ê", "e")
           .replace("à", "a")
           .replace("â", "a")
           .replace("î", "i")
           .replace("ô", "o")
           .replace("ù", "u")
           .replace("-", "_")              # Tirets => underscore
    ))

# ==========================================================================================
# 5. Fonctions utilitaires et eligibility
# ==========================================================================================

def get_mode_api(mode_projet):
    """Mode projet -> mode Google API (pour Distance Matrix)."""
    return MAPPING_GOOGLE_API.get(mode_projet)

def calculer_distance_km(adresse_depart, mode_projet):
    """Distance Google Maps (km) selon mode projet."""
    if not adresse_depart or pd.isna(adresse_depart) or not mode_projet:
        return None
    mode_api = get_mode_api(mode_projet)
    if not mode_api:
        return None
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": adresse_depart,
        "destinations": ADRESSE_TRAVAIL,
        "key": GOOGLE_API_KEY,
        "mode": mode_api
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        status = data["rows"][0]["elements"][0]["status"]
        if status != "OK":
            return None
        distance_m = data["rows"][0]["elements"][0]["distance"]["value"]
        return distance_m / 1000
    except Exception as e:
        logger.warning(f"Erreur API Google Maps pour '{adresse_depart}' : {e}")
        return None

def verifier_eligibilite(adresse, mode):
    """Retourne (is_eligible: bool, motif: str, distance: float, mode_normalise: str)"""
    if not adresse or pd.isna(adresse):
        return False, "Adresse manquante", None, None
    if not mode or pd.isna(mode):
        return False, "Mode de transport manquant", None, None
    mode_projet = normaliser_mode_transport(mode)
    if not mode_projet:
        return False, f"Mode non éligible ({mode})", None, None
    distance = calculer_distance_km(adresse, mode_projet)
    if distance is None:
        return False, "Distance non calculable", None, mode_projet
    if distance > MODES_TRANSPORT[mode_projet]:
        return False, f"Distance {distance:.1f} km > seuil ({MODES_TRANSPORT[mode_projet]} km)", distance, mode_projet
    return True, "", distance, mode_projet

# ==========================================================================================
# 6. Pipeline principal
# ==========================================================================================

def pipeline_nettoyage_rh():
    logger.info("=== Démarrage du pipeline de nettoyage RH ===")
    helper = MinIOHelper()

    # -- 1. Téléchargement du fichier source MinIO
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        try:
            helper.client.download_file(
                Bucket=helper.bucket,
                Key=MINIO_SOURCE_KEY,
                Filename=tmpfile.name
            )
            logger.success(f"✅ Fichier RH téléchargé : {MINIO_SOURCE_KEY}")
        except Exception as e:
            logger.error(f"❌ Echec téléchargement fichier source : {e}")
            return
        df = pd.read_excel(tmpfile.name)

    # -- 2. Nettoyage/normalisation des colonnes
    df = normaliser_colonnes(df)
    logger.info(f"Fichier RH chargé ({len(df)} lignes)")

    # -- 3. Analyse exploratoire
    analyse_exploratoire_avancee(df)

    # -- 4. Vérification des colonnes attendues
    # Liste des colonnes indispensables pour le traitement RH
    champs_requis = ["id_salarie", "nom", "prenom", "adresse_du_domicile", "moyen_de_deplacement"]

    # Vérification que toutes sont bien présentes dans le fichier
    if not all(col in df.columns for col in champs_requis):
        logger.error("❌ Colonnes requises manquantes")
        return
    # -- 5. Vérification d’éligibilité
    eligibles, exclus = [], []
    logger.info("Calcul des distances et éligibilité (API Google Maps)…")
    for _, row in df.iterrows():
        is_eligible, motif, distance, mode_projet = verifier_eligibilite(
            row["adresse_du_domicile"], row["moyen_de_deplacement"]
        )
        ligne = {
            "uid": str(uuid.uuid4()),
            "id_salarie": row.get("id_salarie"),
            "nom": row.get("nom"),
            "prenom": row.get("prenom"),
            "adresse_du_domicile": row.get("adresse_du_domicile"),
            "moyen_de_deplacement": row.get("moyen_de_deplacement"),
            "mode_normalise": mode_projet,
            "distance_km": distance,
            "eligible": is_eligible,
            "motif_exclusion": motif,
            "salaire_brut_annuel": row.get("salaire_brut") 
        }
        (eligibles if is_eligible else exclus).append(ligne)

    df_eligibles = pd.DataFrame(eligibles)
    df_exclus = pd.DataFrame(exclus)
    
    # -- Ajout colonne deplacement_sportif : True si le mode est sportif
    df_eligibles["deplacement_sportif"] = df_eligibles["mode_normalise"].isin(["marche", "vélo"])
    df_eligibles["salaire_brut_annuel"] = pd.to_numeric(df_eligibles["salaire_brut_annuel"], errors="coerce").fillna(0).astype(int)

    logger.info(f"{len(df_eligibles)} éligibles / {len(df_exclus)} exclus.")
    logger.info(f"Taux d'éligibilité : {100 * len(df_eligibles)/len(df):.2f}%")

    # -- 6. Validation qualité (Great Expectations)
    if not df_eligibles.empty:
        logger.info("Validation de la qualité avec Great Expectations…")

        ge_df = ge.from_pandas(df_eligibles)

        # Expectations détaillées (modifie selon tes besoins)
        expectations = [
            ("expect_column_values_to_not_be_null", dict(column="id_salarie")),
            ("expect_column_values_to_not_be_null", dict(column="nom")),
            ("expect_column_values_to_not_be_null", dict(column="prenom")),
            ("expect_column_values_to_not_be_null", dict(column="adresse_du_domicile")),
            ("expect_column_values_to_not_be_null", dict(column="distance_km")),
            ("expect_column_values_to_be_between", dict(column="distance_km", min_value=0, max_value=100)),
            ("expect_column_values_to_be_of_type", dict(column="distance_km", type_="float64")),
            ("expect_column_values_to_be_of_type", dict(column="deplacement_sportif", type_="bool")),
            ("expect_column_values_to_be_between", dict(column="salaire_brut_annuel", min_value=10000, max_value=100000)),
            ("expect_column_values_to_be_of_type", dict(column="salaire_brut_annuel", type_="int64")),

        ]
        # Dates (optionnel)
        if "date_de_naissance" in df_eligibles.columns:
            expectations.append(("expect_column_values_to_be_between", dict(
                column="date_de_naissance",
                min_value="1900-01-01",
                max_value=pd.Timestamp.today().strftime("%Y-%m-%d")
            )))
        if "date_d_embauche" in df_eligibles.columns:
            expectations.append(("expect_column_values_to_be_between", dict(
                column="date_d_embauche",
                min_value="1990-01-01",
                max_value=pd.Timestamp.today().strftime("%Y-%m-%d")
            )))

        # Appliquer toutes les expectations à ge_df
        for exp_type, kwargs in expectations:
            logger.info(f"Test expectation: {exp_type} -- {kwargs}")
            getattr(ge_df, exp_type)(**kwargs)

        # ✅ Validation globale du DataFrame selon toutes les expectations définies ci-dessus
        checkpoint_result = ge_df.validate(result_format="SUMMARY")

        # Vérification du succès global
        if not checkpoint_result.success:
            logger.error("❌ Certaines expectations ont échoué.")
            raise Exception("Validation Great Expectations échouée, pipeline interrompu.")
        else:
            logger.success("✅ Validation Great Expectations réussie : toutes les expectations sont remplies.")

        # ✅ Génération du rapport HTML lisible
        rendered = ValidationResultsPageRenderer().render(checkpoint_result)
        html = DefaultJinjaPageView().render(rendered)

        # Nom du rapport horodaté
        report_name = f"validation_reports/rapport_GE_RH_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        report_path = os.path.join(TMP_DIR, "rapport.html")        # Emplacement temporaire

        # Sauvegarde du rapport HTML localement (dans le container)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html)

        # Upload vers MinIO dans le dossier prévu (via boto3)
        try:
            helper.client.upload_file(
                Filename=report_path,
                Bucket=helper.bucket,
                Key=report_name
            )
            logger.success(f"📄 Rapport GE HTML uploadé dans MinIO : {report_name}")
        except Exception as e:
            logger.error(f"Erreur upload rapport GE : {e}")

        # Vérification que le fichier est bien présent dans MinIO
        if report_name in helper.list_objects("validation_reports/"):
            logger.info(f"✅ Vérification MinIO : {report_name} est bien présent dans le bucket.")
        else:
            logger.warning(f"⚠️ Rapport non trouvé après upload : {report_name}")

    # -- 7. Export vers MinIO
    def upload_excel(df, key, label):
        """
        Upload un DataFrame Excel dans MinIO via un buffer en mémoire (sans fichier temporaire)
        """
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False)
        buffer.seek(0)
        helper.client.put_object(
            Bucket=helper.bucket,
            Key=key,
            Body=buffer,
            ContentLength=buffer.getbuffer().nbytes
        )
        logger.success(f"✅ Fichier {label} uploadé sur MinIO : {key}")


    if not df_eligibles.empty:
        upload_excel(df_eligibles, MINIO_CLEANED_KEY, "RH éligibles")
    if not df_exclus.empty:
        upload_excel(df_exclus, MINIO_EXCLUS_KEY, "RH exclus")

    # -- 8. Insertion PostgreSQL
    if not df_eligibles.empty:
        engine = create_engine(DB_CONN_STRING)
        df_eligibles.to_sql("employes", engine, if_exists="replace", index=False, schema="sportdata")
        logger.success("✅ Données insérées dans PostgreSQL (sportdata.employes)")

    logger.info("=== Pipeline terminé ===")

# ==========================================================================================
# 7. Point d’entrée
# ==========================================================================================
if __name__ == "__main__":
    try:
        pipeline_nettoyage_rh()
    except Exception as e:
        logger.error(f"❌ Pipeline interrompu : {e}")
        raise

# ==========================================================================================
# Fin du fichier – Nettoyage RH avec audit, mapping dynamique, logs pédagogiques
# ==========================================================================================
