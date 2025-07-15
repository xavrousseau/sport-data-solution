# ==========================================================================================
# Script      : nettoyer_donnees_rh.py
# Objectif    : Audit exploratoire, nettoyage et filtrage d’éligibilité RH avec mapping dynamique.
# Auteur      : Xavier Rousseau | Juillet 2025, version corrigée & commentée
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
import unicodedata
import re
import io 

# ==========================================================================================
# 1. Chargement des variables d’environnement (.env global)
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

# ==========================================================================================
# 2. Chemins, constantes globales et mappings
# ==========================================================================================
MINIO_SOURCE_KEY = "referentiels/donnees_rh.xlsx"
MINIO_CLEANED_KEY = "raw/donnees_rh_cleaned.xlsx"
MINIO_EXCLUS_KEY = "raw/donnees_rh_exclus.xlsx"
TMP_DIR = "/tmp"

ADRESSE_TRAVAIL = "1362 Avenue des Platanes, 34970 Lattes, France"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

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
# Mapping des modes de transport (le plus exhaustif possible, toutes variantes acceptées)
# ==========================================================================================
MODES_TRANSPORT = {
    "marche": 15,
    "vélo": 25,
}

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

MAPPING_GOOGLE_API = {
    "marche": "walking",
    "vélo": "bicycling",
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

def get_mode_api(mode_projet):
    """Mode projet -> mode Google API (pour Distance Matrix)."""
    return MAPPING_GOOGLE_API.get(mode_projet)

# ==========================================================================================
# 5. Fonctions utilitaires et eligibility
# ==========================================================================================

def normaliser_colonnes(df):
    """Noms de colonnes harmonisés pour traitement ETL."""
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("'", "_")
        .str.replace("’", "_")
        .str.replace("é", "e")
        .str.replace("è", "e")
        .str.replace("ê", "e")
        .str.replace("à", "a")
        .str.replace("â", "a")
        .str.replace("î", "i")
        .str.replace("ô", "o")
        .str.replace("ù", "u")
        .str.replace("-", "_")
    )
    return df

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
    """Retourne (is_eligible: bool, motif: str, distance: float)"""
    if not adresse or pd.isna(adresse):
        return False, "Adresse manquante", None
    if not mode or pd.isna(mode):
        return False, "Mode de transport manquant", None
    mode_projet = normaliser_mode_transport(mode)
    if not mode_projet:
        return False, f"Mode non éligible ({mode})", None
    distance = calculer_distance_km(adresse, mode_projet)
    if distance is None:
        return False, "Distance non calculable", None
    if distance > MODES_TRANSPORT[mode_projet]:
        return False, f"Distance {distance:.1f} km > seuil ({MODES_TRANSPORT[mode_projet]} km)", distance
    return True, "", distance

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

        # -- 2. Lecture du fichier
        df = pd.read_excel(tmpfile.name)

    # -- 3. Nettoyage/normalisation des colonnes
    df = normaliser_colonnes(df)
    logger.info(f"Fichier RH chargé ({len(df)} lignes)")

    # -- 4. Analyse exploratoire avancée
    analyse_exploratoire_avancee(df, description="Analyse exploratoire RH (brut, normalisé)")

    # -- 5. Vérification des champs nécessaires
    champs_requis = ["id_salarie", "nom", "prenom", "adresse_du_domicile", "moyen_de_deplacement"]
    for champ in champs_requis:
        if champ not in df.columns:
            logger.error(f"Colonne manquante : {champ} - Abandon.")
            return

    # -- 6. Calcul distance et éligibilité
    eligibles = []
    exclus = []
    logger.info("Calcul des distances et filtrage des éligibilités… (peut être long selon la taille)")
    for idx, row in df.iterrows():
        id_salarie = row["id_salarie"]
        nom = row["nom"]
        prenom = row["prenom"]
        adresse = row["adresse_du_domicile"]
        mode = row["moyen_de_deplacement"]
        is_eligible, motif, distance = verifier_eligibilite(adresse, mode)
        ligne = {
            "id_salarie": id_salarie,
            "nom": nom,
            "prenom": prenom,
            "adresse_du_domicile": adresse,
            "moyen_de_deplacement": mode,
            "distance_km": distance,
            "eligible": is_eligible,
            "motif_exclusion": motif
        }
        if is_eligible:
            eligibles.append(ligne)
        else:
            exclus.append(ligne)
            logger.debug(f"❌ Exclusion : {prenom} {nom} - {motif} - mode={mode}, distance={distance}")

    df_eligibles = pd.DataFrame(eligibles)
    df_exclus = pd.DataFrame(exclus)

    logger.info(f"{len(df_eligibles)} salariés éligibles / {len(df_exclus)} exclus.")
    if len(df) > 0:
        taux = 100 * len(df_eligibles) / len(df)
        logger.info(f"📊 Taux d’éligibilité : {taux:.2f}%")

    # -- 7. Validation qualité (Great Expectations)
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

        # Lancer chaque expectation et loguer le résultat
        all_ok = True
        for exp_type, kwargs in expectations:
            logger.info(f"Test expectation: {exp_type} -- {kwargs}")
            result = getattr(ge_df, exp_type)(**kwargs)
            if not result.success:
                all_ok = False
                logger.error(f"❌ ECHEC: {exp_type} -- {kwargs}")
                if hasattr(result, "unexpected_index_list"):
                    logger.error(f"Indices en échec: {result.unexpected_index_list}")
                if hasattr(result, "unexpected_list"):
                    logger.error(f"Valeurs en échec: {result.unexpected_list}")
            else:
                logger.info(f"✅ OK: {exp_type}")

        if not all_ok:
            raise Exception("Validation Great Expectations échouée, pipeline interrompu.")
        else:
            logger.success("✅ Validation Great Expectations réussie : toutes les expectations sont remplies.")

        # -- Génération du rapport HTML Great Expectations
        rendered = ValidationResultsPageRenderer().render(result)
        html = DefaultJinjaPageView().render(rendered)
        report_name = f"validation_reports/rapport_GE_RH_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        report_path = os.path.join(TMP_DIR, "rapport.html")

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html)

        try:
            helper.client.fput_object(
                Bucket=helper.bucket,
                Key=report_name,
                Filename=report_path
            )
            logger.success(f"📄 Rapport GE HTML uploadé dans MinIO : {report_name}")
        except Exception as e:
            logger.error(f"Erreur upload rapport GE : {e}")

    # -- 8. Export fichiers vers MinIO (XLSX)
    def upload_excel_to_minio(dataframe, key, label):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            dataframe.to_excel(writer, index=False)
        output.seek(0)
        try:
            helper.client.put_object(
                Bucket=helper.bucket,
                Key=key,
                Body=output,
                ContentLength=output.getbuffer().nbytes,
            )
            logger.success(f"✅ Fichier {label} uploadé sur MinIO : {key}")
        except Exception as e:
            logger.error(f"Erreur upload MinIO {label} : {e}")

    if not df_eligibles.empty:
        upload_excel_to_minio(df_eligibles, MINIO_CLEANED_KEY, "RH nettoyé (éligibles)")
    if not df_exclus.empty:
        upload_excel_to_minio(df_exclus, MINIO_EXCLUS_KEY, "RH exclus")

    # -- 9. Insertion PostgreSQL
    if not df_eligibles.empty:
        try:
            engine = create_engine(DB_CONN_STRING)
            df_eligibles.to_sql("employes", engine, if_exists="replace", index=False, schema="sportdata")
            logger.success("✅ Données éligibles insérées dans PostgreSQL ('employes')")
        except Exception as e:
            logger.error(f"Erreur insertion PostgreSQL : {e}")

    logger.info("=== Pipeline nettoyage RH terminé ===")

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
