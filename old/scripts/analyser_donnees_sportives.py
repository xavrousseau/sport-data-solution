# ==========================================================================================
# Script      : nettoyer_donnees_sportives.py
# Objectif    : Audit, nettoyage et enrichissement du fichier sportif,
#               rapport d’exclusion détaillé, export MinIO.
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import pandas as pd
import tempfile
from loguru import logger
from minio_helper import MinIOHelper
import unicodedata
import re

# Chemin du fichier sur MinIO
MINIO_SOURCE_KEY = "referentiels/donnees_sportives.xlsx"
MINIO_CLEANED_KEY = "raw/donnees_sportives_cleaned.xlsx"
MINIO_EXCLUS_KEY = "raw/donnees_sportives_exclus.xlsx"
TMP_DIR = "/tmp"

# ==========================================================================================
# 1. Mapping/normalisation des activités sportives
# ==========================================================================================
# Tu peux enrichir ce mapping selon les activités réelles de tes données
MAPPING_ACTIVITE = {
    "running": "Course à pied",
    "runing": "Course à pied",      # faute corrigée
    "course a pied": "Course à pied",
    "marche": "Marche",
    "randonnée": "Randonnée",
    "randonnee": "Randonnée",
    "tennis": "Tennis",
    "natation": "Natation",
    "football": "Football",
    "rugby": "Rugby",
    "badminton": "Badminton",
    "voile": "Voile",
    "judo": "Judo",
    "boxe": "Boxe",
    "escalade": "Escalade",
    "triathlon": "Triathlon",
    "équitation": "Equitation",
    "equitation": "Equitation",
    "tennis de table": "Tennis de table",
    "basketball": "Basketball"
}

def normaliser_activite(activite):
    """
    Nettoie et harmonise un nom d'activité sportive.
    - Minuscule, sans accents, tirets/underscores/espaces standardisés
    - Utilise le mapping défini ci-dessus
    """
    if not isinstance(activite, str):
        return None
    # Retire accents, minuscule
    act = unicodedata.normalize('NFD', activite).encode('ascii', 'ignore').decode('utf-8').lower()
    act = re.sub(r"[-_]", " ", act)
    act = re.sub(r"\s+", " ", act).strip()
    act = act.replace("'", "")  # retire apostrophes si présent
    return MAPPING_ACTIVITE.get(act, activite.strip())  # Garde l'original si pas dans mapping

# ==========================================================================================
# 2. Analyse exploratoire avancée (structure, valeurs, stats)
# ==========================================================================================
def analyse_exploratoire_sport(df, description="Analyse exploratoire des activités sportives"):
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
        uniques = df[col].dropna().unique()[:5]
        logger.info(f"Valeurs uniques (5 premières) : {uniques}")
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

    # Focus sur les variables clés pour la suite du pipeline
    for champ in ["id_salarie", "type_activite", "distance_km", "date", "temps", "commentaire"]:
        cols = [c for c in df.columns if champ in c.lower()]
        if cols:
            logger.info(f"\nExemples pour '{cols[0]}': {df[cols[0]].drop_duplicates().head(5).to_list()}")

    # Audit des types d’activités déclarées
    ACTIVITES = ["type", "activité", "activite", "sport"]
    col_type = next((c for c in df.columns if any(a in c.lower() for a in ACTIVITES)), None)
    if col_type:
        logger.info(f"\n=== Répartition des types d’activité déclarés ({col_type}) ===")
        logger.info(df[col_type].value_counts(dropna=False).to_string())
    else:
        logger.warning("Colonne 'type d’activité' non trouvée !")

    # Audit de l'ID salarié
    col_id = next((c for c in df.columns if "id" in c.lower() and "salari" in c.lower()), None)
    if col_id:
        logger.info(f"\nNombre d'IDs salariés différents dans le fichier sportif : {df[col_id].nunique()}")
    else:
        logger.warning("Colonne 'id salarié' non trouvée !")

# ==========================================================================================
# 3. Pipeline principal de nettoyage et d’export
# ==========================================================================================
def pipeline_nettoyage_sport():
    logger.info("=== Démarrage du pipeline de nettoyage Sportifs ===")
    helper = MinIOHelper()
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        logger.info(f"Téléchargement du fichier sportif depuis MinIO : {MINIO_SOURCE_KEY}")
        try:
            helper.client.download_file(
                Bucket=helper.bucket,
                Key=MINIO_SOURCE_KEY,
                Filename=tmpfile.name
            )
            logger.success(f"✅ Fichier téléchargé depuis MinIO dans {tmpfile.name}")
            df = pd.read_excel(tmpfile.name)
            logger.success(f"✅ {len(df)} lignes chargées depuis {MINIO_SOURCE_KEY}")
        except Exception as e:
            logger.error(f"Erreur accès ou lecture fichier sportif MinIO : {e}")
            return

    # -- Audit exploratoire avant nettoyage
    analyse_exploratoire_sport(df, description="Analyse exploratoire brute des activités sportives")

    # -- Normalisation des colonnes pour l’ETL
    df.columns = [c.strip().lower().replace(" ", "_").replace("'", "") for c in df.columns]

    # -- Nettoyage / mapping activité sportive
    col_activite = next((c for c in df.columns if "activite" in c or "sport" in c), None)
    col_id = next((c for c in df.columns if "id" in c and "salari" in c), None)

    if not col_activite or not col_id:
        logger.error("Colonne activité ou ID salarié non trouvée, pipeline interrompu.")
        return

    eligibles = []
    exclus = []

    for idx, row in df.iterrows():
        activite = row[col_activite]
        id_salarie = row[col_id]
        activite_clean = normaliser_activite(activite) if pd.notna(activite) else None

        ligne = {
            "id_salarie": id_salarie,
            "activite_origine": activite,
            "activite_clean": activite_clean
        }

        if activite_clean and pd.notna(activite_clean) and activite_clean.strip() != "":
            eligibles.append(ligne)
        else:
            ligne["motif_exclusion"] = "Activité manquante ou non reconnue"
            exclus.append(ligne)

    df_eligibles = pd.DataFrame(eligibles)
    df_exclus = pd.DataFrame(exclus)

    logger.info(f"{len(df_eligibles)} activités sportives valides / {len(df_exclus)} exclus.")

    # -- Export vers MinIO (cleaned + exclusions)
    import io
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
        upload_excel_to_minio(df_eligibles, MINIO_CLEANED_KEY, "Sport nettoyé (éligibles)")
    if not df_exclus.empty:
        upload_excel_to_minio(df_exclus, MINIO_EXCLUS_KEY, "Sport exclus")

    logger.info("=== Pipeline nettoyage Sportifs terminé ===")

# ==========================================================================================
# 4. Point d’entrée
# ==========================================================================================
if __name__ == "__main__":
    try:
        pipeline_nettoyage_sport()
    except Exception as e:
        logger.error(f"❌ Pipeline interrompu : {e}")
        raise

# ==========================================================================================
# Fin du fichier – Nettoyage Sportifs avec audit, mapping activité, exclusions, export MinIO
# ==========================================================================================
