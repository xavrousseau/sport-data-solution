# ==========================================================================================
# Script      : import_employes.py
# Objectif    : Importer les employés depuis le fichier RH dans PostgreSQL (stack Docker)
# Auteur      : Xavier Rousseau | Version finalisée Juillet 2025
# Contexte    : Exécution depuis le container Airflow via docker exec ou DAG
# ==========================================================================================

import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from loguru import logger
from pathlib import Path

# ------------------------------------------------------------------------------------------
# 1. Emplacement du .env et chargement des variables d’environnement
# (on suppose que le script tourne DANS le container Airflow)
# ------------------------------------------------------------------------------------------
ENV_PATH = Path("/opt/airflow/.env")
if not ENV_PATH.exists():
    raise FileNotFoundError(f"❌ Fichier .env introuvable dans le container : {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)

# ------------------------------------------------------------------------------------------
# 2. Configuration des logs (console + fichier)
# ------------------------------------------------------------------------------------------
LOGS_DIR = Path("/opt/airflow/logs")
LOGS_DIR.mkdir(exist_ok=True)
logger.remove()
logger.add(sys.stdout, level="INFO",
           format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>")
logger.add(str(LOGS_DIR / "import_employes.log"), rotation="1 MB", retention="10 days", encoding="utf-8")

# ------------------------------------------------------------------------------------------
# 3. Récupération des variables PostgreSQL depuis le .env
# ------------------------------------------------------------------------------------------
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

logger.info(f"🔍 Connexion à PostgreSQL → {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# ------------------------------------------------------------------------------------------
# 4. Chargement du fichier RH (Excel) depuis dossier partagé
# ------------------------------------------------------------------------------------------
FICHIER_RH = Path("/opt/airflow/data/inputs/donnees_rh.xlsx")

# ------------------------------------------------------------------------------------------
# 5. Fonction utilitaire : normalisation des noms de colonnes
# ------------------------------------------------------------------------------------------
def normaliser_colonnes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les noms de colonnes pour les adapter au format SQL (snake_case, sans accents)
    """
    df.columns = [
        col.strip().lower()
        .replace(" ", "_").replace("'", "_").replace("’", "_")
        .replace("é", "e").replace("è", "e").replace("ê", "e")
        .replace("à", "a").replace("â", "a")
        for col in df.columns
    ]
    return df

# ------------------------------------------------------------------------------------------
# 6. Fonction principale : chargement dans PostgreSQL
# ------------------------------------------------------------------------------------------
def main():
    conn, cur = None, None

    try:
        # --- Vérification fichier RH ---
        if not FICHIER_RH.exists():
            logger.error(f"❌ Fichier RH introuvable : {FICHIER_RH}")
            return

        df_rh = pd.read_excel(FICHIER_RH)
        df_rh = normaliser_colonnes(df_rh)
        logger.info(f"📄 Chargement de {len(df_rh)} employés depuis le fichier Excel")

        # --- Connexion PostgreSQL ---
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()
        logger.success("✅ Connexion PostgreSQL réussie")

        # --- Création de la table employes ---
        logger.info("🧱 Création de la table employes (DROP + CREATE)")
        cur.execute("DROP TABLE IF EXISTS employes;")
        cur.execute("""
            CREATE TABLE employes (
                id_salarie INTEGER PRIMARY KEY,
                nom TEXT,
                prenom TEXT,
                date_de_naissance DATE,
                bu TEXT,
                date_d_embauche DATE,
                salaire_brut FLOAT,
                type_de_contrat TEXT,
                nombre_de_jours_de_cp INTEGER,
                adresse_du_domicile TEXT,
                moyen_de_deplacement TEXT
            );
        """)
        conn.commit()

        # --- Insertion des données ---
        logger.info("📥 Insertion des employés en base...")
        for _, row in df_rh.iterrows():
            cur.execute("""
                INSERT INTO employes (
                    id_salarie, nom, prenom, date_de_naissance, bu,
                    date_d_embauche, salaire_brut, type_de_contrat,
                    nombre_de_jours_de_cp, adresse_du_domicile, moyen_de_deplacement
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_salarie) DO NOTHING;
            """, tuple(row[col] if pd.notna(row[col]) else None for col in df_rh.columns))

        conn.commit()
        logger.success("🎯 Insertion terminée avec succès.")

    except Exception as e:
        logger.exception(f"❌ Erreur lors de l'import : {e}")
        if conn:
            conn.rollback()

    finally:
        try:
            if cur: cur.close()
            if conn: conn.close()
            logger.info("🔌 Connexion PostgreSQL fermée.")
        except:
            pass

# ------------------------------------------------------------------------------------------
# 7. Point d’entrée du script
# ------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()

# ==========================================================================================
# FIN DU SCRIPT — import_employes.py
# ==========================================================================================
