# ==========================================================================================
# Script      : initialisation.py
# Objectif    : Charger les données RH & générer dynamiquement des activités sportives
# Auteur      : Xavier Rousseau | Juin 2025
#
# Fonctionnalités principales :
# - Lit les données RH (Excel), insère dans PostgreSQL
# - Génère aléatoirement des activités sportives pour chaque salarié
# - Insère toutes les données dans la base
# - Logging détaillé (console + fichier logs/)
# ==========================================================================================

import pandas as pd
import psycopg2
from pathlib import Path
from loguru import logger
import sys
import os
import warnings
from faker import Faker
import random
from datetime import timedelta

# --- Désactivation des warnings pandas (propre en prod, à activer en dev si besoin) ---
warnings.filterwarnings("ignore")

# ------------------------------------------------------------------------------------------
# 1. Répertoires de base, data et logs
# ------------------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data" / "inputs"
LOGS_DIR = BASE_DIR.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / "initialisation.log"

# ------------------------------------------------------------------------------------------
# 2. Setup du logger (console + fichier logs/initialisation.log)
# ------------------------------------------------------------------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO",
           format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>")
logger.add(str(LOG_FILE), rotation="1 MB", retention="10 days", encoding="utf-8", level="DEBUG")

# ------------------------------------------------------------------------------------------
# 3. Configuration PostgreSQL (adapter en prod !)
# ------------------------------------------------------------------------------------------
DB_HOST = "sport-postgres"
DB_PORT = 5432
DB_NAME = "sportdata"
DB_USER = "user"
DB_PASSWORD = "password"

FICHIER_RH = DATA_DIR / "donnees_rh.xlsx"
FICHIER_SPORT = DATA_DIR / "donnees_sportives.xlsx"

# ------------------------------------------------------------ ------------------------------
# 4. Paramètres des types d'activités sportives générées (valeurs réalistes)
# ------------------------------------------------------------------------------------------
ACTIVITES = {
    "course à pied": {"min_dist": 2000, "max_dist": 15000, "min_time": 900, "max_time": 5400},
    "vélo": {"min_dist": 5000, "max_dist": 30000, "min_time": 1800, "max_time": 7200},
    "marche": {"min_dist": 1000, "max_dist": 8000, "min_time": 600, "max_time": 3600},
    "randonnée": {"min_dist": 5000, "max_dist": 20000, "min_time": 1800, "max_time": 14400},
    "tennis": {"min_dist": 0, "max_dist": 0, "min_time": 1800, "max_time": 7200},
    "escalade": {"min_dist": 0, "max_dist": 0, "min_time": 1200, "max_time": 5400},
    "badminton": {"min_dist": 0, "max_dist": 0, "min_time": 1200, "max_time": 5400},
    "triathlon": {"min_dist": 10000, "max_dist": 40000, "min_time": 3600, "max_time": 10800}
}

faker = Faker("fr_FR")
random.seed(42)  # Pour reproductibilité des résultats

# ------------------------------------------------------------------------------------------
# 5. Fonctions utilitaires
# ------------------------------------------------------------------------------------------

def normaliser_colonnes(df):
    """
    Normalise les noms de colonnes pour correspondre aux noms SQL (snake_case, sans accents)
    """
    df.columns = [
        col.strip().lower()
        .replace(" ", "_")
        .replace("'", "_")
        .replace("’", "_")
        .replace("é", "e").replace("è", "e").replace("ê", "e")
        .replace("à", "a").replace("â", "a")
        for col in df.columns
    ]
    return df

def generer_activite(id_salarie, sport_libre):
    """
    Génère une activité sportive aléatoire pour un salarié donné,
    basée sur le sport préféré si dispo, sinon tirage au sort.
    """
    sport = sport_libre.lower() if pd.notna(sport_libre) else random.choice(list(ACTIVITES.keys()))
    meta = ACTIVITES.get(sport, ACTIVITES["course à pied"])
    date_debut = faker.date_time_between(start_date='-1y', end_date='now')
    distance = random.randint(meta["min_dist"], meta["max_dist"]) if meta["max_dist"] > 0 else None
    duree = random.randint(meta["min_time"], meta["max_time"])
    commentaire = random.choice([
        f"Session de {sport} {faker.word()} et motivante.",
        f"Bonne séance de {sport}, {faker.word()} au top.",
        f"{sport.capitalize()} agréable, {faker.word()} réussie.",
        f"{sport.capitalize()} effectué sous un temps {faker.word()}."
    ])
    return (
        int(id_salarie),
        date_debut,
        sport,
        distance,
        duree,
        commentaire
    )

# ------------------------------------------------------------------------------------------
# 6. Fonction principale
# ------------------------------------------------------------------------------------------

def main():
    conn, cur = None, None
    try:
        logger.info("Connexion à la base PostgreSQL...")
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()
        logger.success("Connexion PostgreSQL OK.")

        # --- Création/réinitialisation des tables ---
        logger.info("Création des tables (DROP + CREATE)...")
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
        cur.execute("DROP TABLE IF EXISTS activites_sportives;")
        cur.execute("""
            CREATE TABLE activites_sportives (
                id SERIAL PRIMARY KEY,
                id_salarie INTEGER,
                date_debut TIMESTAMP,
                type_activite TEXT,
                distance_m FLOAT,
                duree_s INTEGER,
                commentaire TEXT
            );
        """)
        conn.commit()
        logger.success("Tables employes et activites_sportives créées.")

        # --- Insertion RH ---
        logger.info(f"Lecture du fichier RH : {FICHIER_RH}")
        df_rh = pd.read_excel(FICHIER_RH)
        df_rh = normaliser_colonnes(df_rh)
        logger.info(f"Insertion des données RH ({len(df_rh)})...")
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
        logger.success("Données RH insérées dans la table employes.")

        # --- Génération et insertion activités sportives ---
        logger.info(f"Lecture du fichier sportif : {FICHIER_SPORT}")
        df_sport = pd.read_excel(FICHIER_SPORT)
        df_sport = normaliser_colonnes(df_sport)
        logger.info(f"Génération des activités pour {len(df_sport)} salariés...")
        for _, row in df_sport.iterrows():
            activite = generer_activite(row["id_salarie"], row.get("pratique_d_un_sport", None))
            cur.execute("""
                INSERT INTO activites_sportives
                    (id_salarie, date_debut, type_activite, distance_m, duree_s, commentaire)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, activite)

        conn.commit()
        logger.success("✅ Activités sportives générées et insérées dans PostgreSQL.")

    except Exception as e:
        logger.exception(f"❌ Une erreur est survenue : {e}")
        if conn:
            conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.info("Connexion PostgreSQL fermée.")

# ------------------------------------------------------------------------------------------
# 7. Point d'entrée du script
# ------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()

# ==========================================================================================
# FIN DU SCRIPT — initialisation.py
# ==========================================================================================
