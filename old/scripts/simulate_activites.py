# ==========================================================================================
# Script      : simulate_activites.py
# Objectif    : Générer des activités sportives réalistes pour les employés en base (PostgreSQL)
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import sys
import random
import psycopg2
from dotenv import load_dotenv
from faker import Faker
from datetime import datetime, timedelta
from loguru import logger
from pathlib import Path
from utils_google import verifier_eligibilite

# ------------------------------------------------------------------------------------------
# 1. Setup environnement
# ------------------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logger.remove()
logger.add(sys.stdout, level="INFO",
           format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>")
logger.add(str(LOGS_DIR / "simulate_activites.log"), rotation="1 MB", retention="10 days", encoding="utf-8")

load_dotenv(BASE_DIR.parent / ".env")

DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
SIMULATION_MONTHS = int(os.getenv("SIMULATION_MONTHS", 12))
MIN_ACTIVITIES = int(os.getenv("SIMULATION_MIN_ACTIVITIES", 10))
MAX_ACTIVITIES = int(os.getenv("SIMULATION_MAX_ACTIVITIES", 100))

faker = Faker("fr_FR")
random.seed(42)

# ------------------------------------------------------------------------------------------
# 2. Génération d'activités sportives pour un employé
# ------------------------------------------------------------------------------------------
def generer_activites(id_salarie, mode, nb):
    activites = []
    for _ in range(nb):
        date = faker.date_time_between(start_date=f"-{SIMULATION_MONTHS}M", end_date="now")
        distance = random.randint(2000, 15000)
        duree = random.randint(900, 5400)
        commentaire = f"Session de {mode} - {faker.word()} motivante."
        activites.append((id_salarie, date, mode, distance, duree, commentaire))
    return activites

# ------------------------------------------------------------------------------------------
# 3. Fonction principale
# ------------------------------------------------------------------------------------------
def main():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()
        logger.success("Connexion PostgreSQL OK")

        cur.execute("SELECT id_salarie, adresse_du_domicile, moyen_de_deplacement FROM employes;")
        employes = cur.fetchall()
        logger.info(f"{len(employes)} employes chargés.")

        cur.execute("DROP TABLE IF EXISTS activites_sportives;")
        cur.execute("""
            CREATE TABLE activites_sportives (
                id SERIAL PRIMARY KEY,
                id_salarie INTEGER,
                date_activite TIMESTAMP,
                type_activite TEXT,
                distance_m FLOAT,
                duree_s INTEGER,
                commentaire TEXT
            );
        """)
        conn.commit()

        inserted = 0
        for emp in employes:
            id_salarie, adresse, mode = emp
            if not verifier_eligibilite(adresse, mode):
                continue
            nb = random.randint(MIN_ACTIVITIES, MAX_ACTIVITIES)
            activites = generer_activites(id_salarie, mode, nb)
            for act in activites:
                cur.execute("""
                    INSERT INTO activites_sportives
                    (id_salarie, date_activite, type_activite, distance_m, duree_s, commentaire)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, act)
                inserted += 1

        conn.commit()
        logger.success(f"{inserted} activités sportives insérées.")

    except Exception as e:
        logger.exception(f"Erreur : {e}")
        if conn:
            conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()
        logger.info("Connexion PostgreSQL fermée.")

# ------------------------------------------------------------------------------------------
# 4. Entrée
# ------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()

# ==========================================================================================
# FIN DU SCRIPT
# ==========================================================================================
