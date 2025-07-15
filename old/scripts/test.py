# ==========================================================================================
# Script      : initialisation.py
# Objectif    : Générer dynamiquement des activités sportives domicile-travail éligibles
# Auteur      : Xavier Rousseau | Juin 2025
#
# Fonctionnalités principales :
# - Génère des employés avec adresses fictives
# - Calcule la distance réelle domicile-travail via Google Maps
# - Filtre selon les règles par mode de transport actif
# - Insère les activités dans PostgreSQL
# ==========================================================================================

import os
import sys
import requests
import psycopg2
import random
from datetime import datetime, timedelta
from faker import Faker
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv

# ------------------------------------------------------------------------------------------
# 1. Setup des chemins et logger
# ------------------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / "initialisation.log"

logger.remove()
logger.add(sys.stdout, level="INFO",
           format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>")
logger.add(str(LOG_FILE), rotation="1 MB", retention="10 days", encoding="utf-8", level="DEBUG")

# ------------------------------------------------------------------------------------------
# 2. Chargement des variables d'environnement
# ------------------------------------------------------------------------------------------
load_dotenv(BASE_DIR.parent / ".env")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Adresse fixe de destination (travail)
ADRESSE_TRAVAIL = "1362 Avenue des Platanes, 34970 Lattes, France"

# Modes de transport éligibles
MODES_TRANSPORT = {
    "marche": 15,
    "course à pied": 15,
    "vélo": 25,
    "trottinette": 25,
    "roller": 25,
    "skateboard": 25
}

faker = Faker("fr_FR")
random.seed(42)

# ------------------------------------------------------------------------------------------
# 3. Fonctions utilitaires
# ------------------------------------------------------------------------------------------

def calculer_distance_km(adresse_depart):
    """
    Utilise l'API Google Maps pour estimer la distance en km entre deux adresses
    """
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": adresse_depart,
        "destinations": ADRESSE_TRAVAIL,
        "key": GOOGLE_API_KEY,
        "mode": "walking"
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        distance_m = data["rows"][0]["elements"][0]["distance"]["value"]
        return distance_m / 1000  # en km
    except Exception as e:
        logger.warning(f"Erreur API Google Maps pour {adresse_depart}: {e}")
        return None

def generer_employe(i):
    """
    Génère un employé fictif avec une adresse et un mode de déplacement éligible
    """
    nom = faker.last_name()
    prenom = faker.first_name()
    adresse = faker.address().replace("\n", ", ")
    mode = random.choice(list(MODES_TRANSPORT.keys()))
    return {
        "id_salarie": i,
        "nom": nom,
        "prenom": prenom,
        "adresse": adresse,
        "mode_deplacement": mode
    }

def generer_activite(id_salarie, mode, distance_km):
    """
    Génère une activité sportive fictive représentant un trajet domicile-travail
    """
    duree_min = int(distance_km * random.uniform(4, 8))  # durée estimée en minutes
    calories = int(distance_km * random.uniform(30, 70))  # estimation calories
    date = faker.date_time_between(start_date='-60d', end_date='now')
    return (
        id_salarie,
        date,
        mode,
        round(distance_km, 2),
        duree_min,
        calories
    )

# ------------------------------------------------------------------------------------------
# 4. Fonction principale
# ------------------------------------------------------------------------------------------

def main(nb_employes=30):
    logger.info(f"🚀 Génération de {nb_employes} employés et activités sportives...")
    conn, cur = None, None
    nb_valides, nb_activites = 0, 0

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()
        logger.success("Connexion PostgreSQL OK.")

        # Création des tables
        cur.execute("DROP TABLE IF EXISTS activites_sportives;")
        cur.execute("""
            CREATE TABLE activites_sportives (
                id SERIAL PRIMARY KEY,
                id_salarie INTEGER,
                date_activite TIMESTAMP,
                mode TEXT,
                distance_km FLOAT,
                duree_min INTEGER,
                calories INTEGER
            );
        """)
        conn.commit()

        # Génération et insertion
        for i in range(1, nb_employes + 1):
            emp = generer_employe(i)
            distance = calculer_distance_km(emp["adresse"])

            if distance is None:
                continue

            max_distance = MODES_TRANSPORT[emp["mode_deplacement"]]
            if distance > max_distance:
                logger.info(f"❌ {emp['prenom']} {emp['nom']} - {distance:.1f} km trop long pour {emp['mode_deplacement']}")
                continue

            activite = generer_activite(emp["id_salarie"], emp["mode_deplacement"], distance)
            cur.execute("""
                INSERT INTO activites_sportives
                (id_salarie, date_activite, mode, distance_km, duree_min, calories)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, activite)
            nb_valides += 1
            nb_activites += 1

        conn.commit()
        logger.success(f"✅ {nb_valides} employés valides, {nb_activites} activités insérées.")
    except Exception as e:
        logger.exception(f"Erreur inattendue : {e}")
        if conn:
            conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()
        logger.info("Connexion PostgreSQL fermée.")

# ------------------------------------------------------------------------------------------
# 5. Point d'entrée
# ------------------------------------------------------------------------------------------

if __name__ == "__main__":
    main(nb_employes=30)

# ==========================================================================================
# FIN DU SCRIPT — initialisation.py
# ==========================================================================================
