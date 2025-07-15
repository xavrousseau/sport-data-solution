# ==========================================================================================
# Script      : simuler_pratiques_sportives.py
# Objectif    : Générer des activités sportives simulées (type Strava)
#               à partir des salariés éligibles et les injecter dans PostgreSQL + MinIO.
#               Envoie aussi des notifications ntfy simulant un Slack-like.
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import pandas as pd
import numpy as np
from faker import Faker
from random import choice, randint, uniform
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from loguru import logger
import os
from dotenv import load_dotenv
from minio_helper import MinIOHelper
import tempfile
import requests
import uuid

# ==========================================================================================
# 1. Chargement des variables d’environnement (.env)
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

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

# Connexion MinIO
MINIO_RH_KEY = "raw/donnees_rh_cleaned.xlsx"
MINIO_XLSX_KEY = "simulation/activites_sportives.xlsx"
TMP_DIR = "/tmp"

# Notifications ntfy
NTFY_URL = os.getenv("NTFY_URL", "http://localhost:8080")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata.activites")

# Paramètres de simulation depuis .env
NB_MOIS = int(os.getenv("SIMULATION_MONTHS", 12))
ACTIVITES_MIN = int(os.getenv("SIMULATION_MIN_ACTIVITIES", 10))
ACTIVITES_MAX = int(os.getenv("SIMULATION_MAX_ACTIVITIES", 100))

# Fichier de sortie local
FICHIER_EXPORT_XLSX = "simulations_activites_sportives.xlsx"
TABLE_SQL = "activites_sportives"

# Liste d'activités enrichie (déplacements + sports de loisir)
ACTIVITES = [
    "Course à pied", "Marche", "Vélo", "Trottinette", "Roller", "Skateboard",
    "Randonnée", "Natation", "Escalade", "Fitness", "Musculation",
    "Boxe", "Tennis", "Basketball", "Football", "Badminton",
    "Yoga", "Pilates", "Danse", "Karaté", "Judo"
]

fake = Faker("fr_FR")

# ==========================================================================================
# 2. Chargement des salariés éligibles depuis MinIO (issus du nettoyage RH)
# ==========================================================================================
def charger_salaries_eligibles_minio():
    helper = MinIOHelper()
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        helper.client.download_file(
            Bucket=helper.bucket,
            Key=MINIO_RH_KEY,
            Filename=tmpfile.name
        )
        logger.success(f"✅ Fichier RH nettoyé téléchargé depuis MinIO : {tmpfile.name}")
        df_rh = pd.read_excel(tmpfile.name)
    col_id = next((c for c in df_rh.columns if "id" in c.lower() and "salarie" in c.lower()), "id_salarie")
    return df_rh[[col_id, "nom", "prenom"]].rename(columns={col_id: "id_salarie"})

# ==========================================================================================
# 3. Envoi d'une notification ntfy (simule un Slack-like)
# ==========================================================================================
def envoyer_message_ntfy(prenom, sport, distance, temps, commentaire=""):
    km = distance / 1000
    minutes = temps // 60
    message = f"🔥 Bravo {prenom} ! Tu viens de faire {km:.1f} km de {sport.lower()} en {minutes} min"
    if commentaire:
        message += f" — {commentaire}"
    try:
        response = requests.post(f"{NTFY_URL}/{NTFY_TOPIC}", data=message.encode("utf-8"))
        if response.status_code == 200:
            logger.debug(f"📢 Message ntfy envoyé : {message}")
        else:
            logger.warning(f"⚠️ Envoi ntfy échoué ({response.status_code}) : {message}")
    except Exception as e:
        logger.error(f"Erreur ntfy : {e}")

# ==========================================================================================
# 4. Simulation d'activités sportives
# ==========================================================================================
def simuler_activites_strava(df_salaries, nb_mois, activites_min, activites_max, max_ntfy=10):
    activities = []
    messages_envoyes = 0
    date_debut = datetime.now() - timedelta(days=nb_mois * 30)

    for _, row in df_salaries.iterrows():
        id_salarie = row["id_salarie"]
        nom = row["nom"]
        prenom = row["prenom"]
        nb_activites = randint(activites_min, activites_max)

        for _ in range(nb_activites):
            sport_type = choice(ACTIVITES)
            date_activite = date_debut + timedelta(
                days=randint(0, nb_mois * 30),
                hours=randint(6, 20),
                minutes=randint(0, 59)
            )

            # Distance et temps simulés selon le sport
            if sport_type in ["Course à pied", "Running", "Marche", "Trottinette", "Roller", "Skateboard"]:
                distance = int(uniform(2000, 15000))
                temps = int(distance / uniform(1.8, 3.5))
            elif sport_type == "Vélo":
                distance = int(uniform(4000, 35000))
                temps = int(distance / uniform(4.5, 10))
            elif sport_type == "Randonnée":
                distance = int(uniform(8000, 25000))
                temps = int(distance / uniform(2, 4))
            elif sport_type == "Natation":
                distance = int(uniform(400, 2500))
                temps = int(distance / uniform(0.8, 1.6))
            else:
                distance = int(uniform(1000, 8000))
                temps = int(distance / uniform(1.5, 3.0))

            commentaire = fake.sentence(nb_words=8) if randint(0, 3) == 0 else ""

            activities.append({
                "uid": str(uuid.uuid4()),
                "id_salarie": id_salarie,
                "nom": nom,
                "prenom": prenom,
                "date": date_activite,
                "jour": date_activite.date(),
                "type_activite": sport_type,
                "distance_km": round(distance / 1000, 2),
                "temps_sec": temps,
                "commentaire": commentaire
            })

            if messages_envoyes < max_ntfy:
                envoyer_message_ntfy(prenom, sport_type, distance, temps, commentaire)
                messages_envoyes += 1

    df = pd.DataFrame(activities)
    logger.info(f"Nombre total d'activités simulées : {len(df)}")
    return df

# ==========================================================================================
# 5. Export vers Excel, MinIO et PostgreSQL
# ==========================================================================================
def exporter_excel(df, fichier):
    try:
        df.to_excel(fichier, index=False)
        logger.success(f"✅ Export Excel : {fichier}")
    except Exception as e:
        logger.error(f"Erreur export Excel : {e}")

def upload_file_to_minio(local_file, minio_key, helper):
    with open(local_file, "rb") as f:
        file_size = os.fstat(f.fileno()).st_size
        try:
            helper.client.put_object(
                Bucket=helper.bucket,
                Key=minio_key,
                Body=f,
                ContentLength=file_size
            )
            logger.success(f"✅ Fichier uploadé sur MinIO : {minio_key}")
        except Exception as e:
            logger.error(f"Erreur upload MinIO : {e}")

def inserer_donnees_postgres(df, table_sql, db_conn_string):
    engine = create_engine(db_conn_string)
    try:
        df.to_sql(table_sql, engine, if_exists="replace", index=False, schema="sportdata")
        logger.success(f"✅ {len(df)} activités insérées dans PostgreSQL (table '{table_sql}')")
    except Exception as e:
        logger.error(f"Erreur PostgreSQL : {e}")

# ==========================================================================================
# 6. Pipeline principal
# ==========================================================================================
if __name__ == "__main__":
    try:
        logger.info("=== Démarrage simulation d'activités sportives ===")
        df_salaries = charger_salaries_eligibles_minio()
        df_activites = simuler_activites_strava(df_salaries, NB_MOIS, ACTIVITES_MIN, ACTIVITES_MAX)

        # Récapitulatif par activité
        logger.info("📊 Récapitulatif des activités générées :")
        logger.info(f"➡️ Total d’activités simulées : {len(df_activites)}")
        for sport, count in df_activites["type_activite"].value_counts().items():
            logger.info(f" - {sport:<15} : {count} activité(s)")

        # Top salariés les plus actifs
        logger.info("\n👥 Top 5 des salariés les plus actifs :")
        for id_salarie, count in df_activites["id_salarie"].value_counts().head(5).items():
            logger.info(f" - ID {id_salarie:<10} : {count} activité(s)")

        inserer_donnees_postgres(df_activites, TABLE_SQL, DB_CONN_STRING)
        exporter_excel(df_activites, FICHIER_EXPORT_XLSX)
        helper = MinIOHelper()
        upload_file_to_minio(FICHIER_EXPORT_XLSX, MINIO_XLSX_KEY, helper)

        logger.success("🎯 Pipeline terminé : PostgreSQL + MinIO + ntfy ✅")
    except Exception as e:
        logger.error(f"❌ Erreur pipeline simulation : {e}")
        raise


# ==========================================================================================
# Fin du fichier – Simulation d’activités sportives avec paramètres .env et ntfy
# ==========================================================================================
