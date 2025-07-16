# ==========================================================================================
# Script      : simuler_pratiques_sportives.py
# Objectif    : Générer des activités sportives simulées (type Strava)
#               à partir des salariés éligibles et les injecter dans PostgreSQL + MinIO.
#               Envoie aussi des notifications ntfy simulant un Slack-like + Kafka.
# Auteur      : Xavier Rousseau | Mis à jour Juillet 2025
# ==========================================================================================

import pandas as pd
import numpy as np
from faker import Faker
from random import choice, randint, uniform
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from loguru import logger
import os
import uuid
import json
import tempfile
import requests
from dotenv import load_dotenv
from minio_helper import MinIOHelper
from kafka import KafkaProducer

# ==========================================================================================
# 1. Chargement des variables d’environnement (.env)
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

MINIO_RH_KEY = "raw/donnees_rh_cleaned.xlsx"
MINIO_XLSX_KEY = "simulation/activites_sportives.xlsx"
TMP_DIR = "/tmp"

NTFY_URL = os.getenv("NTFY_URL", "http://localhost:8080")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata_activites")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata_activites")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")

NB_MOIS = int(os.getenv("SIMULATION_MONTHS", 12))
ACTIVITES_MIN = int(os.getenv("SIMULATION_MIN_ACTIVITIES", 10))
ACTIVITES_MAX = int(os.getenv("SIMULATION_MAX_ACTIVITIES", 100))

EXPORT_XLSX_PATH = "airflow/data/outputs/simulations_activites_sportives.xlsx"
TABLE_SQL = "activites_sportives"

ACTIVITES = [
    "Course à pied", "Marche", "Vélo", "Trottinette", "Roller", "Skateboard",
    "Randonnée", "Natation", "Escalade", "Fitness", "Musculation",
    "Boxe", "Tennis", "Basketball", "Football", "Badminton",
    "Yoga", "Pilates", "Danse", "Karaté", "Judo"
]

fake = Faker("fr_FR")

# ==========================================================================================
# 2. Chargement des salariés éligibles depuis MinIO
# ==========================================================================================
def charger_salaries_eligibles_minio():
    helper = MinIOHelper()
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        helper.client.download_file(helper.bucket, MINIO_RH_KEY, tmpfile.name)
        logger.success(f"✅ Fichier RH nettoyé téléchargé depuis MinIO : {tmpfile.name}")
        df_rh = pd.read_excel(tmpfile.name)
    col_id = next((c for c in df_rh.columns if "id" in c.lower() and "salarie" in c.lower()), "id_salarie")
    return df_rh[[col_id, "nom", "prenom"]].rename(columns={col_id: "id_salarie"})

# ==========================================================================================
# 3. NTFY : Message simulé de Slack-like
# ==========================================================================================
def envoyer_message_ntfy(prenom, sport, distance, temps, commentaire=""):
    km = distance / 1000
    minutes = temps // 60
    sports_deplacement = {
        "Course à pied", "Marche", "Vélo", "Trottinette", "Roller", "Skateboard",
        "Randonnée", "Natation"
    }
    is_deplacement = sport in sports_deplacement

    if is_deplacement:
        messages_motivants = [
            f"🔥 Bravo {prenom} ! Tu viens de faire {km:.1f} km de {sport.lower()} en {minutes} min. 💪",
            f"🏅 {prenom} vient de compléter {km:.1f} km de {sport.lower()} — impressionnant !",
            f"🚴‍♂️ {prenom} s’est dépassé avec {km:.1f} km de {sport.lower()} aujourd’hui !",
            f"✨ {prenom} continue sur sa lancée : {minutes} minutes de {sport.lower()}.",
        ]
        messages_fun = [
            f"🔥 Bravo {prenom} ! Tu viens de faire {km:.1f} km de {sport.lower()} en {minutes} min — {commentaire}",
            f"🎉 Activité de {prenom} : {sport.lower()} sur {km:.1f} km — {commentaire}",
        ]
    else:
        messages_motivants = [
            f"💪 {prenom} a transpiré pendant {minutes} minutes de {sport.lower()} !",
            f"✨ {prenom} enchaîne avec {minutes} min de {sport.lower()} — respect !",
            f"🏋️ {prenom} vient de terminer une session intense de {sport.lower()} ({minutes} min)",
            f"🔥 {prenom} s’est donné à fond pendant {minutes} minutes de {sport.lower()}",
        ]
        messages_fun = [
            f"📢 {prenom} en {sport.lower()} : {minutes} min — {commentaire}",
            f"🎊 {prenom} vient de boucler {minutes} min de {sport.lower()} — {commentaire}"
        ]

    message = choice(messages_fun) if commentaire and randint(0, 1) == 0 else choice(messages_motivants)
    try:
        response = requests.post(f"{NTFY_URL}/{NTFY_TOPIC}", data=message.encode("utf-8"))
        if response.status_code == 200:
            logger.debug(f"📢 Message ntfy envoyé : {message}")
        else:
            logger.warning(f"⚠️ Envoi ntfy échoué ({response.status_code}) : {message}")
    except Exception as e:
        logger.error(f"Erreur ntfy : {e}")

# ==========================================================================================
# 4. Envoi dans Kafka (chaque message = activité JSON)
# ==========================================================================================
def envoyer_message_kafka(producer, topic, message_dict):
    try:
        json_msg = json.dumps(message_dict).encode("utf-8")
        producer.send(topic, value=json_msg)
        logger.debug(f"🛰️ Message Kafka envoyé : {message_dict}")
    except Exception as e:
        logger.error(f"Erreur Kafka : {e}")

# ==========================================================================================
# 5. Simulation des activités sportives + ntfy + kafka
# ==========================================================================================
def simuler_activites_strava(df_salaries, nb_mois, activites_min, activites_max, max_ntfy=10):
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
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

            activity = {
                "uid": str(uuid.uuid4()),
                "id_salarie": id_salarie,
                "nom": nom,
                "prenom": prenom,
                "date": date_activite.isoformat(),
                "jour": date_activite.date().isoformat(),
                "type_activite": sport_type,
                "distance_km": round(distance / 1000, 2),
                "temps_sec": temps,
                "commentaire": commentaire
            }

            activities.append(activity)
            envoyer_message_kafka(producer, KAFKA_TOPIC, activity)

            if messages_envoyes < max_ntfy:
                envoyer_message_ntfy(prenom, sport_type, distance, temps, commentaire)
                messages_envoyes += 1

    producer.flush()
    return pd.DataFrame(activities)

# ==========================================================================================
# 6. Export des résultats
# ==========================================================================================
def exporter_excel(df, fichier):
    os.makedirs(os.path.dirname(fichier), exist_ok=True)
    df.to_excel(fichier, index=False)
    logger.success(f"✅ Export Excel : {fichier}")

def upload_file_to_minio(local_file, minio_key, helper):
    with open(local_file, "rb") as f:
        helper.client.put_object(helper.bucket, minio_key, f, length=os.fstat(f.fileno()).st_size)
        logger.success(f"✅ Fichier uploadé sur MinIO : {minio_key}")

def inserer_donnees_postgres(df, table_sql, db_conn_string):
    engine = create_engine(db_conn_string)
    df.to_sql(table_sql, engine, if_exists="replace", index=False, schema="sportdata")
    logger.success(f"✅ {len(df)} activités insérées dans PostgreSQL (table '{table_sql}')")

# ==========================================================================================
# 7. Pipeline principal
# ==========================================================================================
if __name__ == "__main__":
    try:
        logger.info("=== Démarrage simulation d'activités sportives ===")
        df_salaries = charger_salaries_eligibles_minio()
        df_activites = simuler_activites_strava(df_salaries, NB_MOIS, ACTIVITES_MIN, ACTIVITES_MAX)

        logger.info("📊 Récapitulatif des activités générées :")
        logger.info(f"➡️ Total d’activités simulées : {len(df_activites)}")
        for sport, count in df_activites["type_activite"].value_counts().items():
            logger.info(f" - {sport:<15} : {count} activité(s)")

        logger.info("\n👥 Top 5 des salariés les plus actifs :")
        for id_salarie, count in df_activites["id_salarie"].value_counts().head(5).items():
            logger.info(f" - ID {id_salarie:<10} : {count} activité(s)")

        inserer_donnees_postgres(df_activites, TABLE_SQL, DB_CONN_STRING)
        exporter_excel(df_activites, EXPORT_XLSX_PATH)
        helper = MinIOHelper()
        upload_file_to_minio(EXPORT_XLSX_PATH, MINIO_XLSX_KEY, helper)

        logger.success("🎯 Pipeline terminé : PostgreSQL + MinIO + ntfy + Kafka ✅")
    except Exception as e:
        logger.error(f"❌ Erreur pipeline simulation : {e}")
        raise

# ==========================================================================================
# Fin du fichier – Simulation d’activités sportives avec messages variés et export structuré
# ==========================================================================================
