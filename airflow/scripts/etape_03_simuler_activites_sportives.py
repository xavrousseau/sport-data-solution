# ==========================================================================================
# Script      : etape_03_simuler_activites_sportives.py
# Objectif    : Générer des activités sportives simulées (type Strava)
#               à partir des salariés éligibles, puis injecter dans PostgreSQL + MinIO.
#               Envoie aussi des notifications ntfy réalistes et messages Kafka.
# Auteur      : Xavier Rousseau | juillet 2025
# ==========================================================================================

import os
import uuid
import json
import tempfile
from random import choice, randint, uniform
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text
from kafka import KafkaProducer

from minio_helper import MinIOHelper
from ntfy_helper import (
    envoyer_message_ntfy,   # ✅ Appel centralisé pour ntfy
    ACTIVITES,
    COMMENTAIRES_REALISTES,
    LIEUX_POPULAIRES,
    EMOJIS_SPORTIFS
)

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
DB_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Fichiers MinIO (clé d’entrée et d’export)
MINIO_RH_KEY = "raw/donnees_rh_cleaned.xlsx"
MINIO_XLSX_KEY = "simulation/activites_sportives.xlsx"
RAW_MINIO_KEY = "raw/activites_sportives_simulees.xlsx"
EXPORT_XLSX_PATH = "exports/simulations_activites_sportives.xlsx"
TMP_DIR = "/tmp"

# Paramètres Kafka
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata_activites")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")

# Paramètres de simulation
NB_MOIS = int(os.getenv("SIMULATION_MONTHS", 12))
ACTIVITES_MIN = int(os.getenv("SIMULATION_MIN_ACTIVITIES", 10))
ACTIVITES_MAX = int(os.getenv("SIMULATION_MAX_ACTIVITIES", 100))
MAX_NTFY_MESSAGES = int(os.getenv("MAX_NTFY_MESSAGES", 30))
TABLE_SQL = "activites_sportives"

# ==========================================================================================
# 2. Fonctions utilitaires : chargement, export, PostgreSQL, Kafka
# ==========================================================================================

def charger_salaries_eligibles_minio():
    """
    Télécharge le fichier RH nettoyé depuis MinIO et retourne les salariés éligibles.
    """
    helper = MinIOHelper()
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        helper.client.download_file(helper.bucket, MINIO_RH_KEY, tmpfile.name)
        logger.success("✅ Données RH éligibles téléchargées depuis MinIO")
        df_rh = pd.read_excel(tmpfile.name)
    return df_rh[["id_salarie", "nom", "prenom"]]

def exporter_excel(df, fichier):
    """
    Exporte un DataFrame en fichier Excel local.
    """
    os.makedirs(os.path.dirname(fichier), exist_ok=True)
    df.to_excel(fichier, index=False)
    logger.success(f"✅ Export Excel : {fichier}")

def upload_file_to_minio(local_file, minio_key, helper):
    """
    Upload d’un fichier local vers MinIO à la clé donnée.
    """
    with open(local_file, "rb") as f:
        helper.client.put_object(
            Bucket=helper.bucket,
            Key=minio_key,
            Body=f,
            ContentLength=os.fstat(f.fileno()).st_size
        )
        logger.success(f"✅ Upload MinIO : {minio_key}")

def inserer_donnees_postgres(df, table_sql, db_conn_string):
    """
    Insertion des données dans PostgreSQL (schéma sportdata).
    """
    engine = create_engine(db_conn_string)
    df.to_sql(table_sql, engine, if_exists="append", index=False, schema="sportdata")
    logger.success(f"✅ PostgreSQL (append) : {table_sql} ({len(df)} lignes)")

def envoyer_message_kafka(producer, topic, message_dict):
    """
    Envoie un message JSON à Kafka via un KafkaProducer.
    """
    try:
        json_msg = json.dumps(message_dict).encode("utf-8")
        producer.send(topic, value=json_msg)
    except Exception as e:
        logger.warning(f"⚠️ Kafka erreur : {e}")

def generer_commentaire(prenom):
    """
    Génère un commentaire réaliste, optionnellement enrichi d’un lieu et d’un emoji.
    """
    texte = choice(COMMENTAIRES_REALISTES)
    if randint(0, 2): texte += f" ({choice(LIEUX_POPULAIRES)})"
    if randint(0, 1): texte += f" {choice(EMOJIS_SPORTIFS)}"
    return texte

# ==========================================================================================
# 3. Simulation des activités sportives (type Strava)
# ==========================================================================================

def simuler_activites_strava(df_salaries, nb_mois, activites_min, activites_max, max_ntfy=MAX_NTFY_MESSAGES):
    """
    Génère des activités sportives simulées pour chaque salarié éligible.
    Envoie aussi une notification ntfy (max une par salarié) et Kafka pour chaque activité.
    """
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    activities, messages_envoyes, ids_notifies = [], 0, set()
    date_debut = datetime.now() - timedelta(days=nb_mois * 30)

    for _, row in df_salaries.iterrows():
        id_salarie, nom, prenom = row["id_salarie"], row["nom"], row["prenom"]

        for _ in range(randint(activites_min, activites_max)):
            sport = choice(ACTIVITES)
            date = date_debut + timedelta(days=randint(0, nb_mois * 30), hours=randint(6, 20), minutes=randint(0, 59))
            distance = int(uniform(1000, 15000))  # en mètres
            temps = int(distance / uniform(2.0, 4.0))  # en secondes
            commentaire = generer_commentaire(prenom) if randint(0, 3) == 0 else ""

            activity = {
                "uid": str(uuid.uuid4()),
                "id_salarie": id_salarie,
                "nom": nom,
                "prenom": prenom,
                "date": date.isoformat(),
                "jour": date.date().isoformat(),
                "date_debut": date.strftime("%d/%m/%Y"),  # JJ/MM/AAAA
                "type_activite": sport,
                "distance_km": round(distance / 1000, 2),
                "temps_sec": temps,
                "commentaire": commentaire
            }

            activities.append(activity)
            envoyer_message_kafka(producer, KAFKA_TOPIC, activity)

            # Envoi de la notification ntfy (une seule fois par salarié)
            if messages_envoyes < max_ntfy and id_salarie not in ids_notifies:
                envoyer_message_ntfy(prenom, sport, distance / 1000, temps // 60)  # ✅ conversion ici
                ids_notifies.add(id_salarie)
                messages_envoyes += 1

    producer.flush()
    return pd.DataFrame(activities)

# ==========================================================================================
# 4. Pipeline principal (exécutable en CLI ou via Airflow)
# ==========================================================================================

def pipeline_simulation_sport():
    """
    Pipeline complet de simulation :
    - Chargement RH depuis MinIO
    - Simulation d’activités sportives
    - Insertion PostgreSQL
    - Export Excel
    - Upload MinIO (simulation + copie raw pour calcul JBE)
    """
    try:
        logger.info("🚀 Simulation d'activités sportives : démarrage...")

        df_salaries = charger_salaries_eligibles_minio()
        if df_salaries.empty:
            logger.warning("⚠️ Aucun salarié éligible. Arrêt.")
            return

        df_activites = simuler_activites_strava(df_salaries, NB_MOIS, ACTIVITES_MIN, ACTIVITES_MAX)
        if df_activites.empty:
            logger.warning("⚠️ Aucune activité générée.")
            return

        logger.info(f"📊 {len(df_activites)} activités simulées.")
        logger.debug(f"Exemples d'activités simulées : {df_activites[['uid', 'prenom', 'type_activite']].head(3).to_dict(orient='records')}")
        inserer_donnees_postgres(df_activites, TABLE_SQL, DB_CONN_STRING)
        exporter_excel(df_activites, EXPORT_XLSX_PATH)

        helper = MinIOHelper()
        if os.path.exists(EXPORT_XLSX_PATH):
            upload_file_to_minio(EXPORT_XLSX_PATH, MINIO_XLSX_KEY, helper)
            upload_file_to_minio(EXPORT_XLSX_PATH, RAW_MINIO_KEY, helper)
        else:
            logger.error(f"❌ Fichier d’export introuvable : {EXPORT_XLSX_PATH}")

    except Exception as e:
        logger.error(f"❌ Erreur critique dans le pipeline de simulation : {e}")
        raise

# ==========================================================================================
# 5. Point d’entrée CLI
# ==========================================================================================

if __name__ == "__main__":
    pipeline_simulation_sport()
