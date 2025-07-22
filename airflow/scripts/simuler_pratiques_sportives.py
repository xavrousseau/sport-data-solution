# ==========================================================================================
# Script      : simuler_pratiques_sportives.py
# Objectif    : Générer des activités sportives simulées (type Strava)
#               à partir des salariés éligibles et les injecter dans PostgreSQL + MinIO.
#               Envoie aussi des notifications ntfy simulant un Slack-like + Kafka.
# Auteur      : Xavier Rousseau | Version propre et commentée - Juillet 2025
# ==========================================================================================

import os
import uuid
import json
import tempfile
from random import choice, randint, uniform
from datetime import datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine
from kafka import KafkaProducer

from minio_helper import MinIOHelper

# ==========================================================================================
# 1. Chargement des variables d'environnement depuis le fichier .env
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
EXPORT_XLSX_PATH = "airflow/data/outputs/simulations_activites_sportives.xlsx"
TMP_DIR = "/tmp"

NTFY_URL = os.getenv("NTFY_URL", "http://localhost:8080")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata_activites")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata_activites")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")

NB_MOIS = int(os.getenv("SIMULATION_MONTHS", 12))
ACTIVITES_MIN = int(os.getenv("SIMULATION_MIN_ACTIVITIES", 10))
ACTIVITES_MAX = int(os.getenv("SIMULATION_MAX_ACTIVITIES", 100))
TABLE_SQL = "activites_sportives"

# ==========================================================================================
# 2. Données statiques de simulation (types d'activités, commentaires, lieux, emojis)
# ==========================================================================================

ACTIVITES = [
    "Course à pied", "Marche", "Vélo", "Trottinette", "Roller", "Skateboard",
    "Randonnée", "Natation", "Escalade", "Fitness", "Musculation",
    "Boxe", "Tennis", "Basketball", "Football", "Badminton",
    "Yoga", "Pilates", "Danse", "Karaté", "Judo"
]

COMMENTAIRES_REALISTES = [
    "Reprise en douceur après une pause.",
    "Très bonne séance aujourd'hui ! 💪",
    "C'était dur mais je ne regrette pas.",
    "J'ai battu mon record perso !",
    "Belle sortie dans la nature.",
    "Bonne ambiance, bon rythme.",
    "Temps idéal pour ce sport.",
    "Encore un effort avant le week-end !",
    "Avec quelques collègues du bureau.",
    "Motivé(e) comme jamais aujourd’hui !",
    "Petite séance rapide entre midi et deux.",
    "Pas facile, mais ça fait du bien.",
    "Objectif atteint pour aujourd’hui.",
    "J’ai testé un nouveau parcours.",
    "De belles sensations malgré le vent.",
    "Un peu fatigué(e), mais satisfait(e).",
    "Toujours un plaisir de bouger.",
    "Je progresse petit à petit.",
    "Une sortie plus longue que prévu.",
    "Ça m’a vidé la tête !",
    "Retour progressif après blessure.",
    "Session matinale pour bien démarrer.",
    "Bonne séance cardio aujourd’hui.",
    "J’ai bien transpiré 😅",
    "Toujours motivé(e) même sous la pluie.",
    "Rien de mieux qu’un peu de sport pour décompresser.",
    "Sortie découverte dans un nouveau coin.",
    "Avec de la musique dans les oreilles, c’est encore mieux 🎧",
    "Un peu raide aujourd’hui, mais content(e) d’avoir bougé.",
    "Beaucoup de monde dehors, mais bonne ambiance.",
    "Une belle montée, j’ai souffert mais je suis fier(e)."
]

LIEUX_POPULAIRES = [
    "au parc du Thabor", "le long du canal d’Ille-et-Rance", "sur les quais de Bordeaux",
    "au bord du lac d’Annecy", "dans les bois de Vincennes", "au parc de la Tête d'Or",
    "au bord du Lez", "à la plage du Prado", "dans la forêt de Fontainebleau",
    "au canal du Midi", "vers Saint-Guilhem", "sur les berges de la Garonne"
]

EMOJIS_SPORTIFS = ["💪", "🔥", "🌟", "🏃‍♂️", "🚴‍♀️", "🏞️", "😅", "🙌", "⛰️", "🎯"]

# ==========================================================================================
# 3. Fonctions utilitaires : MinIO, PostgreSQL, Kafka, ntfy, etc.
# ==========================================================================================

def charger_salaries_eligibles_minio():
    helper = MinIOHelper()
    with tempfile.NamedTemporaryFile(suffix=".xlsx", dir=TMP_DIR) as tmpfile:
        helper.client.download_file(helper.bucket, MINIO_RH_KEY, tmpfile.name)
        logger.success("✅ Données RH éligibles téléchargées depuis MinIO")
        df_rh = pd.read_excel(tmpfile.name)
    return df_rh[["id_salarie", "nom", "prenom"]]

def envoyer_message_ntfy(prenom, sport, distance, temps, commentaire=""):
    km = distance / 1000
    minutes = temps // 60
    message = f"{prenom} a fait {km:.1f} km de {sport.lower()} en {minutes} min. {commentaire}"
    try:
        requests.post(f"{NTFY_URL}/{NTFY_TOPIC}", data=message.encode("utf-8"))
        logger.debug(f"🔔 ntfy envoyé : {message}")
    except Exception as e:
        logger.warning(f"⚠️ Erreur ntfy : {e}")

def envoyer_message_kafka(producer, topic, message_dict):
    try:
        json_msg = json.dumps(message_dict).encode("utf-8")
        producer.send(topic, value=json_msg)
    except Exception as e:
        logger.warning(f"⚠️ Kafka erreur : {e}")

def exporter_excel(df, fichier):
    os.makedirs(os.path.dirname(fichier), exist_ok=True)
    df.to_excel(fichier, index=False)
    logger.success(f"✅ Export Excel : {fichier}")

def upload_file_to_minio(local_file, minio_key, helper):
    with open(local_file, "rb") as f:
        helper.client.put_object(
            Bucket=helper.bucket,
            Key=minio_key,
            Body=f,
            ContentLength=os.fstat(f.fileno()).st_size
        )
        logger.success(f"✅ Upload MinIO : {minio_key}")

def inserer_donnees_postgres(df, table_sql, db_conn_string):
    engine = create_engine(db_conn_string)
    df.to_sql(table_sql, engine, if_exists="replace", index=False, schema="sportdata")
    logger.success(f"✅ PostgreSQL inséré : {table_sql} ({len(df)} lignes)")

def generer_commentaire(prenom):
    texte = choice(COMMENTAIRES_REALISTES)
    if randint(0, 2): texte += f" ({choice(LIEUX_POPULAIRES)})"
    if randint(0, 1): texte += f" {choice(EMOJIS_SPORTIFS)}"
    return texte

# ==========================================================================================
# 4. Génération des activités sportives simulées (type Strava)
# ==========================================================================================

def simuler_activites_strava(df_salaries, nb_mois, activites_min, activites_max, max_ntfy=30):
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    activities, messages_envoyes, ids_notifies = [], 0, set()
    date_debut = datetime.now() - timedelta(days=nb_mois * 30)

    for _, row in df_salaries.iterrows():
        id_salarie, nom, prenom = row["id_salarie"], row["nom"], row["prenom"]
        for _ in range(randint(activites_min, activites_max)):
            sport = choice(ACTIVITES)
            date = date_debut + timedelta(days=randint(0, nb_mois * 30), hours=randint(6, 20), minutes=randint(0, 59))
            distance = int(uniform(1000, 15000))
            temps = int(distance / uniform(2.0, 4.0))
            commentaire = generer_commentaire(prenom) if randint(0, 3) == 0 else ""

            activity = {
                "uid": str(uuid.uuid4()),
                "id_salarie": id_salarie,
                "nom": nom,
                "prenom": prenom,
                "date": date.isoformat(),
                "jour": date.date().isoformat(),
                "type_activite": sport,
                "distance_km": round(distance / 1000, 2),
                "temps_sec": temps,
                "commentaire": commentaire
            }

            activities.append(activity)
            envoyer_message_kafka(producer, KAFKA_TOPIC, activity)

            if messages_envoyes < max_ntfy and id_salarie not in ids_notifies:
                envoyer_message_ntfy(prenom, sport, distance, temps, commentaire)
                ids_notifies.add(id_salarie)
                messages_envoyes += 1

    producer.flush()
    return pd.DataFrame(activities)

# ==========================================================================================
# 5. Pipeline principal (réutilisable via Airflow ou CLI)
# ==========================================================================================

def pipeline_simulation_sport():
    try:
        logger.info("=== Simulation d'activités sportives : Démarrage ===")

        df_salaries = charger_salaries_eligibles_minio()
        df_activites = simuler_activites_strava(df_salaries, NB_MOIS, ACTIVITES_MIN, ACTIVITES_MAX)

        logger.info(f"📊 {len(df_activites)} activités simulées.")

        inserer_donnees_postgres(df_activites, TABLE_SQL, DB_CONN_STRING)
        exporter_excel(df_activites, EXPORT_XLSX_PATH)
        upload_file_to_minio(EXPORT_XLSX_PATH, MINIO_XLSX_KEY, MinIOHelper())

        logger.success("🎯 Simulation terminée avec succès ✅")

    except Exception as e:
        logger.error(f"❌ Erreur dans la simulation : {e}")
        raise

# ==========================================================================================
# 6. Exécution directe (CLI)
# ==========================================================================================

if __name__ == "__main__":
    pipeline_simulation_sport()
