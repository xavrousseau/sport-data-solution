# ==========================================================================================
# Script      : etape_03_simuler_activites_sportives.py
# Objectif    : Simuler des activités sportives à partir des employés éligibles (PostgreSQL),
#               puis stocker ces activités dans PostgreSQL, MinIO (Excel) et Kafka (JSON brut).
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import json
import random
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
from loguru import logger
from minio_helper import MinIOHelper
from kafka import KafkaProducer

# ==========================================================================================
# 1. Chargement des variables d’environnement (.env global)
# ==========================================================================================

load_dotenv(dotenv_path=".env", override=True)

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata.sportdata.activites_sportives")

MINIO_EXPORT_KEY = "clean/activites_sportives_simulees.xlsx"

# ==========================================================================================
# 2. Paramètres de simulation
# ==========================================================================================

SIMULATION_MONTHS = int(os.getenv("SIMULATION_MONTHS", 12))
SIMULATION_MIN = int(os.getenv("SIMULATION_MIN_ACTIVITIES", 10))
SIMULATION_MAX = int(os.getenv("SIMULATION_MAX_ACTIVITIES", 100))

TYPES_ACTIVITES = [
    "Course à pied", "Marche", "Vélo", "Trottinette", "Roller", "Skateboard",
    "Randonnée", "Natation", "Escalade", "Fitness", "Musculation",
    "Boxe", "Tennis", "Basketball", "Football", "Badminton",
    "Yoga", "Pilates", "Danse", "Karaté", "Judo",
    "Handball", "Rugby", "Ping-pong", "Padel", "Squash",
    "CrossFit", "Ski alpin", "Ski de fond", "Snowboard", "Aviron",
    "Canoë-kayak", "Surf", "Kitesurf", "Plongée", "Stand-up paddle",
    "Marche nordique", "Parkour", "Gymnastique", "Trampoline"
]


def generer_commentaire(prenom):
    """
    Génère un commentaire réaliste, optionnellement enrichi d’un lieu et d’un emoji.
    """
    texte = choice(COMMENTAIRES_REALISTES)
    if randint(0, 2): texte += f" ({choice(LIEUX_POPULAIRES)})"
    if randint(0, 1): texte += f" {choice(EMOJIS_SPORTIFS)}"
    return texte

# ==========================================================================================
# 3. Génération d’une activité sportive
# ==========================================================================================

def generer_activite(salarie):
    """
    Génère une activité sportive réaliste pour un salarié éligible.
    Retourne un dictionnaire prêt à être inséré/exporté.
    """
    jours_aleatoires = random.randint(0, 30 * SIMULATION_MONTHS)
    date_activite = datetime.today() - timedelta(days=jours_aleatoires)
    type_activite = random.choice(TYPES_ACTIVITES)
    distance = round(random.uniform(1.5, 25), 2)
    temps = int(distance * random.uniform(5, 10))  # minutes
    date_debut = date_activite.replace(hour=random.randint(6, 20), minute=random.randint(0, 59))
    commentaire = generer_commentaire(prenom) if randint(0, 1) == 0 else ""
    return {
        "uid": f"{salarie['id_salarie']}_{date_activite.strftime('%Y%m%d')}_{random.randint(1000, 9999)}",
        "id_salarie": salarie["id_salarie"],
        "nom": salarie["nom"],
        "prenom": salarie["prenom"],
        "date": date_activite.strftime("%Y-%m-%d"),
        "jour": date_activite.strftime("%A"),
        "date_debut": date_debut.strftime("%Y-%m-%d %H:%M:%S"),
        "type_activite": type_activite,
        "distance_km": distance,
        "temps_sec": temps * 60,
        "commentaire": commentaire
    }

# ==========================================================================================
# 4. Pipeline principal
# ==========================================================================================

def pipeline_simulation():
    """
    Pipeline principal de simulation d'activités sportives :
    - Récupération des salariés sportifs depuis PostgreSQL
    - Génération des activités simulées
    - Insertion PostgreSQL (append, sans écraser)
    - Export Excel vers MinIO
    - Publication Kafka (format Debezium simulé)
    """
    logger.info("🚀 Démarrage de la simulation d’activités sportives")

    # Connexion à PostgreSQL
    engine = create_engine(DB_CONN_STRING)
    df_salaries = pd.read_sql("SELECT * FROM sportdata.employes WHERE deplacement_sportif = True", engine)

    if df_salaries.empty:
        logger.warning("⚠️ Aucun salarié sportif trouvé dans la base.")
        return

    logger.info(f"✅ {len(df_salaries)} salariés sportifs identifiés pour simulation")

    # Génération des activités simulées
    activites = []
    for _, salarie in df_salaries.iterrows():
        nb_activites = random.randint(SIMULATION_MIN, SIMULATION_MAX)
        for _ in range(nb_activites):
            activite = generer_activite(salarie)
            activites.append(activite)

    df_activites = pd.DataFrame(activites)
    logger.info(f"✅ {len(df_activites)} activités simulées pour {len(df_salaries)} salariés")

    # Insertion dans PostgreSQL en mode 'append' (ajout sans écraser la table)
    df_activites.to_sql("activites_sportives", engine, index=False, if_exists="append", schema="sportdata")
    logger.success("📦 Données insérées dans PostgreSQL (sportdata.activites_sportives)")

    # Export Excel vers MinIO
    helper = MinIOHelper()
    helper.upload_excel(df_activites, MINIO_EXPORT_KEY, "Activités simulées")

    # Publication vers Kafka (format Debezium simulé)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for _, row in df_activites.iterrows():
        # Construction du message Debezium simulé
        message = {
            "payload": {
                "op": "c",  # 'c' pour création (insert)
                "after": row.to_dict(),
                "ts_ms": int(datetime.utcnow().timestamp() * 1000)  # Timestamp Debezium
            }
        }
        # Envoi du message au topic Kafka
        producer.send(KAFKA_TOPIC, value=message)
        logger.info(f"📤 Message Kafka envoyé : {message['payload']['after']['uid']}")

    # Flush (force l'envoi des messages restants)
    producer.flush()
    logger.success(f"📤 {len(df_activites)} messages Kafka envoyés sur le topic {KAFKA_TOPIC}")

    logger.success("🎯 Simulation terminée avec succès.")


# ==========================================================================================
# 5. Point d’entrée
# ==========================================================================================

if __name__ == "__main__":
    try:
        pipeline_simulation()
    except Exception as e:
        logger.error(f"❌ Erreur lors de la simulation : {e}")
