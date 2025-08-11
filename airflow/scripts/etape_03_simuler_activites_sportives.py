# ==========================================================================================
# Script      : etape_03_simuler_activites_sportives.py (version réaliste avec profils & saison)
# Objectif    : Générer des activités sportives simulées réalistes à partir des employés éligibles (PostgreSQL),
#               stocker dans PostgreSQL, MinIO (Excel), publier sur Kafka (Debezium JSON enveloppé).
# Auteur      : Xavier Rousseau | Août 2025
# ==========================================================================================

import os
import json
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from loguru import logger
from kafka import KafkaProducer

from minio_helper import MinIOHelper
from ntfy_helper import (
    # envoyer_message_ntfy,   # (SUPPRIMÉ : notifications désormais côté Spark uniquement)
    ACTIVITES,
    COMMENTAIRES_REALISTES,
    LIEUX_POPULAIRES,
    EMOJIS_SPORTIFS
)

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

SIMULATION_MONTHS = int(os.getenv("SIMULATION_MONTHS", 12))

# ==========================================================================================
# 2. Profils sportifs, saisonnalité & calendrier réaliste
# ==========================================================================================

PROFILS = [
    {"label": "Sédentaire", "proba": 0.15, "min": 5, "max": 15, "sports": ["Marche", "Yoga"]},
    {"label": "Occasionnel", "proba": 0.40, "min": 10, "max": 30, "sports": ["Course à pied", "Randonnée", "Vélo"]},
    {"label": "Régulier", "proba": 0.35, "min": 30, "max": 70, "sports": ["Vélo", "Course à pied", "Fitness"]},
    {"label": "Compétiteur", "proba": 0.10, "min": 60, "max": 150, "sports": ["Triathlon", "Vélo", "Natation"]}
]

SAISONNALITE = {
    1: ["Course à pied", "Fitness", "Yoga", "Marche"],
    2: ["Course à pied", "Fitness", "Yoga", "Marche"],
    3: ["Course à pied", "Vélo", "Randonnée", "Escalade", "Marche"],
    4: ["Vélo", "Randonnée", "Course à pied", "Escalade"],
    5: ["Vélo", "Randonnée", "Natation", "Course à pied"],
    6: ["Vélo", "Randonnée", "Natation", "Escalade"],
    7: ["Vélo", "Randonnée", "Natation", "Surf"],
    8: ["Vélo", "Randonnée", "Natation", "Surf"],
    9: ["Course à pied", "Randonnée", "Vélo", "Fitness"],
    10: ["Course à pied", "Randonnée", "Vélo"],
    11: ["Course à pied", "Fitness", "Yoga"],
    12: ["Course à pied", "Fitness", "Yoga", "Marche"]
}

JOURS_SEMAINE = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
PROBA_JOURS = [0.10, 0.12, 0.12, 0.13, 0.13, 0.20, 0.20]

# ==========================================================================================
# 3. Génération avancée d’une activité sportive réaliste
# ==========================================================================================

def generer_commentaire_avance(prenom, type_activite, mois, special=False):
    if special:
        return f"🎯 {prenom} a participé à une course collective ! Un vrai défi d'équipe."
    if type_activite in ["Vélo", "Course à pied", "Randonnée"]:
        if mois in [5, 6, 7, 8]:
            return f"{prenom} s'est dépassé(e) sous le soleil {random.choice(EMOJIS_SPORTIFS)}"
    if type_activite in ["Fitness", "Yoga"]:
        if mois in [1, 2]:
            return f"{prenom} garde la forme en intérieur {random.choice(EMOJIS_SPORTIFS)}"
    texte = random.choice(COMMENTAIRES_REALISTES)
    if random.randint(0, 2): texte += f" ({random.choice(LIEUX_POPULAIRES)})"
    if random.randint(0, 1): texte += f" {random.choice(EMOJIS_SPORTIFS)}"
    return texte

def date_realiste(date_base, mois, jours_gen):
    annee = date_base.year if mois <= date_base.month else date_base.year - 1
    while True:
        jour_semaine = random.choices(JOURS_SEMAINE, weights=PROBA_JOURS)[0]
        jour = random.randint(1, 28)
        try:
            dt = datetime(annee, mois, jour, random.randint(6, 20), random.randint(0, 59))
        except ValueError:
            continue
        if dt <= datetime.now() and dt not in jours_gen:
            return dt

def generer_activites_salarie(salarie, date_base):
    profil = random.choices(PROFILS, weights=[p['proba'] for p in PROFILS])[0]
    nb_activites = random.randint(profil["min"], profil["max"])
    jours_gen = set()
    activites = []

    # Détermination de la liste des mois simulés (inchangé)
    mois_list = sorted({(date_base.month + i - 1) % 12 + 1 for i in range(SIMULATION_MONTHS)})

    # --- [Ajout] Injection d’au moins 1 activité aujourd’hui avec 50% de chance par salarié ---
    if random.random() < 0.5:
        dt = datetime.now()
        type_activite = random.choice(profil["sports"])
        distance = round(random.uniform(1.5, 6.0), 2)
        temps_sec = random.randint(1200, 5400)
        commentaire = f"{salarie['prenom']} a pratiqué {type_activite} aujourd'hui — notif ntfy !"
        activites.append({
            "uid": str(uuid.uuid4()),
            "id_salarie": salarie["id_salarie"],
            "nom": salarie["nom"],
            "prenom": salarie["prenom"],
            "date": dt.isoformat(),
            "jour": dt.date().isoformat(),
            "date_debut": dt.isoformat(),
            "type_activite": type_activite,
            "distance_km": distance,
            "temps_sec": temps_sec,
            "commentaire": commentaire,
            "profil": profil["label"]
        })
        jours_gen.add(dt)

    # --- Génération classique aléatoire ---
    for _ in range(nb_activites):
        mois = random.choice(mois_list)
        dt = date_realiste(date_base, mois, jours_gen)
        jours_gen.add(dt)
        favoris = list(set(profil["sports"]) & set(SAISONNALITE.get(mois, profil["sports"])))
        if favoris and random.random() < 0.7:
            type_activite = random.choice(favoris)
        else:
            type_activite = random.choice(SAISONNALITE.get(mois, profil["sports"]))
        if type_activite in ["Course à pied", "Marche", "Randonnée"]:
            base = random.uniform(2, 6) if profil["label"] == "Sédentaire" else random.uniform(5, 15)
            distance = int(base * random.uniform(0.8, 1.2) * 1000)
        elif type_activite in ["Vélo", "Roller", "Trottinette"]:
            base = random.uniform(5, 20) if profil["label"] == "Sédentaire" else random.uniform(15, 50)
            distance = int(base * random.uniform(0.7, 1.3) * 1000)
        elif type_activite in ["Natation"]:
            base = random.uniform(0.5, 1.2) if profil["label"] == "Sédentaire" else random.uniform(1, 3)
            distance = int(base * random.uniform(0.8, 1.2) * 1000)
        else:
            base = random.uniform(1, 4) if profil["label"] == "Sédentaire" else random.uniform(2, 10)
            distance = int(base * random.uniform(0.7, 1.2) * 1000)
        vitesse = random.uniform(2.0, 4.5) if type_activite in ["Course à pied", "Marche", "Randonnée"] else random.uniform(5.0, 12.0)
        temps = max(int(distance / vitesse), 180)
        special_event = random.random() < 0.05
        commentaire = generer_commentaire_avance(salarie["prenom"], type_activite, mois, special=special_event)
        uid = str(uuid.uuid4())
        activites.append({
            "uid": uid,
            "id_salarie": salarie["id_salarie"],
            "nom": salarie["nom"],
            "prenom": salarie["prenom"],
            "date": dt.isoformat(),
            "jour": dt.date().isoformat(),
            "date_debut": dt.isoformat(),
            "type_activite": type_activite,
            "distance_km": round(distance / 1000, 2),
            "temps_sec": temps,
            "commentaire": commentaire,
            "profil": profil["label"]
        })
    return activites

# ==========================================================================================
# 4. Pipeline principal de simulation, insertion et publication
# ==========================================================================================

def pipeline_simulation():
    """
    Pipeline avancé pour simuler, historiser et publier des activités sportives réalistes.
    Étapes :
      1. Extraction des salariés sportifs depuis PostgreSQL (table employes)
      2. Attribution d'un profil à chaque salarié, génération d'activités (saisonnalité, week-end…)
      3. Insertion en base PostgreSQL (CDC Debezium)
      4. Export Excel vers MinIO (Power BI, contrôle)
      5. Publication dans Kafka (JSON Debezium enveloppé), logs allégés
    """
    logger.info("🚀 Démarrage de la simulation réaliste d’activités sportives (PostgreSQL)")

    # 1. Extraction RH filtrée
    engine = create_engine(DB_CONN_STRING)
    df_salaries = pd.read_sql(
        "SELECT * FROM sportdata.employes WHERE deplacement_sportif = True", engine
    )
    if df_salaries.empty:
        logger.warning("⚠️ Aucun salarié sportif trouvé dans la base.")
        return
    logger.info(f"✅ {len(df_salaries)} salariés sportifs identifiés pour simulation (profils)")

    # 2. Génération avancée des activités
    activites = []
    date_base = datetime.now() - timedelta(days=SIMULATION_MONTHS * 30)
    for _, salarie in df_salaries.iterrows():
        activites.extend(generer_activites_salarie(salarie, date_base))

    df_activites = pd.DataFrame(activites)
    logger.info(f"✅ {len(df_activites)} activités simulées (profils, saisonnalité, événements spéciaux inclus)")

    # 3. Insertion en base PostgreSQL (append)
    df_activites.to_sql(
        "activites_sportives", engine, index=False, if_exists="append", schema="sportdata"
    )
    logger.success("📦 Données insérées dans PostgreSQL (sportdata.activites_sportives)")

    # 4. Export Excel vers MinIO pour BI/contrôle
    helper = MinIOHelper()
    helper.upload_excel(df_activites, MINIO_EXPORT_KEY, "Activités simulées (profils + saison)")

    # 5. Publication Kafka (format Debezium enveloppé, logs allégés)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    n_total = len(df_activites)
    logger.info(f"Début publication Kafka : {n_total} activités à envoyer")

    for i, (_, row) in enumerate(df_activites.iterrows()):
        message = {
            "payload": {
                "op": "c",  # 'c' = création (insert)
                "after": row.to_dict(),
                "ts_ms": int(datetime.utcnow().timestamp() * 1000)
            }
        }
        producer.send(KAFKA_TOPIC, value=message)

        # Log les 3 premières, toutes les 500, et la dernière activité seulement
        if i < 3 or i % 500 == 0 or i == n_total - 1:
            logger.info(f"📤 Kafka envoyé : {row['uid']} (activité {i+1}/{n_total})")

    producer.flush()
    logger.success(f"📤 {n_total} messages Kafka Debezium envoyés sur le topic {KAFKA_TOPIC}")
    logger.success("🎯 Simulation réaliste terminée avec succès.")

# ==========================================================================================
# 5. Point d’entrée
# ==========================================================================================

if __name__ == "__main__":
    try:
        pipeline_simulation()
    except Exception as e:
        logger.error(f"❌ Erreur lors de la simulation : {e}")
