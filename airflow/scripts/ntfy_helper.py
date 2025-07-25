# ==========================================================================================
# Script       : ntfy_helper.py
# Objectif     : Centraliser toutes les fonctions de notification via NTFY
# Utilisé par  : Scripts Python (simulation) + Jobs Spark (streaming + qualité)
# Auteur       : Xavier Rousseau | Modifié par ChatGPT, juillet 2025
# ==========================================================================================

import os
import requests
from random import choice
from dotenv import load_dotenv
from loguru import logger
from datetime import datetime
import time 

# ==========================================================================================
# 1. Chargement des variables d’environnement (.env)
# ==========================================================================================

load_dotenv(dotenv_path=".env", override=True)

NTFY_URL = os.getenv("NTFY_URL", "http://localhost:8080")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata_activites")

# ==========================================================================================
# 2. Données statiques pour enrichir les messages
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
    "au canal du Midi", "vers Saint-Guilhem", "sur les berges de la Garonne",
    "au parc de la Penfeld", "le long du Lez", "au parc du Thabor",
    "près du pont de Rohan"
]

EMOJIS_SPORTIFS = ["💪", "🔥", "🌟", "🏃‍♂️", "🚴‍♀️", "🏞️", "😅", "🙌", "⛰️", "🎯"]

# ==========================================================================================
# 3. Fonction principale : envoyer un message enrichi (activité sportive)
# ==========================================================================================

def envoyer_message_ntfy(prenom, sport, km, minutes):
    """
    Envoie une notification enrichie pour une activité sportive via ntfy.

    Args:
        prenom (str): Prénom du salarié
        sport (str): Type d’activité (ex. Vélo)
        km (float): Distance parcourue (en km)
        minutes (int): Temps passé (en minutes)
    """
    lieu = choice(LIEUX_POPULAIRES)
    emoji = choice(EMOJIS_SPORTIFS)
    commentaires = [
        f"{emoji} Bravo {prenom} ! Tu viens de faire {km:.1f} km de {sport.lower()} en {minutes} min {lieu}",
        f"{emoji} {prenom} a bien transpiré : {km:.1f} km en {minutes} minutes {lieu}",
        f"{emoji} {prenom} s’est donné à fond en {sport.lower()} {lieu} ({minutes} min)",
        f"{emoji} Belle performance de {prenom} : {km:.1f} km parcourus en {minutes} minutes !",
        f"{emoji} {prenom} garde la forme avec une session de {sport.lower()} de {km:.1f} km {lieu} 🏞️",
        f"{emoji} {prenom} vient de boucler {km:.1f} km en {sport.lower()} {lieu}, chapeau 🎩",
        f"{emoji} {prenom} enchaîne les défis : {minutes} min de {sport.lower()} pour {km:.1f} km !",
        f"{emoji} {prenom} ne lâche rien : {km:.1f} km de {sport.lower()} sous le soleil ☀️",
        f"{emoji} {prenom} a bien mérité une pause après {minutes} min de {sport.lower()} {lieu}",
        f"{emoji} Excellente session de {sport.lower()} pour {prenom} : {km:.1f} km parcourus 💥"
    ]
    message = choice(commentaires)
    try:
        requests.post(f"{NTFY_URL}/{NTFY_TOPIC}", data=message.encode("utf-8"))
        logger.info(f"🔔 Notification envoyée : {message}")
    except Exception as e:
        logger.error(f"❌ Erreur NTFY : {e}")

# ==========================================================================================
# 4. Fonction dédiée au contrôle qualité (succès / échec)
# ==========================================================================================

def notifier_qualite(nb_valides: int, nb_erreurs: int):
    """
    Envoie une notification simple sur l'état du contrôle qualité (job Spark).

    Args:
        nb_valides (int): Nombre de lignes valides
        nb_erreurs (int): Nombre de lignes en erreur
    """
    try:
        if nb_erreurs > 0:
            message = f"❌ Contrôle qualité : {nb_erreurs} erreur(s) détectée(s)."
        else:
            message = f"✅ Contrôle qualité OK : {nb_valides} lignes valides."

        requests.post(f"{NTFY_URL}/{NTFY_TOPIC}", data=message.encode("utf-8"))
        logger.info(f"🔔 Notification ntfy envoyée : {message}")
    except Exception as e:
        logger.warning(f"⚠️ Erreur lors de l’envoi ntfy : {e}")

# ==========================================================================================
# 5. Notification détaillée de fin de pipeline (primes + bien-être)
# ==========================================================================================

def envoyer_resume_pipeline(df_primes, df_bien_etre, url_rapport, topic="avantages_sportifs"):
    """
    Envoie un résumé structuré des avantages RH via ntfy.
    """
    try:
        message_resume = (
            f"🎯 Pipeline terminé avec succès !\n"
            f"💶 {len(df_primes)} primes sportives attribuées\n"
            f"🧘 {len(df_bien_etre)} journées bien-être accordées\n"
            f"📁 Données dans MinIO + PostgreSQL\n"
            f"🔍 Rapport GE : {url_rapport}"
        )
        requests.post(f"{NTFY_URL.rstrip('/')}/{topic}", data=message_resume.encode("utf-8"))
        logger.success("🔔 Notification ntfy (résumé global) envoyée.")
    except Exception as e:
        logger.warning(f"⚠️ Erreur ntfy (résumé global) : {e}")
    time.sleep(1)

    try:
        df_resume = df_primes[["id_salarie", "nom", "prenom", "prime_montant_eur", "nb_activites"]].merge(
            df_bien_etre[["id_salarie", "nb_journees_bien_etre"]],
            on="id_salarie", how="outer"
        ).fillna({"prime_montant_eur": 0, "nb_journees_bien_etre": 0, "nb_activites": 0})

        lignes = []
        for _, row in df_resume.iterrows():
            if row["prime_montant_eur"] > 0 or row["nb_journees_bien_etre"] > 0:
                avantages = []
                if row["prime_montant_eur"] > 0:
                    avantages.append(f"💶 Prime ({int(row['prime_montant_eur'])} €) — {int(row['nb_activites'])} activité(s)")
                if row["nb_journees_bien_etre"] > 0:
                    avantages.append(f"🧘 {int(row['nb_journees_bien_etre'])} JBE")
                lignes.append(f"- {row['prenom']} {row['nom']} : " + " | ".join(avantages))

        for i in range(0, len(lignes), 10):
            bloc = "\n".join(lignes[i:i+10])
            message = f"📌 Bénéficiaires ({i+1} à {min(i+10, len(lignes))}) :\n{bloc}"
            requests.post(f"{NTFY_URL.rstrip('/')}/{topic}", data=message.encode("utf-8"))
            time.sleep(1)
        logger.success("🔔 Notifications ntfy (détail bénéficiaires) envoyées.")
    except Exception as e:
        logger.warning(f"⚠️ Erreur ntfy (bénéficiaires) : {e}")

# ==========================================================================================
# 6. Notification d’erreur en cas de crash pipeline
# ==========================================================================================

def envoyer_message_erreur(topic, message):
    """
    Envoie un message d’erreur formaté à ntfy.
    """
    try:
        requests.post(f"{NTFY_URL.rstrip('/')}/{topic}", data=message.encode("utf-8"))
        logger.warning(f"⚠️ Notification ntfy d’échec envoyée sur topic {topic}")
    except Exception as e:
        logger.error(f"❌ Impossible d’envoyer la notification d’erreur : {e}")
