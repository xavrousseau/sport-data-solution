# ==========================================================================================
# Script      : utils_google.py
# Objectif    : Fonctions utilitaires pour calcul de distance via API Google Maps + éligibilité
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
ADRESSE_TRAVAIL = "1362 Avenue des Platanes, 34970 Lattes, France"

MODES_TRANSPORT = {
    "marche": 15,
    "course à pied": 15,
    "vélo": 25,
    "trottinette": 25,
    "roller": 25,
    "skateboard": 25
}

# ------------------------------------------------------------------------------------------
# 1. Calculer distance entre deux adresses (km)
# ------------------------------------------------------------------------------------------
def calculer_distance_km(adresse_depart):
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
        return distance_m / 1000
    except Exception as e:
        logger.warning(f"Erreur API Google Maps : {e}")
        return None

# ------------------------------------------------------------------------------------------
# 2. Vérifier éligibilité en fonction du mode de transport et de la distance
# ------------------------------------------------------------------------------------------
def verifier_eligibilite(adresse, mode):
    if not adresse or not mode:
        return False
    mode = mode.lower().strip()
    if mode not in MODES_TRANSPORT:
        logger.info(f"Mode non éligible : {mode}")
        return False
    distance = calculer_distance_km(adresse)
    if distance is None:
        return False
    if distance > MODES_TRANSPORT[mode]:
        logger.info(f"Distance {distance:.1f} km > limite pour {mode}")
        return False
    return True
