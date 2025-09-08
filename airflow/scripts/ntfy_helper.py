# ==========================================================================================
# ntfy_helper.py — Notifications NTFY (robustes) + ressources "humaines"
# Compat ascendante avec etape_03 / etape_08 / etape_09
#
# Contenu :
#   1) Chargement ENV + logging hygiénique (pas de secrets)
#   2) Session HTTP avec retries (429/5xx), timeouts
#   3) Constantes "storytelling" (LIEUX_POPULAIRES, ACTIVITES, COMMENTAIRES_REALISTES, EMOJIS_SPORTIFS)
#   4) Sélection de lieux COHÉRENTS par sport (catalogue typé + tagging auto)
#   5) Aides de formatage (emoji, commentaire_coherent)
#   6) Fonctions publiques d'envoi NTFY :
#        - envoyer_message_texte(message, topic=None)
#        - envoyer_resume_pipeline(df_primes, df_bien_etre, url_rapport, topic="avantages_sportifs")
#        - envoyer_message_erreur(topic, message)
# ==========================================================================================

from __future__ import annotations

import os
import time
import hashlib
import unicodedata
import random
from typing import Iterable, Dict, Set

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dotenv import load_dotenv
from loguru import logger

# ------------------------------------------------------------------------------------------
# 1) ENV & LOGGING — charger .env (Airflow puis local) et éviter les fuites de secrets
# ------------------------------------------------------------------------------------------

try:
    load_dotenv("/opt/airflow/.env", override=True)
except Exception:
    # Exécutions locales : ce chemin n'existe pas forcément → c'est OK.
    pass
load_dotenv(".env", override=True)

NTFY_URL: str = os.getenv("NTFY_URL", "http://localhost:8080").rstrip("/")
NTFY_TOPIC: str = os.getenv("NTFY_TOPIC", "sportdata_activites")
NTFY_USE_EMOJIS: bool = os.getenv("NTFY_USE_EMOJIS", "true").lower() in {"1", "true", "yes", "on"}

# Filtrage basique des logs pour ne pas faire fuiter de secrets accidentellement.
_SENSITIVE = ("PASSWORD", "SECRET", "TOKEN", "KEY", "ACCESS")

def _no_secret(record) -> bool:
    return not any(tok in str(record["message"]).upper() for tok in _SENSITIVE)

logger.remove()
logger.add(lambda m: print(m, end=""), level="INFO", filter=_no_secret)

# ------------------------------------------------------------------------------------------
# 2) HTTP robuste — session requests avec retries + timeouts
# ------------------------------------------------------------------------------------------

# Timeouts (connect, read) raisonnables pour un service interne/close-by.
_DEF_TIMEOUT = (3, 10)

def _session() -> requests.Session:
    """
    Construit une session HTTP résiliente :
      - retries automatiques sur 429/5xx (avec backoff),
      - respect du header Retry-After,
      - montée sur http/https.
    """
    s = requests.Session()
    r = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    a = HTTPAdapter(max_retries=r)
    s.mount("http://", a)
    s.mount("https://", a)
    return s

_SES = _session()

def _post_ntfy(url: str, data: bytes) -> None:
    """
    POST simple vers NTFY avec gestion d'erreurs.
    On garde la signature minimale (texte brut), sans headers exotiques.
    """
    try:
        resp = _SES.post(url, data=data, timeout=_DEF_TIMEOUT)
        if resp.status_code >= 400:
            logger.error(f"[NTFY] HTTP {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        logger.error(f"[NTFY] POST failed: {e!r}")

# ------------------------------------------------------------------------------------------
# 3) Constantes "humaines" — storytelling utilisé par etape_03/08
#     (Conservées à l'identique pour compatibilité)
# ------------------------------------------------------------------------------------------

LIEUX_POPULAIRES = [
    "au parc du Thabor","au bord de la mer","dans le bois de Boulogne","sur les quais",
    "au bord du canal Saint-Martin","autour du lac de Ty Colt","au parc Borély","au parc de Procé",
    "dans la vallée de Chevreuse","dans les Monts d’Arrée","à la Plaine des Sports",
    "autour de l’étang de Cora","près du stade","au parc de la Tête d’Or",
    "sur la Corniche","le long de la Loire","au parc de Bercy","au parc des Buttes-Chaumont",
    "au bois de Vincennes","le long du canal de l’Ourcq","au parc de la Deûle",
    "au parc de la Villette","dans la forêt de Meudon","à la plage de la Baule",
    "au Mont Saint-Michel","à l’île de Ré","le long de l’Erdre","au parc de Miribel-Jonage",
    "au parc Montsouris","dans la forêt de Rambouillet","autour du lac d’Annecy","au parc naturel du Vercors",
    "dans les Calanques","au parc du Confluent","dans le massif de l’Aigoual",
    "sur les bords du Lez","dans la forêt du Rouvray","au parc de la Citadelle",
    "au parc de la Tête d'Or","dans la forêt de Fontainebleau","au canal du Midi",
    "vers Saint-Guilhem","sur les berges de la Garonne","au parc de la Penfeld",
    "le long du Lez","au parc du Thabor","près du pont de Rohan"
]

ACTIVITES = [
    "Course à pied","Marche","Vélo","Trottinette","Roller","Skateboard","Randonnée","Natation","Escalade",
    "Fitness","Musculation","Boxe","Tennis","Basketball","Football","Badminton","Yoga","Pilates","Danse",
    "Karaté","Judo","Handball","Rugby","Ping-pong","Padel","Squash","CrossFit","Ski alpin","Ski de fond",
    "Snowboard","Aviron","Canoë-kayak","Surf","Kitesurf","Plongée","Stand-up paddle","Marche nordique",
    "Parkour","Gymnastique","Trampoline"
]

COMMENTAIRES_REALISTES = [
    "Reprise en douceur après une pause.","Très bonne séance aujourd'hui !","C'était dur mais je ne regrette pas.",
    "J'ai battu mon record perso !","Belle sortie dans la nature.","Bonne ambiance, bon rythme.",
    "Temps idéal pour ce sport.","Encore un effort avant le week-end !","Avec quelques collègues du bureau.",
    "Motivé(e) comme jamais aujourd’hui !","Petite séance rapide entre midi et deux.","Pas facile, mais ça fait du bien.",
    "Objectif atteint pour aujourd’hui.","J’ai testé un nouveau parcours.","De belles sensations malgré le vent.",
    "Un peu fatigué(e), mais satisfait(e).","Toujours un plaisir de bouger.","Je progresse petit à petit.",
    "Une sortie plus longue que prévu.","Ça m’a vidé la tête !","Retour progressif après blessure.",
]

EMOJIS_SPORTIFS = ["💪","🔥","🌟","🏃‍♂️","🚴‍♀️","🏞️","😅","🙌","⛰️","🎯"]

def _emoji() -> str:
    """Retourne un emoji sportif si activé, sinon chaîne vide (pour commentaires)."""
    return random.choice(EMOJIS_SPORTIFS) if (NTFY_USE_EMOJIS and EMOJIS_SPORTIFS) else ""

# ------------------------------------------------------------------------------------------
# 4) Cohérence sport ↔ lieu
#    Idée : on "tague" un catalogue de lieux (mer/lac/forêt/urbain/...) et
#           on sélectionne un lieu compatible avec le sport demandé.
#           -> zéro "surf au parc du Thabor".
# ------------------------------------------------------------------------------------------

def _slug(s: str) -> str:
    """Normalise une chaîne en minuscules ASCII (utile pour matcher des mots-clés)."""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.lower()

# --- 4.1) Mini-catalogue typé (quelques lieux "emblématiques") --------------
LIEUX_CATALOGUE = [
    {"nom": "sur la côte atlantique",      "tags": {"mer","vent"}},
    {"nom": "dans les Calanques",          "tags": {"mer","falaise"}},
    {"nom": "au lac d’Annecy",             "tags": {"lac","montagne"}},
    {"nom": "au lac du Bourget",           "tags": {"lac"}},
    {"nom": "le long de la Loire",         "tags": {"rivière","plaine"}},
    {"nom": "au canal du Midi",            "tags": {"canal","plat"}},
    {"nom": "dans la vallée de Chevreuse", "tags": {"forêt","vallonné"}},
    {"nom": "au bois de Vincennes",        "tags": {"forêt","urbain"}},
    {"nom": "au parc de la Tête d’Or",     "tags": {"parc","urbain"}},
    {"nom": "dans le Vercors",             "tags": {"montagne"}},
    {"nom": "sur les quais",               "tags": {"urbain","plat"}},
    {"nom": "dans la forêt de Rambouillet","tags": {"forêt"}},
]

# --- 4.2) Tagging automatique de tes LIEUX_POPULAIRES existants -------------
#      On détecte des mots-clés ("lac", "canal", "plage", ...) pour enrichir en tags.
_TAGS_PAR_MOTCLE: Dict[str, Set[str]] = {
    "mer": {"mer"}, "plage": {"mer"},
    "calanque": {"mer","falaise"},
    "lac": {"lac"}, "étang": {"lac"}, "etang": {"lac"},
    "canal": {"canal","plat"}, "quais": {"canal","urbain","plat"},
    "bois": {"forêt"}, "forêt": {"forêt"}, "foret": {"forêt"},
    "parc": {"parc","urbain"}, "stade": {"urbain"},
    "mont ": {"montagne"}, "massif": {"montagne"}, "vercors": {"montagne"},
    "vallée": {"vallonné"}, "vallee": {"vallonné"},
    "corniche": {"mer","falaise"},
    "loire": {"rivière","plaine"},
    "garonne": {"rivière","plaine"},
    "lez": {"rivière","plaine"},
    "erdre": {"rivière","plaine"},
    "ourcq": {"canal","plat"},
    "rambouillet": {"forêt"},
    "fontainebleau": {"forêt"},
    "miribel": {"parc","lac"},
    "tête d’or": {"parc","urbain"}, "tete d'or": {"parc","urbain"},
    "vincennes": {"forêt","urbain"},
}

def _tags_depuis_phrase_lieu(phrase: str) -> Set[str]:
    """Infère un ensemble de tags à partir du libellé d'un lieu."""
    tags: Set[str] = set()
    sl = _slug(phrase)
    for motcle, t in _TAGS_PAR_MOTCLE.items():
        if motcle in sl:
            tags |= t
    # Gardes-fous : si rien trouvé, on essaie des interprétations neutres plausibles
    if not tags:
        if "parc" in sl:
            tags |= {"parc", "urbain"}
        if "quai" in sl:
            tags |= {"urbain", "plat"}
    return tags

_LIEUX_DERIVES = [{"nom": nom, "tags": _tags_depuis_phrase_lieu(nom)} for nom in LIEUX_POPULAIRES]

# On fusionne le mini-catalogue typé + tes lieux tagués automatiquement
_LIEUX_UNIFIES = [*LIEUX_CATALOGUE, *_LIEUX_DERIVES]

# --- 4.3) Terrains admissibles par sport ------------------------------------
SPORT_TERRAINS: Dict[str, Set[str]] = {
    "Surf": {"mer"},
    "Kitesurf": {"mer"},
    "Natation": {"mer","lac","rivière","canal","piscine"},
    "Aviron": {"lac","rivière","canal"},
    "Canoë-kayak": {"lac","rivière","canal"},
    "Vélo": {"urbain","forêt","plat","vallonné","montagne","campagne","canal"},
    "Course à pied": {"urbain","parc","forêt","montagne","plaine","canal"},
    "Randonnée": {"forêt","montagne","plaine"},
    "Escalade": {"falaise","montagne"},
    "Roller": {"urbain","plat"},
    "Trottinette": {"urbain","plat"},
    "Marche": {"urbain","parc","forêt"},
    "Fitness": {"urbain","intérieur"},
    "Yoga": {"urbain","intérieur","parc"},
}

# --- 4.4) Bonus saisonnier léger (mois → bonus) -----------------------------
#      Objectif : privilégier "un peu plus" natation/surf l'été, rando au printemps/été…
SAISON_BONUS: Dict[str, Dict[int, float]] = {
    "Natation": {6:0.4,7:0.6,8:0.6,5:0.2,9:0.2},
    "Surf":     {6:0.2,7:0.3,8:0.3},
    "Vélo":     {4:0.2,5:0.3,6:0.3,7:0.3,8:0.3,9:0.2},
    "Randonnée":{4:0.2,5:0.3,6:0.3,7:0.3,8:0.3,9:0.3,10:0.2},
}

def _hashpick(items: Iterable[dict], key: str) -> dict | None:
    """
    Sélection déterministe dans une liste (stabilité visuelle batch→batch).
    Utilise SHA-256(key) % len(items).
    """
    items = list(items)
    if not items:
        return None
    h = int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16)
    return items[h % len(items)]

def choisir_lieu_pour_sport(sport: str, mois: int | None = None, seed_key: str = "") -> str:
    """
    Retourne un lieu COHÉRENT avec le 'sport' passé :
      1) filtre par tags compatibles (SPORT_TERRAINS),
      2) re-shuffle déterministe si le sport est "de saison" pour 'mois',
      3) choix déterministe via 'seed_key' (ex: "prenom|sport|jour").
    Si aucun candidat spécifique : fallback vers des lieux neutres (parc/urbain).
    """
    sport_c = sport.strip().capitalize()
    terrains = SPORT_TERRAINS.get(sport_c, {"urbain", "parc"})

    # Filtre par intersection de tags
    cands = [L for L in _LIEUX_UNIFIES if (L["tags"] & terrains)]

    # Fallback neutre si rien trouvé
    if not cands:
        cands = [L for L in _LIEUX_UNIFIES if (L["tags"] & {"parc", "urbain"})] or _LIEUX_UNIFIES

    # Légère influence saisonnière : on re-shuffle de façon DÉTERMINISTE (pas de hasard instable)
    if mois is not None:
        bonus = SAISON_BONUS.get(sport_c, {}).get(mois, 0.0)
        if bonus > 0 and len(cands) > 1:
            rnd = random.Random(int(hashlib.md5(f"{sport_c}-{mois}".encode()).hexdigest(), 16))
            rnd.shuffle(cands)

    key = seed_key or f"{sport_c}|{mois or 0}"
    picked = _hashpick(cands, key)
    return picked["nom"] if picked else ""

def commentaire_coherent(prenom: str, sport: str, mois: int, extra_emoji: bool = True) -> str:
    """
    Produit une phrase courte et "propre" :
      - sport → lieu compatible (jamais "surf au Thabor"),
      - "en vélo" corrigé en "à vélo",
      - emoji optionnel (géré par NTFY_USE_EMOJIS).
    """
    lieu = choisir_lieu_pour_sport(sport, mois, seed_key=f"{prenom}|{sport}|{mois}")
    emj = f" {_emoji()}" if extra_emoji else ""
    sport_lc = sport.strip().lower()
    sport_txt = "à vélo" if sport_lc == "vélo" else f"en {sport_lc}"
    # ex: "Jules s’est donné(e) à fond à vélo dans la vallée de Chevreuse 💪"
    return f"{prenom} s’est donné(e) à fond {sport_txt} {lieu}{emj}".strip()

# ------------------------------------------------------------------------------------------
# 5) API publique — fonctions d'envoi NTFY (compatibles avec tes jobs)
# ------------------------------------------------------------------------------------------

def envoyer_message_texte(message: str, topic: str | None = None) -> None:
    """
    Envoie un message TEXTE brut sur NTFY.
    Utilisé par : etape_08 (notifications d'ingestion), et d'autres scripts éventuels.
    """
    try:
        _post_ntfy(f"{NTFY_URL}/{(topic or NTFY_TOPIC)}", message.encode("utf-8"))
        logger.info("🔔 Notification envoyée.")
    except Exception as e:
        logger.error(f"Impossible d’envoyer la notification : {e}")

def envoyer_resume_pipeline(df_primes, df_bien_etre, url_rapport, topic: str = "avantages_sportifs") -> None:
    """
    Résumé concis d'un pipeline (ex: étape 09) :
      - nb de primes et nb de JBE,
      - pointeur/lien de rapport,
      - pas de détail ligne-à-ligne (les jobs gèrent déjà l'échantillonnage).
    """
    try:
        msg = (
            "🎯 Pipeline terminé !\n"
            f"💶 {len(df_primes)} primes sportives\n"
            f"🧘 {len(df_bien_etre)} journées bien-être\n"
            f"🔎 Rapport: {url_rapport}"
        )
        _post_ntfy(f"{NTFY_URL}/{topic}", msg.encode("utf-8"))
        logger.info("🔔 Résumé pipeline envoyé.")
    except Exception as e:
        logger.warning(f"Résumé pipeline KO: {e}")

def envoyer_message_erreur(topic: str | None, message: str) -> None:
    """
    Envoie un message d'erreur/alerte sur NTFY.
    """
    try:
        _post_ntfy(f"{NTFY_URL}/{(topic or NTFY_TOPIC)}", message.encode("utf-8"))
        logger.warning("⚠️ Notification d’erreur envoyée.")
    except Exception as e:
        logger.error(f"Impossible d’envoyer l’erreur : {e}")

# ------------------------------------------------------------------------------------------
# 6) Export "propre" — ce que ce module expose explicitement
# ------------------------------------------------------------------------------------------

__all__ = [
    # Constantes storytelling
    "LIEUX_POPULAIRES", "ACTIVITES", "COMMENTAIRES_REALISTES", "EMOJIS_SPORTIFS",
    # Sélecteurs cohérents
    "choisir_lieu_pour_sport", "commentaire_coherent",
    # Envoi NTFY
    "envoyer_message_texte", "envoyer_resume_pipeline", "envoyer_message_erreur",
    # Réglages
    "NTFY_URL", "NTFY_TOPIC", "NTFY_USE_EMOJIS",
]
