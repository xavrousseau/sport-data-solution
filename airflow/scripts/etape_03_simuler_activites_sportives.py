# ==========================================================================================
# Script      : etape_03_simuler_activites_sportives.py
# Objectif    : Simuler des activités "réalistes" sur X mois glissants (pilotés par .env),
#               puis persister (PostgreSQL), exporter (MinIO Excel) et publier (Kafka).
#
# Améliorations clés :
#   ✅ AUCUNE timezone : toutes les dates sont émises en "YYYY-MM-DD HH:MM:SS" (naïf)
#   ✅ Pas d'activités "du futur" (bornage au temps présent, côté tirage)
#   ✅ Cohérence sport ↔ lieu via ntfy_helper (jamais "surf au Thabor")
#   ✅ Distance ↔ vitesse ↔ durée calculées ensemble (cohérence physique)
#   ✅ Saisonnalité automatique couvrant TOUS les sports (pondération saison + profil)
#   ✅ Déterminisme optionnel via SEED (réexécutions stables)
#   ✅ INSERT Postgres "safe" (chunksize adapté pour éviter la limite 65k paramètres)
#
# Entrées .env (extrait) :
#   - SIMULATION_MONTHS      : int (1 = "mois en cours")
#   - SIMULATION_SEED        : int|None (fixe le RNG Python)
#   - SIMULATION_DRY_RUN     : bool ("true"/"false") — pas d'écritures si true
#   - SIMU_SEASON_WEIGHT     : float pondération saison (défaut 0.7 → favorise les sports de saison)
#   - SIMU_PROFILE_BONUS     : float bonus affinité profil (défaut 1.8 → booste les sports préférés)
#
# Dépendances locales :
#   - minio_helper.MinIOHelper (export Excel)
#   - ntfy_helper :
#        * ACTIVITES                → liste canonique des sports
#        * commentaire_coherent     → fabrique de commentaire court et cohérent
#        * choisir_lieu_pour_sport  → sélection déterministe d’un lieu compatible
# ==========================================================================================

from __future__ import annotations

import os
import sys
import json
import uuid
import random
import signal
import hashlib
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text, bindparam
from dotenv import load_dotenv
from loguru import logger
from kafka import KafkaProducer

# ---- Helpers locaux ---------------------------------------------------------
from minio_helper import MinIOHelper
from ntfy_helper import (
    ACTIVITES,                 # liste canonique de sports (sert à construire la saisonnalité)
    commentaire_coherent,      # commentaire court et cohérent (déjà corrigé côté helper)
    choisir_lieu_pour_sport,   # sélection déterministe d’un lieu valide pour le sport
)

# ==========================================================================================
# 0) ENV & logging hygiénique
# ==========================================================================================

# Charge d'abord l'ENV Airflow (si présent), puis l'ENV local
try:
    load_dotenv("/opt/airflow/.env", override=True)
except Exception:
    pass
load_dotenv(".env", override=True)

# Ne pas fuiter de secrets dans les logs
_SENSITIVE = ("PASSWORD", "SECRET", "TOKEN", "KEY", "ACCESS")
logger.remove()
logger.add(
    sys.stdout,
    level="INFO",
    filter=lambda r: not any(tok in r["message"].upper() for tok in _SENSITIVE)
)

# ==========================================================================================
# 1) Paramètres d'exécution (pilotés par .env)
# ==========================================================================================

SEED = os.getenv("SIMULATION_SEED")                                   # Optionnel : seed RNG
DRY_RUN = os.getenv("SIMULATION_DRY_RUN", "false").lower() in {"1", "true", "yes"}
SIMULATION_MONTHS = int(os.getenv("SIMULATION_MONTHS", 12))           # 1 = mois en cours

# Pondérations de choix de sport
SIMU_SEASON_WEIGHT = float(os.getenv("SIMU_SEASON_WEIGHT", "0.7"))    # 0..1 recommandé
SIMU_PROFILE_BONUS = float(os.getenv("SIMU_PROFILE_BONUS", "1.8"))    # ≥ 1.0 (multiplicatif)

# Connexion Postgres (⚠️ ne jamais logger la chaîne complète)
POSTGRES_USER = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "")
DB_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Kafka
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata.sportdata.activites_sportives")

# MinIO (export Excel de la simulation)
MINIO_EXPORT_KEY = os.getenv("MINIO_SPORT_KEY", "clean/activites_sportives_simulees.xlsx")

# Validations minimales (lecture autorisée en DRY_RUN)
_missing = [k for k in ["POSTGRES_HOST", "POSTGRES_DB"] if not os.getenv(k)]
if _missing and not DRY_RUN:
    raise EnvironmentError(f"Variables d’environnement manquantes : {_missing}")

# RNG déterministe (si fourni)
if SEED is not None:
    try:
        random.seed(int(SEED))
        logger.info(f"🔁 Seed de simulation fixée à {SEED}")
    except Exception:
        logger.warning("Seed invalide, ignorée (simulation non déterministe).")

# ==========================================================================================
# 2) Utilitaires temps & formats (NAÏFS — PAS DE TIMEZONE)
# ==========================================================================================

def _fmt_naive(dt: datetime) -> str:
    """
    Formate un datetime en chaîne SANS fuseau, sans microsecondes.
    Exemple: '2025-08-21 14:03:02'
    """
    return dt.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

NOW_LOCAL = datetime.now()  # point de vérité pour interdire les dates futures (naïf/local)

# ==========================================================================================
# 3) Modèle de comportements (profils)
# ==========================================================================================

# Profils sportifs (les probas doivent approx. sommer à 1.0)
# NOTE : "Triathlon" (absent d'ACTIVITES) est remplacé par Course/Natation/Vélo
PROFILS = [
    {"label": "Sédentaire",  "proba": 0.15, "min": 5,  "max": 15,  "sports": ["Marche", "Yoga"]},
    {"label": "Occasionnel", "proba": 0.40, "min": 10, "max": 30,  "sports": ["Course à pied", "Randonnée", "Vélo"]},
    {"label": "Régulier",    "proba": 0.35, "min": 30, "max": 70,  "sports": ["Vélo", "Course à pied", "Fitness"]},
    {"label": "Compétiteur", "proba": 0.10, "min": 60, "max": 150, "sports": ["Vélo", "Course à pied", "Natation"]},
]

# (Option : on pourrait biaiser week-end vs semaine ici ; on garde simple & crédible)

# ==========================================================================================
# 4) Saisonnalité (auto-couverte pour TOUS les sports) + sous-ensemble "mesurable"
# ==========================================================================================

# Groupes → mois (évite de dupliquer 12 fois la même logique)
GROUP_MONTHS = {
    "ALL_YEAR":         [1,2,3,4,5,6,7,8,9,10,11,12],
    "INDOOR":           [1,2,3,4,5,6,7,8,9,10,11,12],
    "SPRING_TO_AUTUMN": [3,4,5,6,7,8,9,10],
    "SUMMER_WATER":     [5,6,7,8,9],
    "SUMMER_WIND":      [6,7,8],
    "WINTER_SNOW":      [12,1,2,3],
}

# Affectation de TOUS les sports à un groupe saisonnier
SPORT_GROUP = {
    "Course à pied":"ALL_YEAR", "Marche":"ALL_YEAR", "Vélo":"SPRING_TO_AUTUMN",
    "Trottinette":"SPRING_TO_AUTUMN", "Roller":"SPRING_TO_AUTUMN", "Skateboard":"SPRING_TO_AUTUMN",
    "Randonnée":"SPRING_TO_AUTUMN", "Natation":"SUMMER_WATER", "Escalade":"SPRING_TO_AUTUMN",
    "Fitness":"INDOOR","Musculation":"INDOOR","Boxe":"INDOOR","Tennis":"SPRING_TO_AUTUMN",
    "Basketball":"INDOOR","Football":"SPRING_TO_AUTUMN","Badminton":"INDOOR","Yoga":"INDOOR",
    "Pilates":"INDOOR","Danse":"INDOOR","Karaté":"INDOOR","Judo":"INDOOR","Handball":"INDOOR",
    "Rugby":"SPRING_TO_AUTUMN","Ping-pong":"INDOOR","Padel":"SPRING_TO_AUTUMN","Squash":"INDOOR",
    "CrossFit":"INDOOR","Ski alpin":"WINTER_SNOW","Ski de fond":"WINTER_SNOW","Snowboard":"WINTER_SNOW",
    "Aviron":"SUMMER_WATER","Canoë-kayak":"SUMMER_WATER","Surf":"SUMMER_WIND","Kitesurf":"SUMMER_WIND",
    "Plongée":"SUMMER_WATER","Stand-up paddle":"SUMMER_WATER","Marche nordique":"SPRING_TO_AUTUMN",
    "Parkour":"SPRING_TO_AUTUMN","Gymnastique":"INDOOR","Trampoline":"INDOOR",
}

# Construction AUTOMATIQUE de SAISONNALITE (couvre 100% des ACTIVITES)
SAISONNALITE = {m: [] for m in range(1, 13)}
for sport in ACTIVITES:
    grp = SPORT_GROUP.get(sport, "ALL_YEAR")
    for m in GROUP_MONTHS[grp]:
        SAISONNALITE[m].append(sport)

# Sous-ensemble de sports "mesurables" (distance_km + temps_sec cohérents)
MESURABLES = {
    "Vélo","Course à pied","Randonnée","Marche","Natation",
    "Surf","Kitesurf","Aviron","Canoë-kayak","Stand-up paddle",
    "Marche nordique","Roller","Trottinette","Skateboard",
    "Ski alpin","Ski de fond","Snowboard"
}

def _self_check_seasonality():
    """Diagnostics utiles au démarrage (logs only)."""
    couverts = set().union(*[set(v) for v in SAISONNALITE.values()])
    manquants = sorted(set(ACTIVITES) - couverts)
    if manquants:
        logger.warning("SAISONNALITE auto ne couvre pas: " + ", ".join(manquants))
    extra = sorted(set(MESURABLES) - set(ACTIVITES))
    if extra:
        logger.error("MESURABLES contient des sports absents d'ACTIVITES: " + ", ".join(extra))

_self_check_seasonality()

# ==========================================================================================
# 5) Physique des activités : distance ↔ vitesse ↔ durée (cohérentes par sport)
# ==========================================================================================

SPORT_MODELES = {
    "Vélo":           {"vitesse_kmh": (16, 28),  "distance_km": (8, 70),   "min_duree_min": 15},
    "Course à pied":  {"vitesse_kmh": (7, 14),   "distance_km": (3, 20),   "min_duree_min": 10},
    "Randonnée":      {"vitesse_kmh": (3.5, 6),  "distance_km": (4, 20),   "min_duree_min": 30},
    "Marche":         {"vitesse_kmh": (4, 6.5),  "distance_km": (2, 12),   "min_duree_min": 15},
    "Natation":       {"vitesse_kmh": (2, 4),    "distance_km": (0.8, 3.5),"min_duree_min": 15},
    "Surf":           {"vitesse_kmh": (3, 8),    "distance_km": (1, 10),   "min_duree_min": 20},
    "Kitesurf":       {"vitesse_kmh": (8, 18),   "distance_km": (5, 25),   "min_duree_min": 20},
    "Aviron":         {"vitesse_kmh": (6, 12),   "distance_km": (2, 12),   "min_duree_min": 15},
    "Canoë-kayak":    {"vitesse_kmh": (5, 10),   "distance_km": (2, 12),   "min_duree_min": 15},
    "Stand-up paddle":{"vitesse_kmh": (3, 7),    "distance_km": (1, 10),   "min_duree_min": 20},
    "Marche nordique":{"vitesse_kmh": (5, 7.5),  "distance_km": (4, 16),   "min_duree_min": 20},
    "Roller":         {"vitesse_kmh": (10, 20),  "distance_km": (5, 30),   "min_duree_min": 15},
    "Trottinette":    {"vitesse_kmh": (8, 16),   "distance_km": (3, 20),   "min_duree_min": 10},
    "Skateboard":     {"vitesse_kmh": (6, 12),   "distance_km": (2, 12),   "min_duree_min": 10},
    "Ski alpin":      {"vitesse_kmh": (10, 30),  "distance_km": (3, 20),   "min_duree_min": 15},
    "Ski de fond":    {"vitesse_kmh": (8, 16),   "distance_km": (4, 25),   "min_duree_min": 20},
    "Snowboard":      {"vitesse_kmh": (8, 24),   "distance_km": (3, 18),   "min_duree_min": 15},

    # fallback "endurance douce"
    "_DEFAULT":       {"vitesse_kmh": (5, 12),   "distance_km": (2, 8),    "min_duree_min": 10},
}

PROFIL_BIAIS = {  # multiplicateur sur la distance visée selon le profil
    "Sédentaire": 0.8,
    "Occasionnel": 1.0,
    "Régulier": 1.15,
    "Compétiteur": 1.25,
}

def _echantillonner_mesure(sport: str, profil_label: str) -> tuple[float, int]:
    """
    Tire (distance_km, temps_sec) de façon cohérente pour (sport, profil).
    Durée = distance / vitesse ; plancher par sport.
    """
    m = SPORT_MODELES.get(sport, SPORT_MODELES["_DEFAULT"])
    bias = PROFIL_BIAIS.get(profil_label, 1.0)
    v = random.uniform(*m["vitesse_kmh"])
    d = random.uniform(*m["distance_km"]) * random.uniform(0.9, 1.1) * bias
    t_sec = max(int((d / max(v, 1e-6)) * 3600), m["min_duree_min"] * 60)
    d = round(d, 2 if sport == "Natation" else 1)
    return d, t_sec

# ==========================================================================================
# 6) Tirages : profils stables, choix pondéré de sport, dates ≤ NOW, lieux cohérents
# ==========================================================================================

def _profil_stable_pour_salarie(id_salarie: str) -> dict:
    """
    Profil DÉTERMINISTE par salarié : hash → score ∈ [0,1) → tirage par probas cumulées.
    """
    h = hashlib.sha256(str(id_salarie).encode("utf-8")).hexdigest()
    r = int(h[:12], 16) / float(0xFFFFFFFFFFFF)
    cumul = 0.0
    for p in PROFILS:
        cumul += p["proba"]
        if r < cumul:
            return p
    return PROFILS[-1]

def _mois_fenetre(date_debut: datetime, date_fin: datetime) -> list[tuple[int, int]]:
    """
    Liste ordonnée (année, mois) couvrant [date_debut .. date_fin] inclus.
    """
    y, m = date_debut.year, date_debut.month
    y_end, m_end = date_fin.year, date_fin.month
    out = []
    while (y, m) <= (y_end, m_end):
        out.append((y, m))
        if m == 12:
            y += 1; m = 1
        else:
            m += 1
    return out

def _date_past_only(year: int, month: int, used: set[datetime]) -> datetime:
    """
    Tire une date locale (naïve) j=1..28, h=06..20, et vérifie que l'instant ≤ NOW_LOCAL.
    Évite les collisions exactes via 'used'.
    """
    while True:
        day = random.randint(1, 28)
        dt_local = datetime(year, month, day, random.randint(6, 20), random.randint(0, 59))
        if dt_local <= NOW_LOCAL and dt_local not in used:
            return dt_local

def _choisir_sport_pondere(mois: int, profil: dict) -> str:
    """
    Choix d'un sport parmi MESURABLES avec pondération :
      - + saison : sports présents dans SAISONNALITE[mois]
      - + affinité : sports préférés du profil (profil["sports"])
    """
    saison_set = set(SAISONNALITE.get(mois, [])) & MESURABLES
    universe   = set(MESURABLES)
    if not saison_set:
        saison_set = universe

    sports = sorted(universe)  # tri pour stabilité
    weights = []
    pref = set(profil.get("sports", []))

    for sp in sports:
        w = 1.0
        if sp in saison_set:
            # Convertit 0..1 en facteur multiplicatif 1..2
            w *= (1.0 + SIMU_SEASON_WEIGHT)
        if sp in pref:
            w *= SIMU_PROFILE_BONUS
        weights.append(w)

    return random.choices(sports, weights=weights, k=1)[0]

def generer_activites_pour_salarie(salarie: pd.Series, date_base: datetime, date_fin: datetime) -> list[dict]:
    """
    Génère une liste d'activités pour un salarié donné, selon :
      - profil figé (stable),
      - saison (pondérée) + affinité profil,
      - pas d'activité future (≤ NOW_LOCAL),
      - mesures physiques cohérentes,
      - lieu compatible (persisté).
    """
    profil = _profil_stable_pour_salarie(salarie["id_salarie"])
    nb_activites = random.randint(profil["min"], profil["max"])
    jours_gen: set[datetime] = set()
    activites: list[dict] = []

    # Fenêtre mois précise (YYYY,MM) entre date_base et date_fin
    mois_pairs = _mois_fenetre(date_base, date_fin)

    # -------------------------------------------------------
    # 50% des salariés : une activité "aujourd'hui" (≤ NOW)
    # -------------------------------------------------------
    if random.random() < 0.5:
        dt_now = NOW_LOCAL  # naïf
        month_now = dt_now.month
        type_act = _choisir_sport_pondere(month_now, profil)
        distance_km, temps_sec = _echantillonner_mesure(type_act, profil["label"])
        lieu_txt = choisir_lieu_pour_sport(
            type_act, month_now,
            seed_key=f"{salarie['prenom']}|{type_act}|{dt_now.date().isoformat()}"
        )
        commentaire = commentaire_coherent(salarie["prenom"], type_act, month_now, extra_emoji=True)

        activites.append({
            "uid": str(uuid.uuid4()),
            "id_salarie": salarie["id_salarie"],
            "nom": salarie["nom"],
            "prenom": salarie["prenom"],
            "date": _fmt_naive(dt_now),              # ← SANS fuseau
            "jour": dt_now.date().isoformat(),
            "date_debut": _fmt_naive(dt_now),        # ← SANS fuseau
            "type_activite": type_act,
            "distance_km": distance_km,
            "temps_sec": temps_sec,
            "commentaire": commentaire,
            "profil": profil["label"],
            "lieu": lieu_txt,
        })
        jours_gen.add(dt_now)

    # -----------------------
    # Génération principale
    # -----------------------
    for _ in range(nb_activites):
        y, m = random.choice(mois_pairs)
        dt_local = _date_past_only(y, m, jours_gen)  # naïf, borné par NOW_LOCAL
        jours_gen.add(dt_local)

        type_act = _choisir_sport_pondere(m, profil)
        distance_km, temps_sec = _echantillonner_mesure(type_act, profil["label"])

        lieu_txt = choisir_lieu_pour_sport(
            type_act, m,
            seed_key=f"{salarie['prenom']}|{type_act}|{dt_local.date().isoformat()}"
        )
        commentaire = commentaire_coherent(salarie["prenom"], type_act, m, extra_emoji=True)

        activites.append({
            "uid": str(uuid.uuid4()),
            "id_salarie": salarie["id_salarie"],
            "nom": salarie["nom"],
            "prenom": salarie["prenom"],
            "date": _fmt_naive(dt_local),            # ← SANS fuseau
            "jour": dt_local.date().isoformat(),
            "date_debut": _fmt_naive(dt_local),      # ← SANS fuseau
            "type_activite": type_act,
            "distance_km": distance_km,
            "temps_sec": temps_sec,
            "commentaire": commentaire,
            "profil": profil["label"],
            "lieu": lieu_txt,
        })

    return activites

# ==========================================================================================
# 7) Helpers I/O — normalisation, stats, logs
# ==========================================================================================

def _normalize_and_validate(df_activites: pd.DataFrame) -> pd.DataFrame:
    """
    Forçage des types + normalisation des timestamps en chaînes NAÏVES (sans timezone).
    → format 'YYYY-MM-DD HH:MM:SS' pour 'date' et 'date_debut'
    """
    required = {
        "uid","id_salarie","prenom","nom","type_activite","distance_km","temps_sec",
        "date_debut","commentaire","profil","date","jour","lieu"
    }
    miss = required - set(df_activites.columns)
    if miss:
        raise ValueError(f"Colonnes manquantes: {miss}")

    df = df_activites.copy()

    # Normaliser 'date_debut' en NAÏF
    _dt = pd.to_datetime(df["date_debut"], errors="coerce", utc=False)
    try:
        _dt = _dt.dt.tz_localize(None)  # si jamais une tz a réussi à se glisser
    except Exception:
        pass
    df["date_debut"] = _dt.dt.strftime("%Y-%m-%d %H:%M:%S")

    # Normaliser 'date' en NAÏF
    _dt2 = pd.to_datetime(df["date"], errors="coerce", utc=False)
    try:
        _dt2 = _dt2.dt.tz_localize(None)
    except Exception:
        pass
    df["date"] = _dt2.dt.strftime("%Y-%m-%d %H:%M:%S")

    # Types numériques
    df["distance_km"] = df["distance_km"].astype(float)
    df["temps_sec"]   = df["temps_sec"].astype(int)

    # Types texte (Kafka & Postgres)
    for c in ["uid","id_salarie","prenom","nom","type_activite","commentaire","profil","jour","lieu"]:
        df[c] = df[c].astype(str)

    return df

def _summarize(df_activites: pd.DataFrame) -> None:
    """Petit résumé statistiques (ordre de grandeur)."""
    try:
        resume = df_activites.agg({
            "distance_km": ["count","min","mean","max"],
            "temps_sec":   ["min","mean","max"]
        }).round(2)
        logger.info(f"📊 Résumé simulation :\n{resume.to_string()}")
    except Exception:
        pass

def _log_repartition_par_mois(df_act: pd.DataFrame) -> None:
    """Répartition des activités par mois (aaaa-mm)."""
    try:
        mois = pd.to_datetime(df_act["jour"], errors="coerce").dt.strftime("%Y-%m")
        agg = mois.value_counts().sort_index()
        if not agg.empty:
            logger.info("🗓️ Répartition par mois (aaaa-mm) :\n" + agg.to_string())
    except Exception:
        pass

# ==========================================================================================
# 8) Pipeline principal (extraction → génération → purge+insert → export → Kafka)
# ==========================================================================================

def pipeline_simulation():
    label_fenetre = "mois en cours" if SIMULATION_MONTHS == 1 else f"{SIMULATION_MONTHS} mois glissants"
    logger.info(f"🚀 Simulation d’activités sportives ({label_fenetre})")

    engine = create_engine(DB_CONN_STRING)

    if DRY_RUN:
        logger.info("🧪 Mode DRY_RUN : lectures OK, écritures désactivées.")

    # 1) Référentiel RH : salariés éligibles (nettoyés à l’étape 02)
    df_sal = pd.read_sql(
        "SELECT id_salarie, nom, prenom FROM sportdata.employes WHERE deplacement_sportif = TRUE",
        engine
    )
    if df_sal.empty:
        logger.warning("⚠️ Aucun salarié sportif trouvé (sportdata.employes). Abandon.")
        return
    logger.info(f"✅ {len(df_sal)} salariés sportifs à simuler.")

    # 2) Fenêtre temporelle exacte (début..fin) — en NAÏF
    date_fin = NOW_LOCAL
    date_base = date_fin - timedelta(days=SIMULATION_MONTHS * 30)
    logger.info(f"🕒 Fenêtre couverte : {date_base.date()} → {date_fin.date()} ({label_fenetre})")

    # 3) Génération
    activites = []
    for _, sal in df_sal.iterrows():
        activites.extend(generer_activites_pour_salarie(sal, date_base, date_fin))

    df_act = pd.DataFrame(activites)
    logger.info(f"🧩 Génération terminée : {len(df_act)} activités simulées (profils + saison + physique).")

    # 4) Normalisation + stats + répartition temporelle
    df_act = _normalize_and_validate(df_act)
    _summarize(df_act)
    _log_repartition_par_mois(df_act)

    # 5) PURGE + INSERT Postgres (idempotence sur la fenêtre)
    if not DRY_RUN:
        # (Option ceinture+bretelles) — s'assure que la colonne 'lieu' existe
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    ALTER TABLE sportdata.activites_sportives
                    ADD COLUMN IF NOT EXISTS lieu text
                """))
        except Exception as e:
            logger.debug(f"ALTER TABLE (ajout 'lieu') ignoré : {e}")

        # Purge ciblée : on efface les activités depuis date_base pour les salariés simulés
        ids = df_sal["id_salarie"].astype(str).unique().tolist()
        with engine.begin() as conn:
            del_stmt = text("""
                DELETE FROM sportdata.activites_sportives
                WHERE date_debut >= :since AND id_salarie IN :ids
            """).bindparams(bindparam("ids", expanding=True))
            conn.execute(del_stmt, {"since": date_base, "ids": ids})
        logger.info(
            f"🧹 Purge effectuée depuis {date_base.isoformat(timespec='seconds')} "
            f"pour {len(ids)} salariés (anti-doublons)."
        )

        # --- INSERT Postgres avec chunksize "safe" (évite la limite 65_535 paramètres) ---
        num_cols = df_act.shape[1]  # typiquement 13 colonnes
        MAX_PARAMS = 65535
        chunk_safe = max(1000, min(4000, (MAX_PARAMS // max(1, num_cols)) - 10))
        logger.info(f"🧮 Insert Postgres optimisé: colonnes={num_cols}, chunksize={chunk_safe}")

        df_act.to_sql(
            "activites_sportives",
            engine,
            schema="sportdata",
            index=False,
            if_exists="append",
            chunksize=chunk_safe,
            method="multi"
        )
        logger.success(
            f"📦 Insert Postgres → sportdata.activites_sportives ({len(df_act)} lignes)"
        )

        # Index idempotents (créés s’ils n’existent pas)
        try:
            with engine.begin() as conn:
                conn.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_acts_uid ON sportdata.activites_sportives(uid);"
                ))
                conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_acts_salarie_date ON sportdata.activites_sportives(id_salarie, date_debut);"
                ))
            logger.info("🧱 Index vérifiés/créés.")
        except Exception as e:
            logger.debug(f"Index non critiques (ignore) : {e}")

    # 6) Export MinIO (Excel)
    if not DRY_RUN:
        helper = MinIOHelper()
        helper.upload_excel(df_act, MINIO_EXPORT_KEY, f"Activités simulées — {label_fenetre}")
        logger.success(f"📤 Export MinIO : {MINIO_EXPORT_KEY} ({len(df_act)} lignes)")

    # 7) Publication Kafka (enveloppe style Debezium — JSON)
    if not DRY_RUN:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            acks="all",
            retries=5,
            linger_ms=50,
            max_in_flight_requests_per_connection=1,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )

        # Arrêt gracieux : flush/close si SIGTERM
        def _graceful(*_):
            try:
                producer.flush(); producer.close()
            except Exception:
                pass
            sys.exit(0)
        signal.signal(signal.SIGTERM, _graceful)

        n_total = len(df_act)
        logger.info(f"🛰️ Publication Kafka → topic={KAFKA_TOPIC} | total={n_total} messages")

        for i, (_, row) in enumerate(df_act.iterrows()):
            message = {
                "payload": {
                    "op": "c",                       # create
                    "after": row.to_dict(),          # colonnes normalisées
                    "ts_ms": int(datetime.utcnow().timestamp() * 1000),
                }
            }
            try:
                producer.send(KAFKA_TOPIC, key=str(row["id_salarie"]), value=message)
                if (i + 1) % 5000 == 0:
                    producer.flush()
                if i < 3 or (i + 1) % 1000 == 0 or i == n_total - 1:
                    logger.info(f"📤 Kafka: {i+1}/{n_total} (uid={row['uid']})")
            except Exception as e:
                logger.warning(f"⚠️ Kafka échec uid={row.get('uid')} : {e}")

        try:
            producer.flush(); producer.close()
            logger.success(f"✅ Kafka : {n_total} messages publiés sur {KAFKA_TOPIC}.")
        except Exception as e:
            logger.warning(f"⚠️ Kafka flush/close : {e}")
    else:
        logger.info("🧪 DRY_RUN : écriture Postgres/MinIO/Kafka désactivée.")

    logger.success("🎯 Simulation terminée.")

# ==========================================================================================
# 9) Entrée CLI
# ==========================================================================================

if __name__ == "__main__":
    try:
        pipeline_simulation()
    except Exception as e:
        logger.error(f"💥 Erreur simulation : {e}")
        sys.exit(1)
