# =============================================================================
# Dockerfile de base — Projet Python (Airflow, scripts batch, etc.)
# Objectif : Construire une image légère, reproductible et maintenable
#            avec toutes les dépendances du projet (requirements.txt)
# Auteur   : Xavier Rousseau | Juin 2025
# =============================================================================

# -----------------------------------------------------------------------------
# 1. Image de base Python (ici : 3.11-slim pour performance et sécurité)
#    - Utilise l'image officielle Python optimisée (moins de 100 Mo)
#    - Si tu fais de l’Airflow, vérifie la compatibilité des libs Airflow !
# -----------------------------------------------------------------------------
FROM python:3.11-slim

# -----------------------------------------------------------------------------
# 2. Variables d’environnement Python
#    - PYTHONDONTWRITEBYTECODE : empêche la création de fichiers .pyc (propreté)
#    - PYTHONUNBUFFERED        : logs affichés directement en console (no buffer)
#    - LANG / LC_ALL           : encodage UTF-8 forcé (robustesse multi-langue)
# -----------------------------------------------------------------------------
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# -----------------------------------------------------------------------------
# 3. Définition du répertoire de travail principal (tout sera exécuté depuis /app)
#    - Évite les chemins relatifs casse-pied
# -----------------------------------------------------------------------------
WORKDIR /app

# -----------------------------------------------------------------------------
# 4. Installation des dépendances Python du projet
#    - requirements.txt = single source of truth des libs (pandas, airflow, etc.)
#    - --no-cache-dir   = image plus légère (cache pip effacé après install)
# -----------------------------------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------------------
# 5. Copie du code source, des scripts, des tests, des données et des logs (si besoin)
#    - Permet d’avoir accès à tout le nécessaire dans l’image (dev ou prod)
#    - Ajuste ces lignes si tu veux n’exposer que scripts ou data, etc.
# -----------------------------------------------------------------------------
COPY ./scripts ./scripts
COPY ./tests ./tests
COPY ./data ./data
COPY ./logs ./logs

# -----------------------------------------------------------------------------
# 6. Commande de démarrage (par défaut = container interactif idle)
#    - Parfait pour debug, exec manuel ou développement
#    - À personnaliser pour un job, une API, un service…
# -----------------------------------------------------------------------------
CMD ["tail", "-f", "/dev/null"]

# =============================================================================
# FIN DE FICHIER — Dockerfile
# =============================================================================
