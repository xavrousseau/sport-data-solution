# ================================================================================
# FICHIER : Dockerfile.airflow
# OBJET   : Image personnalisée Apache Airflow avec :
#            - Dépendances Python personnalisées
#            - Providers essentiels (Postgres, Redis, Celery)
#            - Timezone UTC fixée
# AUTEUR  : Xavier Rousseau — Révision Juin 2025
# ================================================================================

# ------------------------------------------------------------------------------
# 1. Image de base : Airflow officielle, version fixée
# ------------------------------------------------------------------------------
FROM apache/airflow:2.9.0-python3.10

# ------------------------------------------------------------------------------
# 2. Copie du fichier de dépendances
# ------------------------------------------------------------------------------
COPY requirements.txt /requirements.txt

# ------------------------------------------------------------------------------
# 3. Installation groupée des dépendances + providers Airflow nécessaires
# ------------------------------------------------------------------------------
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install --no-cache-dir \
        apache-airflow-providers-postgres \
        apache-airflow-providers-redis \
        apache-airflow-providers-celery && \
    pip check     # Vérifie que les dépendances sont cohérentes

# ------------------------------------------------------------------------------
# 4. Fixe la timezone à UTC pour éviter des bugs liés à Flask ou aux sessions Airflow
# ------------------------------------------------------------------------------
USER root
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
USER airflow

# ------------------------------------------------------------------------------
# 5. Répertoire de travail par défaut pour Airflow
# ------------------------------------------------------------------------------
WORKDIR /opt/airflow

# ================================================================================
# FIN DU FICHIER
# ================================================================================
