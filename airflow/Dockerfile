# ================================================================================
# FICHIER   : airflow/Dockerfile
# OBJET     : Image légère et stable pour Airflow 2.9.0 avec CeleryExecutor
#             (Les providers nécessaires sont installés via requirements.txt)
# AUTEUR    : Xavier Rousseau — Projet Sport Data Solution
# ================================================================================
# BASE : Image officielle Apache Airflow avec Python 3.10
# ================================================================================

FROM apache/airflow:2.9.0-python3.10

# ------------------------------------------------------------------------------
# Étape 1 : Ajout des dépendances personnalisées du projet (requirements.txt)
# ------------------------------------------------------------------------------
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ------------------------------------------------------------------------------
# Étape 2 : Configuration de la timezone système (UTC recommandé pour Airflow)
# ------------------------------------------------------------------------------
USER root
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
USER airflow

# ------------------------------------------------------------------------------
# Étape 3 : Répertoire de travail par défaut (standard Airflow)
# ------------------------------------------------------------------------------
WORKDIR /opt/airflow

# ================================================================================
# FIN DU FICHIER — airflow/Dockerfile
# ================================================================================
