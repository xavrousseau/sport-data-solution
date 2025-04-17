# ============================================================================
# Dockerfile personnalisé pour Airflow
# Permet d'ajouter le support de DockerOperator (provider Docker)
# ============================================================================

# Image officielle Airflow avec Python 3.11
FROM apache/airflow:2.7.0-python3.11  

# Installation du provider docker d'Airflow (nécessaire pour DockerOperator)
RUN pip install apache-airflow-providers-docker

# (Optionnel) : Ajouter des packages supplémentaires si besoin
# RUN pip install pandas requests openpyxl etc...

# On désactive les exemples de DAGs (pour une interface propre)
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
