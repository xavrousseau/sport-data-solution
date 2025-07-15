# dags/dag_pipeline_referentiels.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    "owner": "xavier",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="dag_pipeline_referentiels",
    description="Pipeline de nettoyage RH + simulation Strava",
    default_args=default_args,
    schedule_interval=None,  # déclenchement manuel
    catchup=False,
    tags=["referentiels", "minio", "strava"],
) as dag:

    upload_referentiels_minio = BashOperator(
        task_id="upload_referentiels_minio",
        bash_command="python /opt/airflow/scripts/upload_referentiels_minio.py",
    )

    nettoyer_donnees_rh = BashOperator(
        task_id="nettoyer_donnees_rh",
        bash_command="python /opt/airflow/scripts/nettoyer_donnees_rh.py",
    )

    simuler_activites_strava = BashOperator(
        task_id="simuler_activites_strava",
        bash_command="python /opt/airflow/scripts/simuler_pratiques_sportives.py",
    )

    upload_referentiels_minio >> nettoyer_donnees_rh >> simuler_activites_strava
