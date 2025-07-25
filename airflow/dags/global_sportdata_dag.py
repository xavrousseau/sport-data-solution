# ======================================================================================
# DAG       : global_sportdata_dag.py
# Objectif  : Orchestration complète du pipeline Sport Data Solution (version corrigée)
# Auteur    : Xavier Rousseau | Corrigé par ChatGPT – Juillet 2025
# ======================================================================================

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from pathlib import Path
import os
import sys
import subprocess

# ======================================================================================
# 1. Ajout du dossier /scripts au PYTHONPATH pour les importations
# ======================================================================================

SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.append(str(SCRIPT_DIR))

# ======================================================================================
# 2. Import des fonctions des étapes (les scripts doivent être bien structurés)
# ======================================================================================

from etape_01_initialiser_minio import main as upload_fichiers_excel
from etape_02_nettoyer_donnees_rh import pipeline_nettoyage_rh
from etape_03_simuler_activites_sportives import pipeline_simulation_sport
from etape_04_calculer_primes_jbe import pipeline_croisement_prime
from etape_05_publier_tables_postgres import main as publier_tables_postgres
from etape_06_lancer_streaming_activites import main as lancer_job_streaming
from etape_08_aggregations_powerbi import main as aggregation_powerbi

# ======================================================================================
# 3. Fonction spécifique pour le contrôle qualité Spark via docker exec
# ======================================================================================

def lancer_job_qualite():
    """
    Lancement du job Spark de contrôle qualité Delta Lake
    via spark-submit dans le conteneur sport-spark.
    """
    cmd = [
        "docker", "exec", "-i", "sport-spark",
        "spark-submit", "/opt/bitnami/spark/jobs/bronze_controle_qualite.py"
    ]
    subprocess.run(cmd, check=True)

# ======================================================================================
# 4. Paramètres de base du DAG
# ======================================================================================

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 1),
    "retries": 0,
}

# ======================================================================================
# 5. Définition du DAG
# ======================================================================================

with DAG(
    dag_id="global_sportdata_pipeline",
    description="Pipeline complet Sport Data Solution (RH → Kafka → Delta → Power BI)",
    default_args=default_args,
    schedule_interval=None,  # exécution manuelle
    catchup=False,
    tags=["sportdata", "bronze", "silver", "spark", "powerbi"],
) as dag:

    # 1. Upload des fichiers Excel vers MinIO
    upload_excel = PythonOperator(
        task_id="upload_fichiers_excel_minio",
        python_callable=upload_fichiers_excel,
    )

    # 2. Nettoyage des données RH + calcul d’éligibilité
    nettoyage_rh = PythonOperator(
        task_id="nettoyer_donnees_rh",
        python_callable=pipeline_nettoyage_rh,
    )

    # 3. Lancement du streaming Spark (doit écouter avant la simulation)
    lancer_streaming = PythonOperator(
        task_id="lancer_streaming_activites",
        python_callable=lancer_job_streaming,
    )

    # 4. Simulation des pratiques sportives (producteur Kafka)
    simulation_sport = PythonOperator(
        task_id="simuler_pratiques_sportives",
        python_callable=pipeline_simulation_sport,
    )

    # 5. Calcul des primes et journées bien-être
    calcul_primes = PythonOperator(
        task_id="calculer_primes_et_journees_bien_etre",
        python_callable=pipeline_croisement_prime,
    )

    # 6. Publication des tables PostgreSQL pour Debezium (CDC)
    publication_cdc = PythonOperator(
        task_id="publier_tables_debezium",
        python_callable=publier_tables_postgres,
    )

    # 7. Contrôle qualité sur Delta Lake
    controle_qualite = PythonOperator(
        task_id="controle_qualite_activites",
        python_callable=lancer_job_qualite,
    )

    # 8. Agrégations Silver pour Power BI + export PostgreSQL
    aggregation_silver = PythonOperator(
        task_id="aggregations_powerbi_silver",
        python_callable=aggregation_powerbi,
    )

    # Fin du pipeline
    fin = DummyOperator(task_id="fin_pipeline")

    # ==================================================================================
    # 6. Orchestration corrigée : STREAMING D'ABORD, SIMULATION APRÈS
    # ==================================================================================

    (
        upload_excel
        >> nettoyage_rh
        >> lancer_streaming
        >> simulation_sport
        >> calcul_primes
        >> publication_cdc
        >> controle_qualite
        >> aggregation_silver
        >> fin
    )
