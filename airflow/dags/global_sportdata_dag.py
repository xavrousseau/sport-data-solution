# ======================================================================================
# DAG       : global_sportdata_dag.py
# Objectif  : Orchestration complète du pipeline Sport Data Solution (hors streaming)
# Auteur    : Xavier Rousseau | Version corrigée par ChatGPT – Juillet 2025
# ======================================================================================

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from pathlib import Path
import sys

# ======================================================================================
# 1. Ajout du dossier /scripts au PYTHONPATH
# ======================================================================================

SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.append(str(SCRIPT_DIR))

# ======================================================================================
# 2. Import des fonctions Python appelées dans le DAG
# ======================================================================================

from etape_01_initialiser_minio import main as initialiser_minio
from etape_02_nettoyer_donnees_rh import pipeline_nettoyage_rh
from etape_03_simuler_activites_sportives import pipeline_simulation_sport
from etape_04_calculer_primes_jbe import pipeline_croisement_prime
from etape_05_publier_tables_postgres import main as publier_postgres
from etape_07_lancer_controle_qualite import main as lancer_qualite
from etape_08_aggregations_powerbi import main as aggregation_powerbi

# ======================================================================================
# 3. Paramètres de base du DAG
# ======================================================================================

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 1),
    "retries": 0,
}

with DAG(
    dag_id="global_sportdata_pipeline",
    description="Pipeline complet : PostgreSQL → Kafka → Delta → Power BI (hors streaming)",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sportdata", "cdc", "spark", "minio", "ntfy", "powerbi"],
) as dag:

    # ÉTIQUETTES DE PHASES
    debut = DummyOperator(task_id="debut_pipeline")
    fin = DummyOperator(task_id="fin_pipeline")

    phase_donnees = DummyOperator(task_id="phase_1_donnees_sources")
    phase_production = DummyOperator(task_id="phase_2_simulation_production")
    phase_controle = DummyOperator(task_id="phase_3_controle_et_agregation")

    # 1. Initialisation MinIO
    t1_init_minio = PythonOperator(
        task_id="initialiser_minio_et_fichiers",
        python_callable=initialiser_minio,
    )

    # 2. Nettoyage des données RH
    t2_nettoyage_rh = PythonOperator(
        task_id="nettoyer_donnees_rh",
        python_callable=pipeline_nettoyage_rh,
    )

    # 3. Publication initiale des tables PostgreSQL
    t3_publier_1 = PythonOperator(
        task_id="publier_tables_postgres_1",
        python_callable=publier_postgres,
    )

    # 4. Simulation des pratiques sportives (Kafka producer)
    t4_simulation = PythonOperator(
        task_id="simuler_activites_sportives",
        python_callable=pipeline_simulation_sport,
    )

    # 5. Calcul des primes + journées bien-être
    t5_primes_jbe = PythonOperator(
        task_id="calculer_primes_et_jbe",
        python_callable=pipeline_croisement_prime,
    )

    # 6. Re-publication des tables PostgreSQL (post-primes)
    t6_publier_2 = PythonOperator(
        task_id="publier_tables_postgres_2",
        python_callable=publier_postgres,
    )

    # 7. Lancer le contrôle qualité Spark (Delta)
    t7_qualite = PythonOperator(
        task_id="lancer_controle_qualite_spark",
        python_callable=lancer_qualite,
    )

    # 8. Aggrégations Power BI + export PostgreSQL
    t8_agregation = PythonOperator(
        task_id="agregation_finale_powerbi",
        python_callable=aggregation_powerbi,
    )

    # ==================================================================================
    # ORCHESTRATION STRUCTURÉE DU PIPELINE
    # ==================================================================================

    debut >> phase_donnees
    phase_donnees >> t1_init_minio >> t2_nettoyage_rh >> t3_publier_1

    t3_publier_1 >> phase_production >> t4_simulation >> t5_primes_jbe >> t6_publier_2

    t6_publier_2 >> phase_controle >> t7_qualite >> t8_agregation >> fin
