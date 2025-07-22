# ======================================================================================
# DAG       : global_sportdata_dag.py
# Objectif  : Orchestration complète du pipeline Sport Data Solution
#             (MinIO → PostgreSQL → Delta → Power BI), via appels directs de fonctions
# Auteur    : Xavier Rousseau | Version corrigée – Juillet 2025
# ======================================================================================

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pathlib import Path
import sys

# ======================================================================================
# 1. Ajout dynamique du dossier /scripts/ au PYTHONPATH
# ======================================================================================

SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.append(str(SCRIPT_DIR))

# ======================================================================================
# 2. Import des fonctions principales depuis les scripts Python
# ======================================================================================

from upload_fichiers_excel_minio import main as upload_fichiers_excel
from nettoyer_donnees_rh import pipeline_nettoyage_rh
from simuler_pratiques_sportives import pipeline_simulation_sport
from bronze_controle_qualite import controle_qualite
from silver_aggregations import pipeline_aggregation_silver

# Facultatif si ces scripts sont prêts
try:
    from croiser_rh_sport_et_calculer_prime import pipeline_croisement_prime
    from ajouter_tables_publication import main as publier_tables_postgres
except ImportError:
    pipeline_croisement_prime = lambda: None
    publier_tables_postgres = lambda: None

# ======================================================================================
# 3. Paramètres par défaut du DAG
# ======================================================================================

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 1),
    "retries": 0,
}

# ======================================================================================
# 4. Définition du DAG principal
# ======================================================================================

with DAG(
    dag_id="global_sportdata_pipeline",
    description="Pipeline complet Sport Data Solution (bronze → silver → export Power BI)",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sportdata", "bronze", "silver", "powerbi"],
) as dag:

    # Étape 1 : Upload initial des fichiers Excel source (RH/Sport) dans MinIO
    upload_excel = PythonOperator(
        task_id="upload_fichiers_excel_minio",
        python_callable=upload_fichiers_excel,
    )

    # Étape 2 : Nettoyage RH avec vérification d’éligibilité
    nettoyage_rh = PythonOperator(
        task_id="nettoyer_donnees_rh",
        python_callable=pipeline_nettoyage_rh,
    )

    # Étape 3 : Simulation des pratiques sportives (Kafka + MinIO + PostgreSQL)
    simulation_sport = PythonOperator(
        task_id="simuler_activites_sportives",
        python_callable=pipeline_simulation_sport,
    )

    # Étape 4 : Croisement RH & Activités + calcul des primes
    calcul_prime = PythonOperator(
        task_id="calculer_primes_et_journees_bien_etre",
        python_callable=pipeline_croisement_prime,
    )

    # Étape 5 : Publication CDC (Debezium)
    publication_cdc = PythonOperator(
        task_id="publier_tables_debezium",
        python_callable=publier_tables_postgres,
    )

    # Étape 6 : Contrôle qualité des activités dans Delta Lake (règles simples)
    controle_qualite_delta = PythonOperator(
        task_id="controle_qualite_activites",
        python_callable=controle_qualite,
    )

    # Étape 7 : Agrégation finale pour restitution (Power BI / PostgreSQL)
    export_aggregations = PythonOperator(
        task_id="aggregations_powerbi_silver",
        python_callable=pipeline_aggregation_silver,
    )

    # ==================================================================================
    # 5. Orchestration logique
    # ==================================================================================

    (
        upload_excel
        >> nettoyage_rh
        >> simulation_sport
        >> calcul_prime
        >> publication_cdc
        >> controle_qualite_delta
        >> export_aggregations
    )
