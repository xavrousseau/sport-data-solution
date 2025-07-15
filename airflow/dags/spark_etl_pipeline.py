# ==========================================================================================
# DAG       : spark_etl_pipeline.py
# Objectif  : Pipeline Spark dédié au traitement Kafka → Delta → Contrôle → Export
# Auteur    : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
from loguru import logger

# ==========================================================================================
# 1. Chemins des scripts Spark
# ==========================================================================================
SCRIPTS_DIR = "/opt/airflow/scripts"

SCRIPTS = {
    "traitement_kafka": "traitement_activites_batch.py",
    "controle_qualite": "controle_qualite_activites.py",
    "verifier_delta": "verifier_donnees_delta.py",
    "export_powerbi": "export_powerbi.py",
}

# ==========================================================================================
# 2. Fonction de wrapper pour lancer les scripts Spark
# ==========================================================================================
def run_script(script_name):
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    if not os.path.exists(script_path):
        logger.error(f"Script introuvable : {script_path}")
        raise FileNotFoundError(script_path)
    logger.info(f"Exécution du script Spark : {script_path}")
    exit_code = os.system(f"python {script_path}")
    if exit_code != 0:
        logger.error(f"Échec script : {script_name}")
        raise Exception(f"Script échoué : {script_name}")
    logger.success(f"✅ Script terminé : {script_name}")

# ==========================================================================================
# 3. DAG Spark
# ==========================================================================================
default_args = {
    "owner": "spark",
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="spark_etl_pipeline",
    default_args=default_args,
    description="Pipeline Spark — Kafka CDC vers Delta, contrôle et export",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["spark", "delta", "etl"],
) as dag:

    # 1. Traitement Redpanda → Delta Lake
    t_kafka_to_delta = PythonOperator(
        task_id="traitement_kafka_batch",
        python_callable=run_script,
        op_args=[SCRIPTS["traitement_kafka"]],
    )

    # 2. Contrôle qualité avec Great Expectations
    t_controle_ge = PythonOperator(
        task_id="controle_qualite_delta",
        python_callable=run_script,
        op_args=[SCRIPTS["controle_qualite"]],
    )

    # 3. Vérification (simple) contenu Delta
    t_verifier_delta = PythonOperator(
        task_id="verifier_donnees_delta",
        python_callable=run_script,
        op_args=[SCRIPTS["verifier_delta"]],
    )

    # 4. Export Power BI (CSV MinIO)
    t_export_powerbi = PythonOperator(
        task_id="export_powerbi_csv",
        python_callable=run_script,
        op_args=[SCRIPTS["export_powerbi"]],
    )

    # Dépendances
    t_kafka_to_delta >> t_controle_ge >> t_verifier_delta >> t_export_powerbi
