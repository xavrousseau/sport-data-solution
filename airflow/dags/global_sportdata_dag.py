# ==========================================================================================
# DAG       : global_sportdata_dag.py
# Objectif  : Orchestration complète du pipeline Avantages Sportifs (RH → Sport → PostgreSQL)
# Auteur    : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
from loguru import logger

# ==========================================================================================
# 1. Définition des chemins des scripts
# ==========================================================================================
SCRIPTS_DIR = "/opt/airflow/scripts"

SCRIPTS = {
    "init_minio": "init_minio_structure.py",
    "upload_excels": "upload_fichiers_excel_minio.py",
    "nettoyer_rh": "nettoyer_donnees_rh.py",
    "nettoyer_sport": "nettoyer_donnees_sportives.py",
    "simuler_activites": "simuler_pratiques_sportives.py",
    "croiser_rh_sport": "croiser_rh_sport_et_calculer_prime.py",
    "ajouter_tables": "ajouter_tables_publication.py",
    "export_powerbi": "export_powerbi.py",
}

# ==========================================================================================
# 2. Tâche générique pour exécuter un script Python
# ==========================================================================================
def run_script(script_name):
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    if not os.path.exists(script_path):
        logger.error(f"❌ Script introuvable : {script_path}")
        raise FileNotFoundError(script_path)
    logger.info(f"🚀 Exécution du script : {script_path}")
    exit_code = os.system(f"python {script_path}")
    if exit_code != 0:
        logger.error(f"❌ Erreur dans le script {script_name} (code: {exit_code})")
        raise Exception(f"Script échoué : {script_name}")
    logger.success(f"✅ Script terminé : {script_name}")

# ==========================================================================================
# 3. Définition du DAG Airflow
# ==========================================================================================
default_args = {
    "owner": "xavier",
    "email": ["admin@sportdata.fr"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="global_sportdata_pipeline",
    default_args=default_args,
    description="DAG global — Pipeline complet Avantages Sportifs",
    schedule_interval=None,  # Exécution manuelle ou planifiée via déclencheur externe
    start_date=days_ago(1),
    tags=["sportdata", "pipeline", "global"],
    catchup=False,
) as dag:

    # Étape 1 : Initialisation MinIO
    t_init_minio = PythonOperator(
        task_id="init_minio_structure",
        python_callable=run_script,
        op_args=[SCRIPTS["init_minio"]],
    )

    # Étape 2 : Upload initial des fichiers Excel RH/Sport
    t_upload_files = PythonOperator(
        task_id="upload_fichiers_excel",
        python_callable=run_script,
        op_args=[SCRIPTS["upload_excels"]],
    )

    # Étape 3 : Nettoyage RH
    t_nettoyer_rh = PythonOperator(
        task_id="nettoyer_donnees_rh",
        python_callable=run_script,
        op_args=[SCRIPTS["nettoyer_rh"]],
    )

    # Étape 4 : Nettoyage activités sportives
    t_nettoyer_sport = PythonOperator(
        task_id="nettoyer_donnees_sportives",
        python_callable=run_script,
        op_args=[SCRIPTS["nettoyer_sport"]],
    )

    # Étape 5 : Simulation des activités
    t_simuler_activites = PythonOperator(
        task_id="simuler_pratiques_sportives",
        python_callable=run_script,
        op_args=[SCRIPTS["simuler_activites"]],
    )

    # Étape 6 : Croisement RH / Sport + calcul de la prime
    t_croiser_rh_sport = PythonOperator(
        task_id="croiser_rh_sport_et_calculer_prime",
        python_callable=run_script,
        op_args=[SCRIPTS["croiser_rh_sport"]],
    )

    # Étape 7 : Ajout des tables de publication
    t_ajouter_tables = PythonOperator(
        task_id="ajouter_tables_publication",
        python_callable=run_script,
        op_args=[SCRIPTS["ajouter_tables"]],
    )

    # Étape 8 : Export Power BI
    t_export_powerbi = PythonOperator(
        task_id="export_powerbi",
        python_callable=run_script,
        op_args=[SCRIPTS["export_powerbi"]],
    )

    # Dépendances
    t_init_minio >> t_upload_files
    t_upload_files >> [t_nettoyer_rh, t_nettoyer_sport]
    t_nettoyer_rh >> t_simuler_activites
    t_simuler_activites >> t_croiser_rh_sport
    t_croiser_rh_sport >> t_ajouter_tables >> t_export_powerbi
