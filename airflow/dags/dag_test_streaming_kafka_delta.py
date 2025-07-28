# ======================================================================================
# DAG       : dag_test_streaming_kafka_delta.py
# Objectif  : Tester le flux Kafka → Spark → Delta Lake indépendamment
# Auteur    : Xavier Rousseau | Juillet 2025
# ======================================================================================

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import subprocess
from loguru import logger

# ======================================================================================
# 1. Définition de la tâche : lancement du job Spark Streaming
# ======================================================================================

def lancer_streaming_spark():
    """
    Lance le job Spark de streaming Kafka → Delta.
    Ce test permet de vérifier que les données Kafka sont bien écrites dans Delta Lake.
    """
    cmd = [
        "docker", "exec", "-i", "sport-spark",
        "spark-submit", "/opt/bitnami/spark/jobs/bronze_streaming_activites.py"
    ]
    try:
        logger.info(f"🚀 Lancement job Spark streaming : {' '.join(cmd)}")
        subprocess.Popen(cmd)  # Non bloquant
        logger.success("✅ Job Spark Streaming lancé en arrière-plan")
    except Exception as e:
        logger.error(f"❌ Erreur lancement Spark : {e}")
        raise

# ======================================================================================
# 2. Paramètres du DAG
# ======================================================================================

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 1),
    "retries": 0,
}

# ======================================================================================
# 3. Définition du DAG minimal de test
# ======================================================================================

with DAG(
    dag_id="test_kafka_spark_delta",
    description="Test de la chaîne Kafka → Spark Streaming → Delta Lake",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["test", "streaming", "kafka", "delta"],
) as dag:

    start_test = DummyOperator(task_id="start_test")

    lancer_spark = PythonOperator(
        task_id="lancer_spark_streaming",
        python_callable=lancer_streaming_spark,
    )

    end_test = DummyOperator(task_id="end_test")

    # Orchestration simple
    start_test >> lancer_spark >> end_test
