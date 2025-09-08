# dags/daily_batch.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime

def streams_healthcheck(**_):
    # TODO: implémenter un check simple (ex: liste des checkpoints S3A mise à jour < N minutes)
    # ou requête sur Kafka lag si tu as un exporter.
    return PokeReturnValue(is_done=True)  # placeholder

with DAG(
    dag_id="daily_batch",
    start_date=datetime(2025, 8, 1),
    schedule="0 5 * * *",
    catchup=False,
    tags=["daily","batch"],
) as dag:

    # Optionnel: santé streams
    check_streams = PythonOperator(
        task_id="streams_healthcheck",
        python_callable=streams_healthcheck
    )

    rh_clean = BashOperator(
        task_id="rh_clean",
        bash_command="python /opt/airflow/scripts/etape_02_nettoyer_donnees_rh.py",
        env={"NTFY_ENABLED": "true"}
    )

    simulate = BashOperator(
        task_id="simulate_activities",
        bash_command="python /opt/airflow/scripts/etape_03_simuler_activites_sportives.py",
        env={"NTFY_ENABLED": "true"}
    )

    compute_primes_jbe = BashOperator(
        task_id="compute_primes_jbe",
        bash_command="python /opt/airflow/scripts/etape_04_calculer_primes_jbe.py",
        env={"NTFY_ENABLED": "true"}
    )

    gold_aggregations = BashOperator(
        task_id="gold_aggregations",
        bash_command="spark-submit /opt/spark/jobs/etape_10_spark_gold_aggregations_powerbi.py"
    )

    # Dépendances
    check_streams >> rh_clean >> simulate >> compute_primes_jbe >> gold_aggregations
