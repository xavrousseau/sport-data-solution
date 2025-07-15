# dags/dag_initialisation_postgres.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "xavier",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="dag_initialisation_postgres",
    description="Initialisation PostgreSQL + Publication Debezium",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["init", "postgres", "debezium"],
) as dag:

    initialiser_publication_debezium = BashOperator(
        task_id="initialiser_publication_debezium",
        bash_command="python /opt/airflow/scripts/ajouter_tables_publication.py",
    )

    initialiser_publication_debezium
