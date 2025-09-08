# dags/bootstrap.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="bootstrap_once",
    start_date=datetime(2025, 8, 1),
    schedule="@once",
    catchup=False,
    tags=["bootstrap","infra"],
) as dag:

    init_minio = BashOperator(
        task_id="init_minio",
        bash_command="python /opt/airflow/scripts/etape_01_initialiser_minio.py"
    )

    publish_pg_cdc = BashOperator(
        task_id="publish_pg_cdc",
        bash_command="python /opt/airflow/scripts/etape_05_publier_tables_postgres.py"
    )

    spark_init_delta = BashOperator(
        task_id="spark_init_delta",
        bash_command="spark-submit /opt/spark/jobs/etape_06_spark_init_delta_table.py"
    )

    init_minio >> publish_pg_cdc >> spark_init_delta
