from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG("hello_world", start_date=datetime(2023, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    def hello():
        print("Hello Airflow !")

    hello_task = PythonOperator(task_id="say_hello", python_callable=hello)
