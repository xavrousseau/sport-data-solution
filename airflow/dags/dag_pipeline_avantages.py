# dags/dag_pipeline_avantages.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'xavier',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='pipeline_avantages_sportifs',
    default_args=default_args,
    description='Pipeline complet : RH -> Sport -> Primes',
    schedule_interval=None,  # Déclenchement manuel
    catchup=False,
    tags=['sport', 'rh', 'avantages']
) as dag:

    t1_nettoyer_rh = BashOperator(
        task_id='nettoyer_donnees_rh',
        bash_command='python /opt/airflow/scripts/nettoyer_donnees_rh.py',
    )

    t2_simuler_sport = BashOperator(
        task_id='simuler_pratiques_sportives',
        bash_command='python /opt/airflow/scripts/simuler_pratiques_sportives.py',
    )

    t3_calculer_prime = BashOperator(
        task_id='calculer_prime_sportive',
        bash_command='python /opt/airflow/scripts/croiser_rh_sport_et_calculer_prime.py',
    )

    t1_nettoyer_rh >> t2_simuler_sport >> t3_calculer_prime
