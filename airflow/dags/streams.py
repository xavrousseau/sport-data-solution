# dags/streams.py
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

common_env = {
    "NTFY_ENABLED": "true",
    "KAFKA_STARTING_OFFSETS": "latest",
}
image = "yourrepo/spark:latest"

with DAG(
    dag_id="streams_services",
    start_date=datetime(2025, 8, 1),
    schedule=None,  # run on-demand
    catchup=False,
    tags=["stream","service"],
) as dag:

    stream_activites = KubernetesPodOperator(
        task_id="stream_activites",
        name="stream-activites",
        image=image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=["/opt/spark/jobs/etape_07_spark_bronze_streaming_activites.py"],
        env_vars=common_env,
        is_delete_operator_pod=False,
        get_logs=True,
        in_cluster=True,
        do_xcom_push=False,
        # restart policy is handled by K8s Deployment ideally (better than a Pod)
    )

    stream_qc = KubernetesPodOperator(
        task_id="stream_qc",
        name="stream-qc",
        image=image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=["/opt/spark/jobs/etape_08_spark_bronze_controle_qualite.py"],
        env_vars=common_env,
        is_delete_operator_pod=False,
        get_logs=True,
        in_cluster=True,
        do_xcom_push=False,
    )

    stream_primes_jbe = KubernetesPodOperator(
        task_id="stream_primes_jbe",
        name="stream-primes-jbe",
        image=image,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=["/opt/spark/jobs/etape_09_spark_bronze_streaming_primes_jbe.py"],
        env_vars=common_env,
        is_delete_operator_pod=False,
        get_logs=True,
        in_cluster=True,
        do_xcom_push=False,
    )

    # Tu peux les lancer en parallèle
    [stream_activites, stream_qc, stream_primes_jbe]
