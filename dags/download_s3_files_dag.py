
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
import requests

# === Déclaration du DAG ===
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'download_s3_files',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Téléchargement de fichiers depuis un S3 public (3 méthodes)"
)

# === PythonOperator : Téléchargement des données RH ===
def download_file(url, dest):
    response = requests.get(url)
    with open(dest, "wb") as f:
        f.write(response.content)

download_rh = PythonOperator(
    task_id="download_rh",
    python_callable=download_file,
    op_kwargs={
        "url": "https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/922_Data+Engineer/1039_P12/Donne%CC%81es+RH.xlsx",
        "dest": "/opt/airflow/data/inputs/donnees_rh.xlsx"
    },
    dag=dag
)

# === BashOperator : Téléchargement des données sportives ===
download_sport = BashOperator(
    task_id='download_sport',
    bash_command=(
        'curl -s -o /opt/airflow/data/inputs/donnees_sportive.xlsx '
        'https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/922_Data+Engineer/1039_P12/Donne%CC%81es+Sportive.xlsx'
    ),
    dag=dag
)

# === DockerOperator : Téléchargement de la note de cadrage ===
download_note = DockerOperator(
    task_id="download_note",
    image="curlimages/curl:latest",
    command=(
        "curl -s -o /data/Note_de_cadrage.pdf "
        "https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/922_Data+Engineer/1039_P12/Note+de+cadrage+_+POC+Avantages+Sportifs+(1).pdf"
    ),
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mount_tmp_dir=False,
    volumes=["/opt/airflow/data/inputs:/data"],
    dag=dag
)

# === Ordre d'exécution (en parallèle) ===
[download_rh, download_sport, download_note]
