# ==========================================================================================
# Fichier     : dags/init_minio_structure_dag.py
# Objectif    : DAG Airflow pour créer un bucket MinIO et sa structure de préfixes ("dossiers")
#
# Description :
#   - Ce DAG effectue l'initialisation de la structure MinIO requise pour la stack Sport Data Solution.
#   - Il crée le bucket s'il n'existe pas, puis force la création des "dossiers" (préfixes S3)
#     en ajoutant un fichier vide ".keep" dans chaque.
#
# Prérequis :
#   - Le module minio_helper.py doit être dans le dossier scripts/ accessible au scheduler Airflow
#   - Les variables d'environnement MinIO doivent être présentes dans /opt/airflow/.env
#
# Utilisation :
#   - Lancer ce DAG manuellement depuis Airflow UI pour initialiser MinIO avant tout traitement.
#
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
import sys

# ------------------------------------------------------------------------------------------
# Ajout du dossier scripts/ au PYTHONPATH pour permettre l'import du helper MinIO
# ------------------------------------------------------------------------------------------
sys.path.append(str(Path("/opt/airflow/scripts")))

from minio_helper import MinIOHelper

# Paramètres de structure du bucket MinIO
BUCKET = "sportdata"
PREFIXES = [
    "referentiels/",
    "simulation/",
    "raw/",
    "resultats/prime_sportive/",
    "resultats/jours_bien_etre/",
    "exports/"
]

# ------------------------------------------------------------------------------------------
# Tâche Python : création du bucket si absent
# ------------------------------------------------------------------------------------------
def create_bucket():
    """
    Crée le bucket MinIO s'il n'existe pas déjà (idempotent).
    """
    helper = MinIOHelper()
    try:
        helper.create_bucket_if_missing()
    except Exception as e:
        print(f"❌ Erreur création bucket : {e}")
        raise

# ------------------------------------------------------------------------------------------
# Tâche Python : création de la structure de préfixes/dossiers
# ------------------------------------------------------------------------------------------
def create_prefixes():
    """
    Crée les "dossiers" (préfixes S3) en ajoutant un fichier vide .keep dans chacun.
    """
    helper = MinIOHelper()
    for prefix in PREFIXES:
        key = prefix + ".keep"
        try:
            helper.client.put_object(Bucket=BUCKET, Key=key, Body=b"")
            print(f"📂 Préfixe créé : {key}")
        except Exception as e:
            print(f"❌ Erreur création préfixe {key} : {e}")
            raise

# ------------------------------------------------------------------------------------------
# Définition du DAG Airflow (exécution manuelle, pas de planification automatique)
# ------------------------------------------------------------------------------------------
with DAG(
    dag_id="init_minio_structure",
    description="Créer bucket et préfixes MinIO (dossiers virtuels S3)",
    start_date=datetime(2025, 7, 3),
    schedule_interval=None,  # Exécution manuelle uniquement
    catchup=False,
    tags=["minio", "init"]
) as dag:

    # Tâche 1 : création du bucket
    t1 = PythonOperator(
        task_id="create_bucket",
        python_callable=create_bucket
    )

    # Tâche 2 : création des préfixes/dossiers
    t2 = PythonOperator(
        task_id="create_prefixes",
        python_callable=create_prefixes
    )

    # Orchestration : création du bucket puis des dossiers
    t1 >> t2

# ------------------------------------------------------------------------------------------
# Fin du fichier : ce DAG initialise la structure MinIO pour le projet
# ------------------------------------------------------------------------------------------
