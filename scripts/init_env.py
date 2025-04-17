# === init_env.py ===
# Ce script crée un fichier .env s’il n’existe pas et y insère une Fernet key sécurisée.
# À lancer une fois lors de l’initialisation du projet.

import os
from cryptography.fernet import Fernet

env_path = ".env"

# Créer une Fernet Key si elle n’existe pas encore
def generate_fernet_key():
    return Fernet.generate_key().decode()

# Contenu de base du fichier .env avec placeholders
DEFAULT_ENV_CONTENT = f"""# === ENV pour Airflow + PostgreSQL ===
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_PORT=5432

# === Airflow Secrets ===
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY={generate_fernet_key()}
AIRFLOW__WEBSERVER__SECRET_KEY=changeme-in-prod

# === Slack / Google Maps ===
SLACK_TOKEN=xoxb-xxx
SLACK_CHANNEL=#avantages-sportifs
GOOGLE_API_KEY=your_api_key

# === Simulation des données sportives ===
SIMULATION_MONTHS=12
SIMULATION_MIN_ACTIVITIES=10
SIMULATION_MAX_ACTIVITIES=100
"""

def init_env_file():
    if os.path.exists(env_path):
        print(f"✅ Le fichier {env_path} existe déjà. Aucune modification.")
    else:
        with open(env_path, "w") as f:
            f.write(DEFAULT_ENV_CONTENT)
        print(f"✅ Fichier {env_path} créé avec une Fernet key aléatoire.")

if __name__ == "__main__":
    init_env_file()
