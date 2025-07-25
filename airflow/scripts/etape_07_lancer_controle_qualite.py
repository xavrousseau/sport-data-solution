# ======================================================================================
# Script      : etape_07_lancer_controle_qualite.py
# Objectif    : Lancer le job Spark de contrôle qualité sur Delta Lake (bronze)
#               - Vérifie les données sportives
#               - Génère un rapport
#               - Peut être intégré dans Airflow
# Auteur      : Xavier Rousseau | Juillet 2025
# ======================================================================================

import subprocess
import os
from dotenv import load_dotenv
from loguru import logger

# ======================================================================================
# 1. Chargement des variables d’environnement (.env global dans /opt/airflow/.env)
# ======================================================================================

load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

SPARK_CONTAINER_NAME = os.getenv("SPARK_CONTAINER_NAME", "sport-spark")
SPARK_SCRIPT_CONTROLE_PATH = os.getenv("SPARK_SCRIPT_CONTROLE_PATH", "/opt/airflow/jobs/bronze_controle_qualite.py")

# ======================================================================================
# 2. Fonction principale appelée par Airflow ou manuellement
# ======================================================================================

def main(**kwargs):
    """
    Lancement du job Spark de contrôle qualité (Delta Lake).
    → Exécute le script Spark dans le conteneur via `docker exec` + `spark-submit`.
    → Attend la fin du processus (bloquant).
    """
    logger.info("🚀 Lancement du job Spark : contrôle qualité sur données Delta Lake")

    # Commande Docker à exécuter : spark-submit dans le conteneur Spark
    cmd = [
        "docker", "exec", "-i", SPARK_CONTAINER_NAME,
        "spark-submit", SPARK_SCRIPT_CONTROLE_PATH
    ]

    try:
        logger.debug(f"🛠️ Commande exécutée : {' '.join(cmd)}")
        subprocess.run(cmd, check=True)  # bloquant, attend la fin
        logger.success("✅ Job Spark de contrôle qualité terminé avec succès.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Échec du job Spark : {e}")
        raise

# ======================================================================================
# 3. Exécution locale (debug / test manuel)
# ======================================================================================

if __name__ == "__main__":
    main()
