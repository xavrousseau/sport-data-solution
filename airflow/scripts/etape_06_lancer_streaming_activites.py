# ======================================================================================
# Script      : etape_06_lancer_streaming_activites.py
# Objectif    : Lancer manuellement le job Spark streaming Kafka → Delta Lake
#               - Ce script déclenche un traitement en continu des activités sportives
#               - Lecture depuis Kafka (Debezium), écriture dans Delta Lake (MinIO)
#               - Utilisable à la main ou via un DAG Airflow
# Auteur      : Xavier Rousseau | Juillet 2025
# ======================================================================================

import subprocess
import os
from dotenv import load_dotenv
from loguru import logger

# ======================================================================================
# 1. Chargement des variables d’environnement (.env global monté dans Airflow)
# ======================================================================================

load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

SPARK_CONTAINER_NAME = os.getenv("SPARK_CONTAINER_NAME", "sport-spark")  # Nom du conteneur Spark
SPARK_STREAMING_SCRIPT = os.getenv("SPARK_SCRIPT_STREAMING_PATH", "/opt/airflow/jobs/bronze_streaming_activites.py")

# ======================================================================================
# 2. Fonction principale appelée par Airflow ou en local
# ======================================================================================

def main(**kwargs):
    """
    Lancement manuel du job Spark Streaming (Kafka → Delta Lake)
    ------------------------------------------------------------
    Ce job lit les messages Kafka contenant les activités sportives
    (produits par Debezium/PostgreSQL), puis :
      - parse les messages JSON
      - valide les champs attendus
      - écrit le flux dans Delta Lake (bronze) sur MinIO

    La commande est lancée en arrière-plan avec subprocess.Popen()
    pour ne pas bloquer Airflow.
    """
    logger.info("🚀 Lancement du job Spark Streaming (Kafka → Delta Lake)")

    cmd = [
        "docker", "exec", "-i", SPARK_CONTAINER_NAME,
        "spark-submit", SPARK_STREAMING_SCRIPT
    ]

    try:
        logger.debug(f"🛠️ Commande exécutée : {' '.join(cmd)}")
        subprocess.Popen(cmd)
        logger.success("✅ Job Spark Streaming lancé en arrière-plan.")
    except Exception as e:
        logger.error(f"❌ Erreur lors du lancement Spark Streaming : {e}")
        raise

# ======================================================================================
# 3. Point d’entrée direct (utilisation en CLI si besoin)
# ======================================================================================

if __name__ == "__main__":
    main()
