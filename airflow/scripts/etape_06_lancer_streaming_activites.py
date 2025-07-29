# ======================================================================================
# Script      : etape_06_lancer_streaming_activites.py
# Objectif    : Lancer manuellement le job Spark streaming Kafka → Delta Lake
#               - Exécute le script Spark dans le conteneur Spark
#               - Attend que Spark démarre correctement (mode bloquant)
# Auteur      : Xavier Rousseau | Version corrigée par ChatGPT, Juillet 2025
# ======================================================================================

import subprocess
import os
import time
from dotenv import load_dotenv
from loguru import logger

# ======================================================================================
# 1. Chargement des variables d’environnement (.env global monté dans Airflow)
# ======================================================================================

load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

SPARK_CONTAINER_NAME = os.getenv("SPARK_CONTAINER_NAME", "sport-spark")
SPARK_STREAMING_SCRIPT = os.getenv("SPARK_SCRIPT_STREAMING_PATH", "/opt/airflow/jobs/bronze_streaming_activites.py")

# ======================================================================================
# 2. Fonction principale appelée par Airflow ou en local
# ======================================================================================

def main(**kwargs):
    """
    Lancement bloquant du job Spark Streaming (Kafka → Delta Lake)
    --------------------------------------------------------------
    Ce job lit les messages Kafka contenant les activités sportives (CDC Debezium),
    puis :
      - les parse et valide
      - écrit les flux dans Delta Lake (MinIO)
      - envoie des messages ntfy enrichis

    Cette version bloque Airflow jusqu'à démarrage effectif, avec un délai de sécurité.
    """
    logger.info("🚀 Démarrage du job Spark Streaming Kafka → Delta Lake")

    cmd = [
        "docker", "exec", "-i", SPARK_CONTAINER_NAME,
        "spark-submit", SPARK_STREAMING_SCRIPT
    ]

    try:
        logger.info(f"🛠️ Commande exécutée : {' '.join(cmd)}")
        # Exécution bloquante (attend démarrage du job Spark)
        subprocess.run(cmd, check=True)
        logger.success("✅ Job Spark Streaming lancé avec succès (mode bloquant)")
    except Exception as e:
        logger.error(f"❌ Erreur lors du lancement Spark Streaming : {e}")
        raise

    # Attente explicite pour laisser Spark initialiser le stream Kafka
    logger.info("⏳ Pause de sécurité pour laisser Spark démarrer (10 secondes)...")
    time.sleep(10)
    logger.info("🟢 Spark est supposé être opérationnel. Suite du pipeline possible.")

# ======================================================================================
# 3. Point d’entrée direct (utilisation CLI possible)
# ======================================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Erreur fatale lors de l’exécution du script : {e}")
        raise
