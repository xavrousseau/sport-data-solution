# ==========================================================================================
# Script      : bronze_controle_qualite.py (version enrichie et robuste)
# Objectif    : Contrôle qualité du flux bronze : ingestion, parsing, validation, stockage Delta Lake (MinIO), notification NTFY
# Auteur      : Xavier Rousseau | Août 2025
# ==========================================================================================

import os
import sys
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

# Ajout du chemin helper NTFY
sys.path.append("/opt/airflow/scripts")
from ntfy_helper import envoyer_message_ntfy

# ==========================================================================================
# 1. Chargement des variables d’environnement (.env)
# ==========================================================================================

load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata.sportdata.activites_sportives")
DELTA_PATH_ACTIVITES = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/")
CHECKPOINT_PATH_DELTA = os.getenv("CHECKPOINT_PATH_DELTA", "/tmp/checkpoints/bronze_activites_sportives")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

# ==========================================================================================
# 2. Schéma structuré du flux d’activités
# ==========================================================================================

schema = StructType([
    StructField("uid", StringType()),
    StructField("id_salarie", LongType()),
    StructField("nom", StringType()),
    StructField("prenom", StringType()),
    StructField("date", StringType()),
    StructField("jour", StringType()),
    StructField("date_debut", StringType()),  # sera converti ensuite en timestamp
    StructField("type_activite", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("temps_sec", IntegerType()),
    StructField("commentaire", StringType())
])

# ==========================================================================================
# 3. Initialisation SparkSession (Delta Lake + S3A/MinIO)
# ==========================================================================================

spark = SparkSession.builder \
    .appName("BronzeControleQualite") \
    .config("spark.jars.packages", ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        "io.delta:delta-core_2.12:2.4.0",
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    ])) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("✅ SparkSession initialisée.")

# ==========================================================================================
# 4. Fonction de validation et d’écriture du micro-batch
# ==========================================================================================

def process_batch(df, epoch_id):
    """
    Fonction exécutée à chaque micro-batch.
    Valide les données, enregistre en Delta Lake et envoie les notifications NTFY.
    """
    count = df.count()
    if count == 0:
        logger.info(f"⚠️ Micro-batch {epoch_id} vide — aucune donnée reçue.")
        return

    logger.info(f"📦 Micro-batch #{epoch_id} — {count} activité(s) reçue(s).")

    # Contrôle qualité simple : aucune valeur nulle sur les champs essentiels
    champs_essentiels = ["uid", "id_salarie", "prenom", "type_activite", "distance_km", "temps_sec", "date_debut"]
    df_valide = df.dropna(subset=champs_essentiels)

    nb_invalide = count - df_valide.count()
    if nb_invalide > 0:
        logger.warning(f"⚠️ {nb_invalide} activité(s) supprimée(s) (valeurs manquantes sur colonnes critiques).")

    # Conversion explicite date_debut en timestamp
    df_valide = df_valide.withColumn("date_debut", to_timestamp("date_debut"))

    # Ecriture append dans Delta Lake (MinIO)
    df_valide.write.format("delta").mode("append").save(DELTA_PATH_ACTIVITES)
    logger.success(f"✅ {df_valide.count()} activité(s) validée(s) et stockée(s) dans Delta Lake.")

    # Envoi des notifications NTFY (attention : .collect() à scaler si petits batchs)
    rows = df_valide.select("prenom", "type_activite", "distance_km", "temps_sec").collect()
    for row in rows:
        try:
            prenom = row["prenom"]
            sport = row["type_activite"]
            km = row["distance_km"]
            minutes = int(row["temps_sec"] / 60) if row["temps_sec"] else 0
            envoyer_message_ntfy(prenom, sport, km, minutes)
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors de l’envoi de notification pour {row['prenom']} : {e}")

# ==========================================================================================
# 5. Lecture du flux Kafka, parsing JSON, conversion date
# ==========================================================================================

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

df_valide = df_kafka.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# ==========================================================================================
# 6. Lancement du streaming en mode append + trigger processing-time
# ==========================================================================================

logger.info("🚀 Démarrage du streaming Kafka → Delta Lake + Contrôle Qualité...")

query = df_valide.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH_DELTA) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
