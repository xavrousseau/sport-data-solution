# ==========================================================================================
# Script      : streaming_activites_sportives.py
# Objectif    : Lire les activités sportives depuis Kafka (Debezium),
#               envoyer des messages ntfy enrichis pour chaque activité,
#               et stocker les activités dans Delta Lake (bronze).
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import requests
import os
from dotenv import load_dotenv
from faker import Faker
from random import choice

# ==========================================================================================
# 1. Chargement des variables d’environnement
# ==========================================================================================

load_dotenv(dotenv_path=".env", override=True)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata.sportdata.activites_sportives")
NTFY_URL = os.getenv("NTFY_URL", "http://sport-ntfy")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata_activites")

# ==========================================================================================
# 2. Lieux et emojis (pour enrichir les messages)
# ==========================================================================================

LIEUX_POPULAIRES = [
    "au parc de la Penfeld", "le long du Lez", "au parc du Thabor",
    "près du pont de Rohan", "à la plage du Prado", "dans les bois de Vincennes"
]

EMOJIS = ["🔥", "💪", "🎯", "😅", "🏃‍♀️", "🚴", "🌟", "🏋️", "🎉"]
fake = Faker(locale="fr_FR")

# ==========================================================================================
# 3. Fonction : envoyer une notification NTFY enrichie
# ==========================================================================================

def envoyer_message_ntfy(prenom, sport, km, minutes):
    lieu = choice(LIEUX_POPULAIRES)
    emoji = choice(EMOJIS)
    commentaires = [
        f"{emoji} Bravo {prenom} ! Tu viens de faire {km:.1f} km de {sport.lower()} en {minutes} min {lieu}",
        f"{emoji} {prenom} a bien transpiré : {km:.1f} km en {minutes} minutes {lieu}",
        f"{emoji} {prenom} s’est donné à fond en {sport.lower()} {lieu} ({minutes} min)"
    ]
    message = choice(commentaires)
    try:
        requests.post(f"{NTFY_URL}/{NTFY_TOPIC}", data=message.encode("utf-8"))
        print("🔔 Notification envoyée :", message)
    except Exception as e:
        print(f"❌ Erreur NTFY : {e}")

# ==========================================================================================
# 4. Fonction foreachBatch pour traiter et notifier chaque activité
# ==========================================================================================

def traiter_batch(df, epoch_id):
    if df.isEmpty():
        print(f"[Batch {epoch_id}] Aucun événement à traiter.")
        return

    print(f"[Batch {epoch_id}] Activités reçues : {df.count()}")

    lignes = df.select("prenom", "type_activite", "distance_km", "temps_sec").collect()
    for row in lignes:
        envoyer_message_ntfy(row.prenom, row.type_activite, row.distance_km, row.temps_sec // 60)

    print("=== Activités notifiées ===")
    df.select("prenom", "type_activite", "distance_km", "temps_sec").show(truncate=False)

# ==========================================================================================
# 5. Schéma de l'activité (payload.after déjà "flattened" par Debezium)
# ==========================================================================================

schema = StructType() \
    .add("uid", StringType()) \
    .add("id_salarie", StringType()) \
    .add("nom", StringType()) \
    .add("prenom", StringType()) \
    .add("date", StringType()) \
    .add("jour", StringType()) \
    .add("type_activite", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("temps_sec", IntegerType()) \
    .add("commentaire", StringType())

# ==========================================================================================
# 6. Initialisation SparkSession
# ==========================================================================================

spark = SparkSession.builder \
    .appName("StreamingActivitesSportives") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_root_user") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_root_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 7. Lecture du flux Kafka (Debezium → JSON avec champ "after")
# ==========================================================================================

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

df_valeurs = kafka_df.selectExpr("CAST(value AS STRING)")

df_json = df_valeurs.select(from_json(col("value"), StructType().add("after", StringType())).alias("data"))
df_activites = df_json.select(from_json(col("data.after"), schema).alias("activite")).select("activite.*")

# ==========================================================================================
# 8. Écriture 1 : notifications ntfy via foreachBatch
# ==========================================================================================

query_ntfy = df_activites.writeStream \
    .foreachBatch(traiter_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/ntfy_activites") \
    .start()

# ==========================================================================================
# 9. Écriture 2 : Delta Lake (bronze) sur MinIO
# ==========================================================================================

df_activites.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("path", "s3a://datalake/bronze/activites_sportives") \
    .option("checkpointLocation", "/tmp/checkpoints/bronze_activites_sportives") \
    .start()

# ==========================================================================================
# 10. Attente de fin
# ==========================================================================================

query_ntfy.awaitTermination()
