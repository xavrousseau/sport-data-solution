# ==========================================================================================
# Script      : notifications_activites_sportives.py
# Objectif    : Lecture temps réel du topic Kafka 'sportdata.activites'
#               et envoi de notifications ntfy pour chaque activité sportive reçue.
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import requests
import os
from dotenv import load_dotenv

# ==========================================================================================
# 1. Chargement configuration ntfy (via .env ou valeurs par défaut)
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

NTFY_URL = os.getenv("NTFY_URL", "http://localhost:8080")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata_activites")

# ==========================================================================================
# 2. Fonction d'envoi d'une notification
# ==========================================================================================
def envoyer_notification(prenom, activite, distance_km, temps_min, commentaire=None):
    if commentaire:
        message = f"🔥 Bravo {prenom} ! Tu viens de faire {distance_km:.1f} km de {activite.lower()} en {temps_min} min — {commentaire}"
    else:
        message = f"🔥 Bravo {prenom} ! Tu viens de faire {distance_km:.1f} km de {activite.lower()} en {temps_min} min. 💪"

    try:
        requests.post(f"{NTFY_URL}/{NTFY_TOPIC}", data=message.encode("utf-8"))
        print(f"✅ Notification envoyée : {message}")
    except Exception as e:
        print(f"Erreur notification ntfy : {e}")

# ==========================================================================================
# 3. Schéma des messages JSON dans le topic Kafka
# ==========================================================================================
message_schema = StructType([
    StructField("uid", StringType()),
    StructField("id_salarie", StringType()),
    StructField("nom", StringType()),
    StructField("prenom", StringType()),
    StructField("type_activite", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("temps_sec", IntegerType()),
    StructField("commentaire", StringType()),
    StructField("date", TimestampType()),
    StructField("jour", StringType())
])

# ==========================================================================================
# 4. Initialisation de Spark Streaming (mode console ou production)
# ==========================================================================================
spark = SparkSession.builder \
    .appName("NotificationsActivitesSportives") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 5. Lecture du topic Kafka
# ==========================================================================================
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "sport-redpanda:9092") \
    .option("subscribe", "sportdata.activites") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir les valeurs JSON en colonnes
json_df = df_kafka.selectExpr("CAST(value AS STRING) as json") \
    .withColumn("data", from_json(col("json"), message_schema)) \
    .select("data.*")

# ==========================================================================================
# 6. Traitement par foreachBatch (envoi notification)
# ==========================================================================================
def notifier_activites(batch_df, batch_id):
    rows = batch_df.collect()
    for row in rows:
        try:
            envoyer_notification(
                prenom=row.prenom,
                activite=row.type_activite,
                distance_km=row.distance_km,
                temps_min=int(row.temps_sec // 60),
                commentaire=row.commentaire
            )
        except Exception as e:
            print(f"❌ Erreur ligne batch {batch_id} : {e}")

# ==========================================================================================
# 7. Lancement du streaming avec foreachBatch
# ==========================================================================================
query = json_df.writeStream \
    .foreachBatch(notifier_activites) \
    .outputMode("append") \
    .start()

query.awaitTermination()

# ==========================================================================================
# Fin du fichier – Notifications temps réel à partir du topic Kafka sportdata.activites
# ==========================================================================================
