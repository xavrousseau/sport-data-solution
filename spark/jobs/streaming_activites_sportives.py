# ==========================================================================================
# Script      : streaming_activites_sportives.py
# Objectif    : Lire les activités sportives depuis Kafka (Debezium),
#               enrichir le message, afficher une synthèse lisible,
#               envoyer une notification NTFY pour chaque activité,
#               et éventuellement écrire en Delta Lake (facultatif).
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, to_timestamp, lit
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType
import requests
import os
from dotenv import load_dotenv
from faker import Faker
from random import choice

# ==========================================================================================
# Chargement des variables d’environnement
# ==========================================================================================
load_dotenv(dotenv_path=".env", override=True)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdata.sportdata.activites_sportives")
NTFY_URL = os.getenv("NTFY_URL", "http://sport-ntfy")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "sportdata_activites")

# ==========================================================================================
# Lieux et emojis (pour enrichir les messages)
# ==========================================================================================
LIEUX_POPULAIRES = [
    "au parc de la Penfeld", "le long du Lez", "au parc du Thabor",
    "près du pont de Rohan", "à la plage du Prado", "dans les bois de Vincennes"
]

EMOJIS = ["🔥", "💪", "🎯", "😅", "🏃‍♀️", "🚴", "🌟", "🏋️", "🎉"]

fake = Faker(locale="fr_FR")

# ==========================================================================================
# Fonction : envoyer une notification NTFY
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
    except Exception as e:
        print(f"Erreur NTFY : {e}")

# ==========================================================================================
# Fonction appelée dans le foreachBatch
# ==========================================================================================
def traiter_batch(df, epoch_id):
    print(f"\n[Batch {epoch_id}] Réception d’un microbatch avec {df.count()} ligne(s)")

    if df.isEmpty():
        print("[INFO] Microbatch vide. Rien à traiter.")
        return

    lignes = df.select("prenom", "type_activite", "distance_km", "temps_sec").collect()
    for row in lignes:
        envoyer_message_ntfy(row.prenom, row.type_activite, row.distance_km, row.temps_sec // 60)

    print("\n=== Activités traitées ===")
    df.select("prenom", "type_activite", "distance_km", "temps_sec").show(truncate=False)

# ==========================================================================================
# Schéma de l’activité (contenu du champ "after")
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
# Session Spark
# ==========================================================================================
spark = SparkSession.builder \
    .appName("StreamingActivitesSportives") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# Lecture du flux Kafka (Debezium → JSON avec champ "after")
# ==========================================================================================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parsing du champ "after"
df_valeurs = kafka_df.selectExpr("CAST(value AS STRING)")

df_json = df_valeurs.select(from_json(col("value"), StructType().add("after", StringType())).alias("data"))
df_activites = df_json.select(from_json(col("data.after"), schema).alias("activite")).select("activite.*")

# ==========================================================================================
# Déclenchement traitement + notifications via foreachBatch
# ==========================================================================================
query = df_activites.writeStream \
    .foreachBatch(traiter_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()
