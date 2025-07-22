# lecture_flux_activites.py
"""
Lecture en continu du topic Kafka 'sportdata_activites'.
Parse les messages JSON et affiche les champs utiles.

Objectif : déboguer et préparer le traitement temps réel des activités sportives.

Auteur : Xavier Rousseau — Juillet 2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType
import os

# ------------------------------------------------------------------------------
# 1. SparkSession
# ------------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Lecture flux activités sportives") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------------------
# 2. Paramètres Kafka / Redpanda
# ------------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
TOPIC = "sportdata_activites"

# ------------------------------------------------------------------------------
# 3. Schéma attendu pour le parsing JSON (doit correspondre à l'objet produit par Debezium)
# ------------------------------------------------------------------------------
schema_payload = StructType() \
    .add("id_salarie", StringType()) \
    .add("type_activite", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("temps_sec", IntegerType()) \
    .add("date", TimestampType()) \
    .add("commentaire", StringType())

schema_wrapper = StructType().add("payload", schema_payload)

# ------------------------------------------------------------------------------
# 4. Lecture streaming du topic
# ------------------------------------------------------------------------------
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# ------------------------------------------------------------------------------
# 5. Parsing JSON des valeurs
# ------------------------------------------------------------------------------
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("parsed", from_json(col("json_str"), schema_wrapper)) \
    .select("parsed.payload.*")

# ------------------------------------------------------------------------------
# 6. Affichage en console
# ------------------------------------------------------------------------------
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
