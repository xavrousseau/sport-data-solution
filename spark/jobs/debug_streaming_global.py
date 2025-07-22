# lecture_flux_globaux.py
"""
Script Spark Structured Streaming pour lire tous les topics Kafka (Redpanda),
et afficher les flux entrants (clé, valeur, timestamp).

Usage :
- Debug global
- Visualisation temps réel
- Surveillance de tous les flux

Auteur : Xavier
Date : 2025-07
"""

from pyspark.sql import SparkSession
import os

# ------------------------------------------------------------------------------
# 1. Initialisation de SparkSession
# ------------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Lecture globale de tous les flux Redpanda") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------------------
# 2. Paramètres Kafka (Redpanda)
# ------------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")

# ------------------------------------------------------------------------------
# 3. Lecture de tous les topics disponibles
# ------------------------------------------------------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribePattern", ".*") \
    .option("startingOffsets", "latest") \
    .load()

# ------------------------------------------------------------------------------
# 4. Transformation : cast clé / valeur
# ------------------------------------------------------------------------------
df_cast = df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

# ------------------------------------------------------------------------------
# 5. Affichage en console
# ------------------------------------------------------------------------------
query = df_cast.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
