# stream_read_employes.py
"""
Script Spark Structured Streaming pour lire les messages Kafka (Redpanda)
issus de Debezium (changements sur la table sportdata.employes).

Étapes :
1. Créer la session Spark
2. Se connecter à Redpanda (Kafka)
3. Lire les messages du topic Debezium
4. Caster les clés et valeurs (JSON)
5. Afficher en console (debug/test)

Auteur : Xavier
Date : 2025-07
"""

from pyspark.sql import SparkSession
import os

# ------------------------------------------------------------------------------
# 1. Initialisation de SparkSession
# ------------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Lecture des employés depuis Debezium / Redpanda") \
    .getOrCreate()

# Réduire le niveau de log (évite le spam dans la console)
spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------------------
# 2. Paramètres Kafka (Redpanda)
# ------------------------------------------------------------------------------
# ➤ Nom du topic Kafka créé par Debezium
topic = "dbserver1.sportdata.employes"

# ➤ Adresse du broker Redpanda (via Docker network)
kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

# ------------------------------------------------------------------------------
# 3. Lecture en streaming du topic Kafka
# ------------------------------------------------------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# ------------------------------------------------------------------------------
# 4. Transformation des messages
# ------------------------------------------------------------------------------
# Les colonnes 'key' et 'value' sont des bytes -> on les convertit en string
valeurs = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# ------------------------------------------------------------------------------
# 5. Affichage des messages en streaming dans la console
# ------------------------------------------------------------------------------
query = valeurs.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
