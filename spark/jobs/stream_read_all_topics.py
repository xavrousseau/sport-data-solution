from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("Stream Kafka depuis Debezium") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Liste des 3 topics à écouter
topics = ",".join([
    "sportdata.sportdata.employes",
    "sportdata.sportdata.activites_sportives",
    "sportdata.sportdata.beneficiaires_primes_sport"
])

# Serveur Kafka (Redpanda)
kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Lecture des messages Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", topics) \
    .option("startingOffsets", "latest") \
    .load()

# Affichage simplifié
messages = df.selectExpr(
    "CAST(topic AS STRING)",
    "CAST(key AS STRING)",
    "CAST(value AS STRING)",
    "timestamp"
)

# Écriture dans la console
query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
