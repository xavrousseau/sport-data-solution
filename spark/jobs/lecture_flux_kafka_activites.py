# ==========================================================================================
# Script      : lecture_flux_kafka_activites.py
# Objectif    : Lire les messages Kafka contenant les activités sportives simulées
#               et les afficher dans la console (streaming)
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType

# ==========================================================================================
# 1. Création de la session Spark
# ==========================================================================================
spark = SparkSession.builder \
    .appName("Lecture flux activités sportives Kafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 2. Schéma JSON attendu dans les messages Kafka
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
# 3. Lecture du topic Kafka
# ==========================================================================================
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "sport-redpanda:9092") \
    .option("subscribe", "sportdata_activites") \
    .option("startingOffsets", "latest") \
    .load()

# ==========================================================================================
# 4. Transformation : JSON → colonnes structurées
# ==========================================================================================
df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*")

# ==========================================================================================
# 5. Affichage dans la console (streaming)
# ==========================================================================================
query = df_json.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .start()

query.awaitTermination()
