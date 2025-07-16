# ==========================================================================================
# Script      : ecriture_delta_activites.py
# Objectif    : Lire les activités sportives depuis Kafka et les stocker en Delta Lake
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType

# ==========================================================================================
# 1. Session Spark avec Delta Lake activé
# ==========================================================================================
spark = SparkSession.builder \
    .appName("Kafka → Delta - Activités sportives") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 2. Schéma attendu pour chaque message JSON
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
# 3. Lecture du flux Kafka
# ==========================================================================================
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "sport-redpanda:9092") \
    .option("subscribe", "sportdata_activites") \
    .option("startingOffsets", "earliest") \
    .load()

# ==========================================================================================
# 4. Parsing JSON → DataFrame structuré
# ==========================================================================================
df_activites = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*")

# ==========================================================================================
# 5. Écriture continue en Delta Lake (append)
# ==========================================================================================
delta_path = "/opt/bitnami/spark/delta/activites_sportives"

query = df_activites.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/delta_activites") \
    .start(delta_path)

query.awaitTermination()
