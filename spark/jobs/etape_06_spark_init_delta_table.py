# ==========================================================================================
# Script      : etape_06_init_delta_table.py
# Objectif    : Initialiser la table Delta Lake (bronze/activites_sportives)
#               - détection existence (pas d'overwrite si déjà créée)
#               - enregistrement metastore
#               - propriétés Delta utiles
#               - partitionnement par 'jour'
#               - timezone UTC
#               - read-back check
#               - endpoint MinIO propre + SSL auto
# Auteur      : Xavier Rousseau | Août 2025
# ==========================================================================================

from delta.tables import DeltaTable

import os
import sys
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
# ==============================================================================
# 1) ENV (safe) + logging sans fuite
# ==============================================================================
SENSITIVE_TOKENS = ("PASSWORD", "SECRET", "TOKEN", "KEY", "ACCESS")
def _no_secret_logs(record):
    msg = record["message"]
    return not any(tok in msg.upper() for tok in SENSITIVE_TOKENS)

def mask(s: str, keep=2) -> str:
    if not s:
        return ""
    return s[:keep] + "***" if len(s) > keep else "***"

logger.remove()
logger.add(sys.stdout, level="INFO", filter=_no_secret_logs)

try:
    load_dotenv("/opt/airflow/.env", override=True)
except Exception:
    pass
load_dotenv(".env", override=True)

# Emplacements & noms
DELTA_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/activites_sportives")
DELTA_TABLE_NAME = os.getenv("DELTA_TABLE_ACTIVITES", "bronze_activites_sportives")  # ident SQL valide

# MinIO (S3A) — pas de valeurs par défaut dangereuses
MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", ""))
MINIO_SECRET_KEY   = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", ""))

# Nettoyage endpoint + SSL auto
ssl_enabled   = MINIO_ENDPOINT_RAW.lower().startswith("https://")
MINIO_ENDPOINT = MINIO_ENDPOINT_RAW.replace("https://", "").replace("http://", "")

# (Optionnel) En prod, empêcher le run sans credentials :
# if not (MINIO_ACCESS_KEY and MINIO_SECRET_KEY):
#     raise RuntimeError("MINIO credentials are missing.")

# ==============================================================================
# 2) SparkSession + config S3A/Delta
# ==============================================================================
spark = (
    SparkSession.builder
    .appName("init_delta_activites_sportives")
    # Timezone verrouillée
    .config("spark.sql.session.timeZone", "Europe/Paris")
    # Delta Lake
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # S3A ↔ MinIO
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    # Delta log store S3
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    # Fichiers
    .config("spark.sql.files.maxRecordsPerFile", "20000")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Logs d’état (sans secrets)
logger.info(f"➡️  DELTA_PATH = {DELTA_PATH}")
logger.info(f"➡️  DELTA_TABLE_NAME = {DELTA_TABLE_NAME}")
logger.info(f"➡️  MINIO_ENDPOINT = {MINIO_ENDPOINT} (ssl={ssl_enabled}) | access={mask(MINIO_ACCESS_KEY)} | secret={mask(MINIO_SECRET_KEY)}")

# ==========================================================================================
# 3) Schéma bronze (aligné sur la production d'événements Debezium-like)
# ==========================================================================================

# ==========================================================================================
# 3) Schéma bronze (aligné sur la production d'événements Debezium-like)
#    - id_salarie en StringType (cohérent Kafka/Postgres)
#    - ajout de 'mois' (yyyy-MM) pour le partitionnement
# ==========================================================================================

schema = StructType([
    StructField("uid",            StringType(),    True),
    StructField("id_salarie",     StringType(),    True),   # ← était LongType
    StructField("prenom",         StringType(),    True),
    StructField("nom",            StringType(),    True),
    StructField("type_activite",  StringType(),    True),
    StructField("distance_km",    DoubleType(),    True),
    StructField("temps_sec",      IntegerType(),   True),
    StructField("date_debut",     TimestampType(), True),   # clé : timestamp
    StructField("commentaire",    StringType(),    True),
    StructField("profil",         StringType(),    True),
    StructField("date",           StringType(),    True),
    StructField("jour",           StringType(),    True),   # yyyy-MM-dd (info)
    StructField("mois",           StringType(),    True),   # yyyy-MM (PARTITION)
])

# ==========================================================================================
# 4) Existence Delta & création si nécessaire
# ==========================================================================================

def delta_exists(path: str) -> bool:
    try:
        DeltaTable.forPath(spark, path)
        return True
    except AnalysisException:
        return False

# DataFrame vide au bon schéma
df_vide = spark.createDataFrame([], schema)

created = False
if not delta_exists(DELTA_PATH):
    logger.warning("📁 Table Delta absente — initialisation (écriture initiale, partition='mois')…")
    (
        df_vide.write
        .format("delta")
        .mode("overwrite")            # première init uniquement
        .option("overwriteSchema", "true")
        .partitionBy("mois")          # ← était 'jour'
        .save(DELTA_PATH)
    )
    created = True
    logger.success("✅ Table Delta créée (schéma bronze/activites_sportives, partition='mois').")
else:
    logger.info("✔️  Table Delta déjà existante — aucune écriture destructive.")


# ==========================================================================================
# 5) Enregistrement dans le metastore Hive (pointer LOCATION)
# ==========================================================================================

# DELTA_TABLE_NAME peut inclure un schema Hive (ex: bronze.activites_sportives)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DELTA_TABLE_NAME}
    USING DELTA
    LOCATION '{DELTA_PATH}'
""")
logger.success(f"🗃️ Table metastore prête : {DELTA_TABLE_NAME} → {DELTA_PATH}")

# ==========================================================================================
# 6) Propriétés Delta utiles (non bloquant si version Delta plus ancienne)
# ==========================================================================================

try:
    spark.sql(f"""
      ALTER TABLE {DELTA_TABLE_NAME} SET TBLPROPERTIES (
        delta.columnMapping.mode = 'name',
        delta.minReaderVersion = '2',
        delta.minWriterVersion = '5',
        delta.logRetentionDuration = '7 days',
        delta.deletedFileRetentionDuration = '7 days',
        delta.enableChangeDataFeed = 'false'
      )
    """)
    # Contraintes simples (attraper les valeurs négatives aberrantes)
    spark.sql(f"ALTER TABLE {DELTA_TABLE_NAME} SET TBLPROPERTIES (delta.constraints.distance_non_negative = 'distance_km >= 0')")
    spark.sql(f"ALTER TABLE {DELTA_TABLE_NAME} SET TBLPROPERTIES (delta.constraints.temps_non_negative = 'temps_sec >= 0')")
    logger.success("🔧 Propriétés Delta & contraintes définies.")
except Exception as e:
    logger.warning(f"Propriétés/contraintes Delta ignorées (non critique) : {e}")

# ==========================================================================================
# 7) Read-back check (sanity)
# ==========================================================================================

try:
    df_check = spark.read.format("delta").load(DELTA_PATH)
    logger.info("🔎 Schéma effectif (dtypes) : " + str(df_check.dtypes))
    logger.info("🔎 Partitioning : 'mois' (string, yyyy-MM)")
except Exception as e:
    logger.warning(f"Read-back check ignoré : {e}")

# ==========================================================================================
# 8) Fin
# ==========================================================================================

spark.stop()
logger.success("🏁 Initialisation Delta terminée.")
