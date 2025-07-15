# ==========================================================================================
# Script      : controle_qualite_activites.py
# Objectif    : Contrôles qualité simples sur les activités sportives Delta (MinIO),
#               avec export des erreurs et notification ntfy.
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from dotenv import load_dotenv
import requests

# ==========================================================================================
# 1. Chargement .env et configuration S3 (MinIO)
# ==========================================================================================
load_dotenv(dotenv_path=".env")

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_ENDPOINT = MINIO_HOST + ":9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
DELTA_INPUT_PATH = "s3a://datalake/bronze/activites_sportives/"
NTFY_URL = os.getenv("NTFY_URL", f"http://localhost:8080/qualite")

spark = SparkSession.builder \
    .appName("Contrôle Qualité Activités Sportives") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================================================
# 2. Chargement du fichier Delta
# ==========================================================================================
try:
    df = spark.read.format("delta").load(DELTA_INPUT_PATH)
    print("✅ Données chargées depuis MinIO")
except Exception as e:
    print(f"❌ Erreur lecture Delta : {e}")
    spark.stop()
    exit(1)

# ==========================================================================================
# 3. Définition des règles de qualité
# ==========================================================================================
regles = [
    ("distance_m > 0", col("distance_m") > 0),
    ("temps_s > 0", col("temps_s") > 0),
    ("sport_type NOT NULL", col("sport_type").isNotNull()),
    ("id_salarie NOT NULL", col("id_salarie").isNotNull())
]

# ==========================================================================================
# 4. Évaluation des règles
# ==========================================================================================
df_valide = df
df_erreurs = spark.createDataFrame([], df.schema)

for nom_regle, condition in regles:
    violations = df_valide.filter(~condition)
    count = violations.count()
    if count > 0:
        print(f"❌ {count} violation(s) : {nom_regle}")
        df_erreurs = df_erreurs.union(violations)
        df_valide = df_valide.filter(condition)
    else:
        print(f"✅ Règle validée : {nom_regle}")

nb_valides = df_valide.count()
nb_erreurs = df_erreurs.count()

# ==========================================================================================
# 5. Résumé & export des erreurs dans MinIO
# ==========================================================================================
print("\n📊 Résumé du contrôle :")
print(f"✔️  Lignes valides   : {nb_valides}")
print(f"❌ Lignes en erreur : {nb_erreurs}")

if nb_erreurs > 0:
    print("📤 Export des erreurs en cours…")
    path_tmp = "/tmp/erreurs_qualite_activites.xlsx"
    df_erreurs.toPandas().to_excel(path_tmp, index=False)

    from minio import Minio
    client = Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    client.fput_object(
        bucket_name="datalake",
        object_name="exports/erreurs_qualite_activites.xlsx",
        file_path=path_tmp
    )
    print("✅ Fichier exporté dans MinIO : exports/erreurs_qualite_activites.xlsx")

# ==========================================================================================
# 6. Notification ntfy automatique
# ==========================================================================================
try:
    if nb_erreurs > 0:
        msg = f"❌ Contrôle qualité : {nb_erreurs} erreur(s) détectée(s) dans les activités sportives."
    else:
        msg = f"✅ Contrôle qualité OK : {nb_valides} lignes valides."
    requests.post(NTFY_URL, data=msg.encode("utf-8"))
    print(f"🔔 Message ntfy envoyé : {msg}")
except Exception as e:
    print(f"⚠️ Erreur envoi ntfy : {e}")

print("\n🔍 Exemples d'erreurs (max 10) :")
df_erreurs.show(10, truncate=False)

spark.stop()
