# ======================================================================================
# Script      : silver_aggregations.py
# Objectif    : Agréger les données sportives par salarié pour usage Power BI,
#               avec export CSV vers MinIO et PostgreSQL + KPI + rapport HTML GE
# Auteur      : Xavier Rousseau | Version enrichie avec rapport GE - Juillet 2025
# ======================================================================================

import os
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, min, max, to_date, expr, weekofyear
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine
import great_expectations as ge
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView
from datetime import datetime

# ======================================================================================
# 1. Variables d’environnement
# ======================================================================================
load_dotenv(dotenv_path=".env")

MINIO_HOST = os.getenv("MINIO_HOST", "sport-minio")
MINIO_ENDPOINT = f"{MINIO_HOST}:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

DELTA_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://datalake/bronze/activites_sportives/")
EXPORT_PATH = "/tmp/avantages_sportifs_export.csv"
EXPORT_MINIO_KEY = os.getenv("POWERBI_EXPORT_TABLE", "avantages_sportifs_export")
EXPORT_MINIO_OBJECT = f"exports/{EXPORT_MINIO_KEY}.csv"

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ======================================================================================
# 2. Pipeline principal
# ======================================================================================
def pipeline_aggregation_silver():
    logger.info("🚀 Initialisation SparkSession")

    spark = SparkSession.builder \
        .appName("Silver Aggregations - Power BI") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info(f"📥 Lecture Delta activités : {DELTA_PATH}")
        df = spark.read.format("delta").load(DELTA_PATH)
        df = df.withColumn("date", to_date(col("date")))
    except Exception as e:
        logger.error(f"❌ Erreur lecture Delta : {e}")
        spark.stop()
        raise

    try:
        df_rh = spark.read.format("delta").load("s3a://datalake/raw/donnees_rh_cleaned/")
        df = df.join(df_rh.select("id_salarie", "nom", "prenom"), on="id_salarie", how="left")
        logger.success("✅ Jointure RH effectuée")
    except Exception as e:
        logger.warning("⚠️ RH non disponibles : nom/prenom remplacés par '?'")
        df = df.withColumn("nom", expr("'?'")).withColumn("prenom", expr("'?'"))

    df = df.withColumn("semaine", weekofyear(col("date")))
    df_weeks = df.select("id_salarie", "semaine").distinct() \
        .groupBy("id_salarie").agg(countDistinct("semaine").alias("nb_semaines_actives"))

    agg = df.groupBy("id_salarie", "nom", "prenom").agg(
        count("*").alias("nb_activites"),
        countDistinct("date").alias("jours_bien_etre"),
        sum("distance_km").alias("distance_totale_km"),
        sum("temps_sec").alias("temps_total_sec"),
        min("date").alias("premiere_activite"),
        max("date").alias("derniere_activite")
    ).join(df_weeks, on="id_salarie", how="left")

    agg = agg \
        .withColumn("distance_km", expr("ROUND(distance_totale_km, 2)")) \
        .withColumn("temps_minutes", expr("ROUND(temps_total_sec / 60.0, 1)")) \
        .withColumn("frequence_moyenne_hebdo", expr("ROUND(nb_activites / nb_semaines_actives, 2)")) \
        .withColumn("prime_sportive_eur", expr("CASE WHEN jours_bien_etre >= 15 THEN 50 ELSE 0 END"))

    df_final = agg.select(
        "id_salarie", "nom", "prenom", "nb_activites", "jours_bien_etre",
        "nb_semaines_actives", "frequence_moyenne_hebdo",
        "distance_km", "temps_minutes",
        "premiere_activite", "derniere_activite", "prime_sportive_eur"
    )

    # Contrôle qualité GE + génération du rapport HTML
    logger.info("🔍 Contrôle qualité GE + rapport HTML")
    df_pd = df_final.toPandas()
    ge_df = ge.from_pandas(df_pd)

    ge_df.expect_column_values_to_not_be_null("id_salarie")
    ge_df.expect_column_values_to_be_between("frequence_moyenne_hebdo", 0, 14)
    ge_df.expect_column_values_to_be_between("distance_km", 0, 10000)
    ge_df.expect_column_values_to_be_between("temps_minutes", 0, 9999)

    result = ge_df.validate()
    if not result.success:
        logger.error("❌ Échec du contrôle qualité GE.")
        raise Exception("Great Expectations validation failed.")

    # Génération rapport HTML
    rendered = ValidationResultsPageRenderer().render(result)
    html = DefaultJinjaPageView().render(rendered)
    html_path = f"/tmp/rapport_ge_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    # Upload rapport dans MinIO
    rapport_key = f"exports/rapport_ge_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    try:
        minio = Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        minio.fput_object("datalake", rapport_key, html_path)
        logger.success(f"📄 Rapport GE HTML uploadé sur MinIO : {rapport_key}")
    except Exception as e:
        logger.warning(f"⚠️ Upload rapport GE échoué : {e}")

    # Export CSV et PostgreSQL
    try:
        df_pd.to_csv(EXPORT_PATH, index=False)
        logger.success(f"✅ Export CSV local : {EXPORT_PATH}")
        minio.fput_object("datalake", EXPORT_MINIO_OBJECT, EXPORT_PATH)
        logger.success(f"📤 Export CSV MinIO : {EXPORT_MINIO_OBJECT}")
        engine = create_engine(DB_CONN_STRING)
        df_pd.to_sql("avantages_sportifs", engine, schema="sportdata", if_exists="replace", index=False)
        logger.success("✅ Export PostgreSQL terminé")
    except Exception as e:
        logger.error(f"❌ Erreur export final : {e}")
    finally:
        spark.stop()

# ======================================================================================
# 3. Point d’entrée
# ======================================================================================
if __name__ == "__main__":
    pipeline_aggregation_silver()
