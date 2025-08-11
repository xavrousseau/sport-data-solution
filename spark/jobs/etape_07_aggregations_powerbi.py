# ======================================================================================
# Script      : etape_07_aggregations_powerbi.py
# Objectif    : Agréger les données sportives pour usage Power BI et PostgreSQL
#               - Jointure avec les données RH (si dispo)
#               - Calcul d’indicateurs hebdomadaires et totaux
#               - Contrôle qualité via Great Expectations
#               - Export vers MinIO (CSV) et PostgreSQL
# Auteur      : Xavier Rousseau | Août 2025
# ======================================================================================

import os
from dotenv import load_dotenv
from loguru import logger
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, min as spark_min, max as spark_max,
    to_date, expr, weekofyear
)
from great_expectations.dataset import PandasDataset
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView
from minio_helper import MinIOHelper
from ntfy_helper import envoyer_message_erreur

# ======================================================================================
# 1. Chargement des variables d’environnement (.env global)
# ======================================================================================

load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

DELTA_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/")
EXPORT_MINIO_OBJECT = "exports/avantages_sportifs.csv"
EXPORT_MINIO_REPORT = f"exports/rapport_ge_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ======================================================================================
# 2. Pipeline principal
# ======================================================================================

def main(**kwargs):
    logger.info("=== DÉMARRAGE AGRÉGATIONS POWER BI ===")
    helper = MinIOHelper()

    # Initialisation SparkSession
    spark = SparkSession.builder \
        .appName("Silver Aggregations - Power BI") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Lecture des activités bronze depuis Delta Lake sur MinIO
        logger.info(f"📅 Lecture des activités depuis Delta Lake (bronze) : {DELTA_PATH}")
        df = spark.read.format("delta").load(DELTA_PATH)
        df = df.withColumn("date", to_date(col("date")))

        # 2. Jointure optionnelle avec les données RH nettoyées
        try:
            logger.info("🔗 Tentative de jointure RH (s3a://sportdata/referentiels/donnees_rh_cleaned/)")
            df_rh = spark.read.format("delta").load("s3a://sportdata/referentiels/donnees_rh_cleaned/")
            df = df.join(df_rh.select("id_salarie", "nom", "prenom"), on="id_salarie", how="left")
            logger.success("✅ Jointure RH effectuée (nom, prénom ajoutés)")
        except Exception as exc:
            df = df.withColumn("nom", expr("'?'"))
            df = df.withColumn("prenom", expr("'?'"))
            logger.warning(f"⚠️ Données RH indisponibles ({exc}) : fallback '?' appliqué.")

        # 3. Calcul de la semaine d'activité (pour les fréquences)
        df = df.withColumn("semaine", weekofyear(col("date")))
        df_weeks = df.select("id_salarie", "semaine").distinct() \
            .groupBy("id_salarie").agg(countDistinct("semaine").alias("nb_semaines_actives"))

        # 4. Agrégations par salarié
        agg = df.groupBy("id_salarie", "nom", "prenom").agg(
            count("*").alias("nb_activites"),
            countDistinct("date").alias("jours_bien_etre"),
            spark_sum("distance_km").alias("distance_totale_km"),
            spark_sum("temps_sec").alias("temps_total_sec"),
            spark_min("date").alias("premiere_activite"),
            spark_max("date").alias("derniere_activite")
        ).join(df_weeks, on="id_salarie", how="left")

        df_final = agg \
            .withColumn("distance_km", expr("ROUND(distance_totale_km, 2)")) \
            .withColumn("temps_minutes", expr("ROUND(temps_total_sec / 60.0, 1)")) \
            .withColumn("frequence_moyenne_hebdo", expr("ROUND(nb_activites / nb_semaines_actives, 2)")) \
            .withColumn("prime_sportive_eur", expr("CASE WHEN jours_bien_etre >= 15 THEN 50 ELSE 0 END")) \
            .select(
                "id_salarie", "nom", "prenom",
                "nb_activites", "jours_bien_etre", "nb_semaines_actives",
                "frequence_moyenne_hebdo", "distance_km", "temps_minutes",
                "premiere_activite", "derniere_activite", "prime_sportive_eur"
            )

        # 5. Contrôle qualité sur l'agrégation (Great Expectations)
        logger.info("🔍 Contrôle qualité Great Expectations")
        df_pd = df_final.toPandas()
        ge_df = PandasDataset(df_pd)

        ge_df.expect_column_values_to_not_be_null("id_salarie")
        ge_df.expect_column_values_to_be_between("frequence_moyenne_hebdo", 0, 14)
        ge_df.expect_column_values_to_be_between("distance_km", 0, 10000)
        ge_df.expect_column_values_to_be_between("temps_minutes", 0, 9999)

        result = ge_df.validate()
        if not result.success:
            raise Exception("❌ Échec de validation Great Expectations")

        # 6. Export du rapport GE sur MinIO
        rendered = ValidationResultsPageRenderer().render(result)
        html = DefaultJinjaPageView().render(rendered)
        helper.upload_object(EXPORT_MINIO_REPORT, html.encode("utf-8"), content_type="text/html")
        logger.success(f"📄 Rapport GE envoyé : {EXPORT_MINIO_REPORT}")
        rapport_url = f"http://localhost:9001/browser/sportdata/{EXPORT_MINIO_REPORT.replace('/', '%2F')}"
        logger.info(f"🔗 Rapport GE disponible : {rapport_url}")

        # 7. Export CSV MinIO pour Power BI
        helper.upload_object(EXPORT_MINIO_OBJECT, df_pd.to_csv(index=False).encode("utf-8"))
        logger.success(f"📃 Export CSV MinIO : {EXPORT_MINIO_OBJECT}")

        # 8. Export PostgreSQL (table sportdata.avantages_sportifs)
        engine = create_engine(DB_CONN_STRING)
        df_pd.to_sql("avantages_sportifs", engine, schema="sportdata", if_exists="replace", index=False)
        logger.success("📈 Table PostgreSQL : sportdata.avantages_sportifs")

        logger.success("🌟 Agrégation et export terminés.")

    except Exception as e:
        logger.error(f"❌ Erreur agrégation Power BI : {e}")
        envoyer_message_erreur("avantages_sportifs", f"🚨 Échec agrégation Power BI\n{e}")
        raise
    finally:
        spark.stop()

# ======================================================================================
# 3. Exécution directe
# ======================================================================================

if __name__ == "__main__":
    main()
