# ======================================================================================
# Script      : etape_10_spark_gold_aggregations_powerbi.py (version robuste + primes/JBE)
# Objet       : Silver/Gold sur activités Δ (MinIO) + enrichissement primes/JBE + export CSV/PG
# Auteur      : Xavier Rousseau |  Août 2025
# ======================================================================================

import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, min as spark_min, max as spark_max,
    to_date, to_timestamp, weekofyear, year, month, date_format, coalesce, lit, when,
    lower, trim, round as spark_round
)
from pyspark.sql.utils import AnalysisException

# Great Expectations (optionnel)
HAS_GE = True
try:
    from great_expectations.dataset import PandasDataset
    from great_expectations.render.renderer import ValidationResultsPageRenderer
    from great_expectations.render.view import DefaultJinjaPageView
except Exception:
    HAS_GE = False

# Helpers (fournis par ton repo)
sys.path.append("/opt/airflow/scripts")
from minio_helper import MinIOHelper  # type: ignore
from ntfy_helper import envoyer_message_erreur  # type: ignore

# ======================================================================================
# 1) ENV & logs
# ======================================================================================

logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda r: "PASSWORD" not in r["message"].upper())

# Chargement .env (Airflow puis repo local)
try:
    load_dotenv("/opt/airflow/.env", override=True)
except Exception:
    pass
load_dotenv(".env", override=True)

# Δ bronze (activités) — **table correcte, pas la racine /bronze/**
DELTA_BRONZE_PATH = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/activites_sportives")

# Δ bronze (primes/JBE)
DELTA_PRIMES_JBE_PATH = os.getenv("DELTA_BASE_PRIMES_JBE", "s3a://sportdata/bronze_primes_jbe")

# MinIO endpoint (sans affichage des creds)
MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS       = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", ""))
MINIO_SECRET       = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", ""))
ssl_enabled        = MINIO_ENDPOINT_RAW.lower().startswith("https://")
MINIO_ENDPOINT     = MINIO_ENDPOINT_RAW.replace("https://", "").replace("http://", "")

# Δ outputs
SILVER_PATH        = os.getenv("DELTA_SILVER_ACTIVITES", "s3a://sportdata/silver/activites_sportives")
GOLD_WEEKLY_PATH   = os.getenv("DELTA_GOLD_HEBDO",      "s3a://sportdata/gold/agg_hebdo")
GOLD_MONTHLY_PATH  = os.getenv("DELTA_GOLD_MENSUEL",    "s3a://sportdata/gold/agg_mensuel")
GOLD_KPI_PATH      = os.getenv("DELTA_GOLD_INDICATEURS","s3a://sportdata/gold/indicateurs")

# Exports MinIO
EXPORT_MINIO_OBJECT = os.getenv("EXPORT_MINIO_CSV", "exports/avantages_sportifs.csv")
EXPORT_MINIO_REPORT = os.getenv(
    "EXPORT_MINIO_GE_REPORT",
    f"exports/rapport_ge_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
)

# PostgreSQL (masqué dans la chaîne log)
POSTGRES_USER     = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "sport-postgres")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "sportdata")
DB_CONN_MASKED    = f"postgresql://{POSTGRES_USER}:{'***' if POSTGRES_PASSWORD else ''}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
RAW_DB_CONN_STR   = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# RH optionnel
DELTA_RH_PATH_OPT = os.getenv("DELTA_RH_CLEAN_PATH", "s3a://sportdata/referentiels/donnees_rh_cleaned/")

# Tuning & options
SHUFFLE_PARTS     = int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
COALESCE_N        = int(os.getenv("DELTA_COALESCE_TARGET", "1"))
MAX_RECORDS_FILE  = int(os.getenv("DELTA_MAX_RECORDS_PER_FILE", "500000"))
GE_STRICT_FAIL    = os.getenv("GE_STRICT_FAIL", "true").lower() in {"1", "true", "yes"}

logger.info(f"MinIO endpoint: {MINIO_ENDPOINT} (SSL: {ssl_enabled})")
logger.info(f"PostgreSQL: {DB_CONN_MASKED}")
logger.info(f"Delta Bronze activités: {DELTA_BRONZE_PATH}")
logger.info(f"Delta Bronze primes/JBE: {DELTA_PRIMES_JBE_PATH}")

# ======================================================================================
# 2) SparkSession (Europe/Paris, Delta, MinIO S3A)
# ======================================================================================

spark = (
    SparkSession.builder
    .appName("Silver/Gold Aggregations - Power BI (robuste + primes/JBE)")
    .config("spark.sql.session.timeZone", "Europe/Paris")   # cohérence avec les jobs streaming
    # Delta
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # S3A ↔ MinIO
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    # Tuning
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ======================================================================================
# 3) Helpers locaux
# ======================================================================================

def _ensure_not_empty(df: DataFrame, name: str, hint: str = ""):
    if df.rdd.isEmpty():
        msg = f"Dataset '{name}' vide — rien à agréger."
        if hint:
            msg += f" Hint: {hint}"
        raise RuntimeError(msg)

def _safe_count(df: DataFrame, label: str):
    try:
        c = df.count()
        logger.info(f"{label} → {c} lignes")
        return c
    except Exception as e:
        logger.warning(f"Impossible de compter '{label}': {e}")
        return -1

def _read_delta_or_none(path: str) -> DataFrame | None:
    try:
        df = spark.read.format("delta").load(path)
        _safe_count(df, f"Lecture Delta OK: {path}")
        return df
    except AnalysisException as e:
        logger.warning(f"Delta introuvable: {path} ({e.desc if hasattr(e,'desc') else e})")
        return None
    except Exception as e:
        logger.warning(f"Lecture Delta échouée: {path} ({e})")
        return None

def _robust_date_columns(df: DataFrame) -> DataFrame:
    """
    Crée date_jour (DATE), annee (INT), mois (INT), semaine (INT) à partir de 'jour'/'date_debut'.
    Évite la collision avec un éventuel 'mois' string du bronze en la renommant en 'mois_part'.
    """
    if "mois" in df.columns:
        df = df.withColumnRenamed("mois", "mois_part")  # garde la partition d'origine si besoin

    base = None
    if "jour" in df.columns:
        base = to_date(col("jour"))
    if base is None or isinstance(base, type(None)):
        base = to_date(col("date_debut"))

    df = df.withColumn("date_jour", coalesce(
        to_date(col("jour")),
        to_date(col("date_debut")),
        to_date(to_timestamp(col("jour"))),
        to_date(to_timestamp(col("jour"), "yyyy-MM-dd HH:mm:ss")),
        to_date(to_timestamp(col("jour"), "dd/MM/yyyy"))
    ))

    df = (df
          .withColumn("annee", year(col("date_jour")))
          .withColumn("mois",  month(col("date_jour")))
          .withColumn("semaine", weekofyear(col("date_jour"))))
    return df

def _read_primes_jbe_or_none(path: str) -> DataFrame | None:
    """Lit le bronze_primes_jbe et prépare 'event_type' + 'ref_temporale' (YYYY-MM)."""
    try:
        av = spark.read.format("delta").load(path)
        av = (av
              .withColumn("event_type", lower(trim(col("event_type"))))
              .withColumn(
                  "ref_temporale",
                  date_format(
                      coalesce(
                          to_timestamp(col("date_prime")),   # si présent (prime)
                          to_timestamp(col("event_time"))    # sinon depuis l'event
                      ), "yyyy-MM"
                  )
              ))
        _safe_count(av, f"Lecture Delta OK (primes/JBE) : {path}")
        return av
    except Exception as e:
        logger.warning(f"Primes/JBE Delta introuvable ({path}) : {e}")
        return None

# ======================================================================================
# 4) Pipeline principal
# ======================================================================================

def main():
    logger.info("=== DÉMARRAGE AGRÉGATIONS POWER BI (silver/gold) — version robuste + primes/JBE ===")
    helper = MinIOHelper()

    try:
        # 1) Lecture bronze Δ (ACTIVITÉS)
        logger.info(f"📥 Lecture Delta bronze activités : {DELTA_BRONZE_PATH}")
        b = _read_delta_or_none(DELTA_BRONZE_PATH)
        if b is None:
            raise RuntimeError(f"Bronze introuvable: {DELTA_BRONZE_PATH}")

        if b.rdd.isEmpty():
            logger.warning("Bronze vide — fin sans action.")
            return

        # Sanity minimal & normalisation types
        required_any = {"uid", "id_salarie"}
        missing_any = required_any - set(b.columns)
        if missing_any:
            raise RuntimeError(f"Colonnes minimales manquantes dans bronze : {sorted(missing_any)}")

        b = b.withColumn("id_salarie", col("id_salarie").cast("string"))
        if "distance_km" in b.columns:
            b = b.withColumn("distance_km", col("distance_km").cast("double"))
        if "temps_sec" in b.columns:
            b = b.withColumn("temps_sec", col("temps_sec").cast("double"))

        # Profil rapide des nulls (debug)
        for c in ["jour", "date_debut", "id_salarie", "type_activite", "distance_km", "temps_sec"]:
            if c in b.columns:
                nulls = b.where(col(c).isNull()).count()
                logger.info(f"NULLs dans {c} = {nulls}")

        # Colonnes date robustes (génère annee/mois/semaine + date_jour)
        b = _robust_date_columns(b)

        # 2) Jointure RH optionnelle (si dispo)
        rh = _read_delta_or_none(DELTA_RH_PATH_OPT)
        if rh is not None and {"id_salarie", "nom", "prenom"} <= set(rh.columns):
            rh = rh.withColumn("id_salarie", col("id_salarie").cast("string")).dropDuplicates(["id_salarie"]) \
                   .select("id_salarie", "nom", "prenom")
            b = b.join(rh, on="id_salarie", how="left")
            logger.success("✅ Jointure RH effectuée (nom/prenom mis à jour si présents).")
        else:
            logger.warning("⚠️ Référentiel RH indisponible ou schéma inattendu — conservation des noms/prénoms du bronze.")

        # 3) SILVER : curation (champs essentiels + non négatifs mais tolérance NULL)
        essentials_ok = (
            col("uid").isNotNull() &
            col("id_salarie").isNotNull() &
            col("date_jour").isNotNull()
        )
        non_negative_ok = (
            (col("distance_km").isNull() | (col("distance_km") >= 0)) &
            (col("temps_sec").isNull()   | (col("temps_sec")   >= 0))
        )
        silver = b.where(essentials_ok & non_negative_ok)
        _safe_count(silver, "SILVER après filtre essentials")
        _ensure_not_empty(
            silver,
            "silver",
            hint="Vérifie le parse de 'jour' → 'date_jour' et la présence de 'uid'/'id_salarie'."
        )

        # Écriture SILVER (partition annee/mois/jour)
        logger.info(f"🪙 Écriture SILVER → {SILVER_PATH}")
        (silver.coalesce(COALESCE_N)
               .write.format("delta")
               .mode("overwrite")
               .option("mergeSchema", "true")
               .option("maxRecordsPerFile", str(MAX_RECORDS_FILE))
               .partitionBy("annee", "mois", "jour")
               .save(SILVER_PATH))

        # 4) GOLD HEBDO (par salarié x semaine)
        gold_week = (
            silver.groupBy("id_salarie", "nom", "prenom", "annee", "semaine")
                  .agg(
                      count("*").alias("nb_activites"),
                      countDistinct("date_jour").alias("jours_actifs"),
                      spark_sum("distance_km").alias("distance_totale_km"),
                      spark_sum("temps_sec").alias("temps_total_sec"),
                      spark_min("date_jour").alias("premiere_activite"),
                      spark_max("date_jour").alias("derniere_activite")
                  )
                  .withColumn("distance_km", spark_round(col("distance_totale_km"), 2))
                  .withColumn("temps_minutes", spark_round(col("temps_total_sec") / 60.0, 1))
                  .drop("distance_totale_km", "temps_total_sec")
        )
        _ensure_not_empty(gold_week, "gold_week")

        logger.info(f"🥇 Écriture GOLD (hebdo) → {GOLD_WEEKLY_PATH}")
        (gold_week.coalesce(COALESCE_N)
                  .write.format("delta")
                  .mode("overwrite")
                  .option("mergeSchema", "true")
                  .option("maxRecordsPerFile", str(MAX_RECORDS_FILE))
                  .partitionBy("annee", "semaine")
                  .save(GOLD_WEEKLY_PATH))

        # 5) GOLD MENSUEL (par salarié x mois)
        gold_month = (
            silver.groupBy("id_salarie", "nom", "prenom", "annee", "mois")
                  .agg(
                      count("*").alias("nb_activites"),
                      countDistinct("date_jour").alias("jours_actifs"),
                      spark_sum("distance_km").alias("distance_totale_km"),
                      spark_sum("temps_sec").alias("temps_total_sec"),
                      spark_min("date_jour").alias("premiere_activite"),
                      spark_max("date_jour").alias("derniere_activite")
                  )
                  .withColumn("annee_mois", date_format(col("premiere_activite"), "yyyy-MM"))
                  .withColumn("distance_km", spark_round(col("distance_totale_km"), 2))
                  .withColumn("temps_minutes", spark_round(col("temps_total_sec") / 60.0, 1))
                  .select(
                      "id_salarie", "nom", "prenom", "annee", "mois", "annee_mois",
                      "nb_activites", "jours_actifs", "distance_km", "temps_minutes",
                      "premiere_activite", "derniere_activite"
                  )
        )
        _ensure_not_empty(gold_month, "gold_month")

        # 5bis) Enrichir le GOLD MENSUEL avec primes/JBE mensuels si dispo
        av = _read_primes_jbe_or_none(DELTA_PRIMES_JBE_PATH)
        if av is not None and {"id_salarie", "event_type", "ref_temporale"} <= set(av.columns):
            primes_m = (av.filter(col("event_type") == "prime")
                          .groupBy("id_salarie", "ref_temporale")
                          .agg(spark_sum("prime_montant_eur").alias("prime_mensuelle_eur")))
            jbe_m = (av.filter(col("event_type") == "jbe")
                        .groupBy("id_salarie", "ref_temporale")
                        .agg(spark_sum("nb_journees_bien_etre").alias("jbe_mensuelle_jours")))

            gold_month = (gold_month
                          .join(primes_m, on=[col("id_salarie") == primes_m["id_salarie"],
                                              col("annee_mois") == primes_m["ref_temporale"]], how="left")
                          .drop(primes_m["id_salarie"], primes_m["ref_temporale"])
                          .join(jbe_m, on=[col("id_salarie") == jbe_m["id_salarie"],
                                           col("annee_mois") == jbe_m["ref_temporale"]], how="left")
                          .drop(jbe_m["id_salarie"], jbe_m["ref_temporale"])
                          .withColumn("prime_mensuelle_eur", coalesce(col("prime_mensuelle_eur"), lit(0.0)))
                          .withColumn("jbe_mensuelle_jours", coalesce(col("jbe_mensuelle_jours"), lit(0))))
            logger.success("✅ GOLD mensuel enrichi avec primes/JBE mensuels.")
        else:
            logger.warning("⚠️ Flux primes/JBE indisponible — GOLD mensuel non enrichi.")

        logger.info(f"🥇 Écriture GOLD (mensuel) → {GOLD_MONTHLY_PATH}")
        (gold_month.coalesce(COALESCE_N)
                   .write.format("delta")
                   .mode("overwrite")
                   .option("mergeSchema", "true")
                   .option("maxRecordsPerFile", str(MAX_RECORDS_FILE))
                   .partitionBy("annee", "mois")
                   .save(GOLD_MONTHLY_PATH))

        # 6) GOLD INDICATEURS (totaux par salarié) + enrichissement primes/JBE
        weeks_per_user = (
            silver.select("id_salarie", "annee", "semaine").distinct()
                  .groupBy("id_salarie")
                  .agg(countDistinct("semaine").alias("nb_semaines_actives"))
        )
        gold_kpi = (
            silver.groupBy("id_salarie", "nom", "prenom")
                  .agg(
                      count("*").alias("nb_activites"),
                      countDistinct("date_jour").alias("jours_bien_etre"),
                      spark_sum("distance_km").alias("distance_totale_km"),
                      spark_sum("temps_sec").alias("temps_total_sec"),
                      spark_min("date_jour").alias("premiere_activite"),
                      spark_max("date_jour").alias("derniere_activite"),
                  )
                  .join(weeks_per_user, on="id_salarie", how="left")
                  .withColumn("distance_km", spark_round(col("distance_totale_km"), 2))
                  .withColumn("temps_minutes", spark_round(col("temps_total_sec") / 60.0, 1))
                  .withColumn(
                      "frequence_moyenne_hebdo",
                      when(col("nb_semaines_actives") > 0,
                           spark_round(col("nb_activites") / col("nb_semaines_actives"), 2))
                      .otherwise(lit(None))
                  )
                  .select(
                      "id_salarie", "nom", "prenom", "nb_activites", "jours_bien_etre", "nb_semaines_actives",
                      "frequence_moyenne_hebdo", "distance_km", "temps_minutes",
                      "premiere_activite", "derniere_activite"
                  )
        )

        # Enrichissement primes/JBE totaux si dispo
        if av is not None and {"id_salarie", "event_type"} <= set(av.columns):
            primes_tot = (av.filter(col("event_type") == "prime")
                           .groupBy("id_salarie")
                           .agg(spark_sum("prime_montant_eur").alias("prime_total_eur")))
            jbe_tot = (av.filter(col("event_type") == "jbe")
                        .groupBy("id_salarie")
                        .agg(spark_sum("nb_journees_bien_etre").alias("jbe_total_jours")))
            gold_kpi = (gold_kpi
                        .join(primes_tot, on="id_salarie", how="left")
                        .join(jbe_tot,   on="id_salarie", how="left")
                        .withColumn("prime_total_eur", coalesce(col("prime_total_eur"), lit(0.0)))
                        .withColumn("jbe_total_jours", coalesce(col("jbe_total_jours"), lit(0))))
            logger.success("✅ KPIs enrichis avec primes/JBE totaux.")
        else:
            logger.warning("⚠️ Flux primes/JBE indisponible — KPIs non enrichis.")

        _ensure_not_empty(gold_kpi, "gold_kpi")

        logger.info(f"🥇 Écriture GOLD (indicateurs) → {GOLD_KPI_PATH}")
        (gold_kpi.coalesce(COALESCE_N)
                 .write.format("delta")
                 .mode("overwrite")
                 .option("mergeSchema", "true")
                 .option("maxRecordsPerFile", str(MAX_RECORDS_FILE))
                 .save(GOLD_KPI_PATH))

        # 7) Great Expectations (sur GOLD indicateurs) — non bloquant si absent / relax
        logger.info("🔍 Contrôle qualité Great Expectations (GOLD indicateurs)")
        df_pd = gold_kpi.toPandas()  # agrégé → volume modéré

        if HAS_GE:
            ge_df = PandasDataset(df_pd)
            ge_df.expect_column_values_to_not_be_null("id_salarie")
            ge_df.expect_column_values_to_be_between("distance_km", 0, 10000)
            ge_df.expect_column_values_to_be_between("temps_minutes", 0, 9999)

            result = ge_df.validate()
            if not result.success and GE_STRICT_FAIL:
                raise Exception("Échec de validation Great Expectations (GOLD indicateurs)")

            # Rapport GE → MinIO
            rendered = ValidationResultsPageRenderer().render(result)
            html = DefaultJinjaPageView().render(rendered)
            helper.upload_object(
                EXPORT_MINIO_REPORT,
                html.encode("utf-8"),
                content_type="text/html"
            )
            logger.success(f"📄 Rapport GE envoyé : {EXPORT_MINIO_REPORT}")
        else:
            logger.warning("Great Expectations indisponible — étape QA sautée.")

        # 8) Export CSV MinIO (GOLD indicateurs) pour Power BI
        helper.upload_object(
            EXPORT_MINIO_OBJECT,
            df_pd.to_csv(index=False).encode("utf-8"),
            content_type="text/csv"
        )
        logger.success(f"📃 Export CSV MinIO : {EXPORT_MINIO_OBJECT}")

        # 9) Export PostgreSQL (sportdata.avantages_sportifs)
        if POSTGRES_USER and POSTGRES_PASSWORD:
            from sqlalchemy import create_engine, text
            engine = create_engine(RAW_DB_CONN_STR)
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS sportdata"))
            # replace (vue matérielle rafraîchie)
            df_pd.to_sql(
                "avantages_sportifs",
                engine,
                schema="sportdata",
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=10_000,
            )
            logger.success("📈 Table PostgreSQL : sportdata.avantages_sportifs (replace).")
        else:
            logger.warning("⚠️ Variables PostgreSQL manquantes — export BDD sauté.")

        logger.success("🌟 Agrégations silver/gold & exports terminés (version robuste + primes/JBE).")

    except Exception as e:
        logger.error(f"❌ Erreur agrégation Power BI : {e}")
        try:
            envoyer_message_erreur("avantages_sportifs", f"🚨 Échec agrégation Power BI\n{e}")
        except Exception:
            pass
        raise
    finally:
        spark.stop()

# ======================================================================================
# 5) Entrée
# ======================================================================================

if __name__ == "__main__":
    main()
