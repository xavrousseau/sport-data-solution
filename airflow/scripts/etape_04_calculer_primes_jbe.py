# ==========================================================================================
# Script      : etape_04_calculer_primes_jbe.py
# Objectif    : Calculer primes & JBE (MinIO → pandas), exporter Excel, insérer Postgres (modèle sans "periode"),
#               publier Kafka (1 msg / bénéficiaire). AUCUNE notification NTFY ici.
# Auteur      : Xavier Rousseau | Août 2025
# ==========================================================================================

import os
import io
import sys
import json
import uuid
from datetime import datetime, date

import pandas as pd
from sqlalchemy import create_engine, text, bindparam
from dotenv import load_dotenv
from loguru import logger
import great_expectations as ge
from kafka import KafkaProducer

# Helpers montés dans /opt/airflow/scripts
sys.path.append("/opt/airflow/scripts")
from minio_helper import MinIOHelper  # type: ignore

# ==========================================================================================
# 0) Logs & ENV
# ==========================================================================================
logger.remove()
logger.add(sys.stdout, level="INFO")

# Charger d'abord l'.env Airflow puis l'.env local
load_dotenv(dotenv_path="/opt/airflow/.env", override=True)
load_dotenv(dotenv_path=".env", override=True)

# PostgreSQL
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "sport-postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sportdata")
DB_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# MinIO
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "sportdata")
MINIO_RH_KEY = os.getenv("MINIO_RH_KEY", "clean/donnees_rh_cleaned.xlsx")
MINIO_SPORT_KEY = os.getenv("MINIO_SPORT_KEY", "clean/activites_sportives_simulees.xlsx")

# Exports Excel
MINIO_EXPORT_PRIMES = os.getenv("MINIO_EXPORT_PRIMES", "gold/beneficiaires_primes_sportives.xlsx")
MINIO_EXPORT_BE = os.getenv("MINIO_EXPORT_BE", "gold/beneficiaires_journees_bien_etre.xlsx")

# Métier
SEUIL_ACTIVITES_PRIME = int(os.getenv("SEUIL_ACTIVITES_PRIME", 10))
POURCENTAGE_PRIME = float(os.getenv("POURCENTAGE_PRIME", 0.05))
JOURNEES_BIEN_ETRE = int(os.getenv("NB_JOURNEES_BIEN_ETRE", 5))
DATE_DU_JOUR = date.today()  # pour la colonne date_prime

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC_PRIMES = os.getenv("KAFKA_TOPIC_PRIMES", "sportdata.sportdata.beneficiaires_primes_sport")
KAFKA_TOPIC_JBE = os.getenv("KAFKA_TOPIC_JBE", "sportdata.sportdata.beneficiaires_journees_bien_etre")

# ==========================================================================================
# 1) Utils : Kafka & GE
# ==========================================================================================
def _producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks=1,
    )

def ge_validate_primes(df: pd.DataFrame) -> None:
    """Contrôle GE minimal non bloquant sur df_primes."""
    try:
        if df is None or df.empty:
            logger.info("GE: aucun contrôle (df_primes vide).\n")
            return
        ge_df = ge.from_pandas(df)
        ge_df.expect_column_values_to_not_be_null("id_salarie")
        ge_df.expect_column_values_to_be_between("nb_activites", min_value=SEUIL_ACTIVITES_PRIME)
        ge_df.expect_column_values_to_be_between("prime_montant_eur", min_value=0)
        result = ge_df.validate(result_format="SUMMARY")
        logger.info(f"GE: success={bool(result.get('success', False))}\n")
    except Exception as e:
        logger.warning(f"GE ignoré (non bloquant) : {e}\n")

# ==========================================================================================
# 2) I/O : charger données sources + exporter Excel
# ==========================================================================================
def charger_donnees() -> tuple[pd.DataFrame, pd.DataFrame]:
    helper = MinIOHelper()
    logger.info("📥 Lecture RH & activités depuis MinIO…\n")
    df_rh = helper.read_excel(MINIO_RH_KEY)
    df_act = helper.read_excel(MINIO_SPORT_KEY)
    logger.success(f"RH: {len(df_rh)} | Activités: {len(df_act)}\n")
    return df_rh, df_act

def exporter_minio_excel(df: pd.DataFrame, key: str, label: str):
    helper = MinIOHelper()
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="export")
    buf.seek(0)
    helper.client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=buf,
        ContentLength=buf.getbuffer().nbytes,
    )
    logger.success(f"📤 Export {label} → s3://{MINIO_BUCKET}/{key}\n")

# ==========================================================================================
# 3) Calcul métier (conforme aux tables)
# ==========================================================================================
def calculer_beneficiaires(df_rh: pd.DataFrame, df_act: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Nb activités / salarié
    df_agg = (
        df_act.groupby(["id_salarie", "nom", "prenom"])
        .agg(nb_activites=pd.NamedAgg(column="uid", aggfunc="count"))
        .reset_index()
    )
    # Jointure salaire
    df_agg = df_agg.merge(df_rh[["id_salarie", "salaire_brut_annuel"]], on="id_salarie", how="left")

    # Primes
    elig = df_agg[df_agg["nb_activites"] >= SEUIL_ACTIVITES_PRIME].copy()
    df_primes = elig.copy()
    df_primes["prime_eligible"] = True
    df_primes["prime_montant_eur"] = (df_primes["salaire_brut_annuel"].astype(float) * POURCENTAGE_PRIME).round(2)
    df_primes["date_prime"] = pd.to_datetime(DATE_DU_JOUR)
    df_primes = df_primes[
        ["id_salarie", "nom", "prenom", "salaire_brut_annuel", "nb_activites",
         "prime_eligible", "prime_montant_eur", "date_prime"]
    ]

    # JBE
    df_be = elig[["id_salarie", "nb_activites"]].copy()
    df_be["nb_journees_bien_etre"] = int(JOURNEES_BIEN_ETRE)
    df_be = df_be[["id_salarie", "nb_activites", "nb_journees_bien_etre"]]

    logger.success(f"Eligibles → primes: {len(df_primes)} | JBE: {len(df_be)}\n")
    return df_primes, df_be

# ==========================================================================================
# 4) Persistance Postgres (sans 'periode')
# ==========================================================================================
def persist_postgres(df_primes: pd.DataFrame, df_be: pd.DataFrame):
    engine = create_engine(DB_CONN_STRING)
    with engine.begin() as conn:
        # PRIMES — suppression des lignes du jour
        conn.execute(
            text("DELETE FROM sportdata.beneficiaires_primes_sport WHERE date_prime = :d"),
            {"d": DATE_DU_JOUR}
        )
        if not df_primes.empty:
            df_primes.to_sql(
                "beneficiaires_primes_sport",
                con=conn,
                schema="sportdata",
                if_exists="append",
                index=False,
            )
            logger.success(f"Postgres: primes insérées ({len(df_primes)})\n")
        else:
            logger.info("Postgres: aucune prime à insérer\n")

        # JBE — suppression ciblée (pas de date dans la table)
        if not df_be.empty:
            ids = df_be["id_salarie"].dropna().astype(int).unique().tolist()
            del_stmt = text("""
                DELETE FROM sportdata.beneficiaires_journees_bien_etre
                WHERE id_salarie IN :ids
            """).bindparams(bindparam("ids", expanding=True))
            conn.execute(del_stmt, {"ids": ids})

            df_be.to_sql(
                "beneficiaires_journees_bien_etre",
                con=conn,
                schema="sportdata",
                if_exists="append",
                index=False,
            )
            logger.success(f"Postgres: JBE insérées ({len(df_be)})\n")
        else:
            logger.info("Postgres: aucune JBE à insérer\n")

# ==========================================================================================
# 5) Publication Kafka — 1 message par bénéficiaire (sans 'periode')
# ==========================================================================================
def publier_kafka(df_primes: pd.DataFrame, df_be: pd.DataFrame):
    if df_primes.empty and df_be.empty:
        logger.info("Kafka: aucun message à publier.\n")
        return

    prod = _producer()
    now_ms = lambda: int(datetime.utcnow().timestamp() * 1000)
    n1 = n2 = 0

    # Primes
    for r in df_primes.itertuples(index=False):
        payload = {
            "uid": str(uuid.uuid4()),
            "event_type": "prime",
            "id_salarie": int(r.id_salarie),
            "nom": str(r.nom),
            "prenom": str(r.prenom),
            "nb_activites": int(r.nb_activites),
            "prime_montant_eur": float(r.prime_montant_eur),
            "date_prime": str(pd.to_datetime(r.date_prime).date()),
        }
        prod.send(KAFKA_TOPIC_PRIMES, value={"payload": {"op": "c", "after": payload, "ts_ms": now_ms()}})
        n1 += 1

    # JBE
    for r in df_be.itertuples(index=False):
        payload = {
            "uid": str(uuid.uuid4()),
            "event_type": "jbe",
            "id_salarie": int(r.id_salarie),
            "nb_activites": int(r.nb_activites),
            "nb_journees_bien_etre": int(r.nb_journees_bien_etre),
        }
        prod.send(KAFKA_TOPIC_JBE, value={"payload": {"op": "c", "after": payload, "ts_ms": now_ms()}})
        n2 += 1

    prod.flush()
    logger.success(f"Kafka publié → primes: {n1} | jbe: {n2}\n")

# ==========================================================================================
# 6) Pipeline
# ==========================================================================================
def pipeline():
    df_rh, df_act = charger_donnees()
    df_primes, df_be = calculer_beneficiaires(df_rh, df_act)

    # Exports Excel
    if not df_primes.empty:
        exporter_minio_excel(df_primes, MINIO_EXPORT_PRIMES, "Primes")
        ge_validate_primes(df_primes)
    else:
        logger.warning("⚠️ Aucune prime à attribuer.\n")

    if not df_be.empty:
        exporter_minio_excel(df_be, MINIO_EXPORT_BE, "JBE")
    else:
        logger.warning("⚠️ Aucune JBE à attribuer.\n")

    # Postgres
    persist_postgres(df_primes, df_be)

    # Kafka (consommé par Spark → notifications unitaires)
    publier_kafka(df_primes, df_be)

    logger.success("🎯 Pipeline terminé.\n")

# ==========================================================================================
# 7) Main
# ==========================================================================================
if __name__ == "__main__":
    pipeline()
