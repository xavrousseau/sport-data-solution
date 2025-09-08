# ==========================================================================================
# Script      : sds_monitor_exporter.py
# Objet       : Exporter Prometheus pour la stack Sport Data Solution
#               - HTTP /metrics (Prometheus)
#               - MinIO: présence + volumétrie par préfixe
#               - Postgres: disponibilité + rowcount tables clés
#               - Kafka/Redpanda: connectivité + (optionnel) lag d’un consumer group
#               - Spark: fraîcheur checkpoints (local ET s3a://)
#               - Delta: fraîcheur tables via _delta_log
#               - GE: timestamp du dernier rapport (exports/ | validation/)
# Auteur      : Xavier Rousseau | Août 2025 (version harmonisée/loguée)
# ==========================================================================================

import os
import sys
import time
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple, List, Optional

from dotenv import load_dotenv
from loguru import logger
from prometheus_client import start_http_server, Gauge, Info

# --- MinIO ---
from minio import Minio
from minio.error import S3Error

# --- Postgres ---
import psycopg2
import psycopg2.extras

# --- Kafka (optionnel) ---
try:
    from confluent_kafka import Consumer, KafkaException
    KAFKA_AVAILABLE = True
except Exception:
    KAFKA_AVAILABLE = False


# ==========================================================================================
# 0) ENV & logging (sans fuite de secrets)
# ==========================================================================================

SENSITIVE_TOKENS = ("PASSWORD", "SECRET", "TOKEN", "KEY", "ACCESS")
def _no_secret_logs(record):
    return not any(tok in record["message"].upper() for tok in SENSITIVE_TOKENS)

def mask(s: str, keep=2) -> str:
    if not s:
        return ""
    return s[:keep] + "***" if len(s) > keep else "***"

logger.remove()
logger.add(sys.stdout, level="INFO", filter=_no_secret_logs)

# Chargement .env (Airflow puis local)
try:
    load_dotenv("/opt/airflow/.env", override=True)
except Exception:
    pass
load_dotenv(".env", override=True)

# --- HTTP exporter ---
MONITOR_PORT       = int(os.getenv("MONITOR_PORT", "9108"))
MONITOR_INTERVAL_S = int(os.getenv("MONITOR_INTERVAL", "30"))

# --- MinIO (endpoint direct ou host:port) ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "").strip()
if not MINIO_ENDPOINT:
    _host = os.getenv("MINIO_HOST", "sport-minio")
    _port = os.getenv("MINIO_PORT", "9000")
    MINIO_ENDPOINT = f"http://{_host}:{_port}"
MINIO_BUCKET   = os.getenv("MINIO_BUCKET_NAME", "sportdata")
MINIO_ACCESS   = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))   # dev only
MINIO_SECRET   = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", "minioadmin"))# dev only
MINIO_SECURE   = MINIO_ENDPOINT.lower().startswith("https://")
MINIO_HOSTPORT = MINIO_ENDPOINT.replace("https://", "").replace("http://", "")

# --- Postgres ---
PG_HOST   = os.getenv("POSTGRES_HOST", "sport-postgres")
PG_PORT   = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB     = os.getenv("POSTGRES_DB", "sportdata")
PG_USER   = os.getenv("POSTGRES_USER", "user")
PG_PASS   = os.getenv("POSTGRES_PASSWORD", "password")
PG_SCHEMA = os.getenv("POSTGRES_SCHEMA", "sportdata")

# --- Kafka / Redpanda ---
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_GROUP_OPT  = os.getenv("KAFKA_MONITOR_GROUP", "")  # ex: avantages-consumer
KAFKA_TOPICS_CSV = os.getenv(
    "KAFKA_MONITOR_TOPICS",
    "sportdata.sportdata.activites_sportives,"
    "sportdata.sportdata.beneficiaires_primes_sport,"
    "sportdata.sportdata.beneficiaires_journees_bien_etre"
)

# --- Checkpoints Spark (fallbacks alignés avec le .env final) ---
CKPT_QC = (
    os.getenv("CHECKPOINT_PATH_DELTA_QC") or
    os.getenv("CHECKPOINT_PATH_QC") or
    "tmp/checkpoints/bronze_qc"
)
# Un seul writer bronze → on mappe CKPT_ACTIVITES sur CKPT_QC si absent
CKPT_ACTIVITES = os.getenv("CHECKPOINT_PATH_ACTIVITES", CKPT_QC)
CKPT_PRIMES    = os.getenv(
    "CHECKPOINT_PATH_PRIMES_JBE",
    os.getenv("CHECKPOINT_PATH_PRIMES", "tmp/checkpoints/primes_jbe")
)

# --- Tables Delta à suivre (fraîcheur via _delta_log) ---
DELTA_PATH_ACTIVITES = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/activites_sportives")
DELTA_PATH_PRIMESJBE = os.getenv("DELTA_BASE_PRIMES_JBE", "s3a://sportdata/bronze_primes_jbe")

# --- Préfixes MinIO à monitorer ---
DEFAULT_MINIO_PREFIXES = ["inputs/","clean/","bronze/","silver/","gold/","exports/","validation/"]
def _parse_prefixes_from_env() -> List[str]:
    raw = os.getenv("MINIO_MONITOR_PREFIXES", "").strip()
    if not raw:
        return DEFAULT_MINIO_PREFIXES[:]
    prefs = [p.strip() for p in raw.split(",") if p.strip()]
    return [(p if p.endswith("/") else p + "/") for p in prefs] or DEFAULT_MINIO_PREFIXES[:]
MINIO_PREFIXES = _parse_prefixes_from_env()

# Logs d’état init (sans secrets)
logger.info(f"Exporter HTTP  : :{MONITOR_PORT} | interval={MONITOR_INTERVAL_S}s")
logger.info(f"MinIO endpoint : {MINIO_HOSTPORT} (SSL: {MINIO_SECURE}) | bucket={MINIO_BUCKET} | access={mask(MINIO_ACCESS)} | secret={mask(MINIO_SECRET)}")
logger.info(f"Postgres       : {PG_HOST}:{PG_PORT}/{PG_DB} | user={PG_USER}")
logger.info(f"Kafka          : bootstrap={KAFKA_BOOTSTRAP} | group={KAFKA_GROUP_OPT or '-'}")
logger.info(f"Checkpoints    : activites={CKPT_ACTIVITES} | qc={CKPT_QC} | primes={CKPT_PRIMES}")
logger.info(f"Delta tables   : activites={DELTA_PATH_ACTIVITES} | primes_jbe={DELTA_PATH_PRIMESJBE}")
logger.info(f"MinIO prefixes : {', '.join(MINIO_PREFIXES)}")


# ==========================================================================================
# 1) Prometheus metrics
# ==========================================================================================

APP_INFO = Info("sds_monitor_info", "Sport Data Solution monitor info")
APP_INFO.info({"version": "1.2.0", "host": socket.gethostname()})

minio_up      = Gauge("sds_minio_up", "MinIO availability (1=up,0=down)")
minio_objects = Gauge("sds_minio_objects_total", "Object count by prefix", ["prefix"])
minio_bytes   = Gauge("sds_minio_bytes_total",   "Total bytes by prefix", ["prefix"])

pg_up         = Gauge("sds_postgres_up", "Postgres availability (1=up,0=down)")
pg_rowcount   = Gauge("sds_postgres_rowcount", "Row count for key tables", ["table"])

kafka_up      = Gauge("sds_kafka_up", "Kafka availability (1=up,0=down)")
kafka_lag     = Gauge("sds_kafka_group_lag", "Consumer group lag by topic", ["group", "topic"])

spark_ckpt_freshness = Gauge(
    "sds_spark_checkpoint_freshness_seconds",
    "Seconds since last checkpoint file modification", ["job"]
)

delta_freshness = Gauge(
    "sds_delta_table_freshness_seconds",
    "Seconds since last Delta commit (_delta_log)", ["table"]
)

ge_last_success_ts = Gauge(
    "sds_ge_last_success_timestamp",
    "Last GE report timestamp (epoch seconds)"
)


# ==========================================================================================
# 2) Helpers MinIO / S3A
# ==========================================================================================

def minio_client() -> Minio:
    """Client MinIO prêt à l’emploi (http/https + creds)."""
    return Minio(
        MINIO_HOSTPORT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=MINIO_SECURE
    )

def s3a_url_to_bucket_prefix(url: str) -> Optional[Tuple[str, str]]:
    """s3a://bucket/prefix → (bucket, prefix) ; None si format inattendu."""
    if not url or not url.lower().startswith("s3a://"):
        return None
    no = url[6:]
    if "/" not in no:
        return (no, "")
    bucket, prefix = no.split("/", 1)
    if prefix.startswith("/"):
        prefix = prefix[1:]
    return (bucket, prefix)

def list_minio_prefix_stats(client: Minio, bucket: str, prefix: str) -> Tuple[bool, int, int]:
    """Comptage récursif d’objets et somme d’octets sous bucket/prefix."""
    nb = 0
    size = 0
    try:
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            nb += 1
            size += getattr(obj, "size", 0) or 0
        return True, nb, size
    except Exception:
        return False, 0, 0

def latest_minio_object_epoch(client: Minio, bucket: str, prefix: str) -> int:
    """Max(last_modified) sous bucket/prefix ; 0 si aucun objet."""
    latest = 0
    try:
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            lm = getattr(obj, "last_modified", None)
            if lm:
                ts = int(lm.timestamp())
                if ts > latest:
                    latest = ts
    except Exception:
        pass
    return latest

def latest_ge_report_epoch(client: Minio, bucket: str) -> int:
    """Dernier rapport GE (.html) sous exports/ ou validation/ (epoch)."""
    latest = 0
    for px in ("exports/", "validation/"):
        try:
            for obj in client.list_objects(bucket, prefix=px, recursive=True):
                name = str(getattr(obj, "object_name", ""))
                if not name.lower().endswith(".html"):
                    continue
                lm = getattr(obj, "last_modified", datetime.now(timezone.utc))
                ts = int(lm.timestamp())
                latest = max(latest, ts)
        except Exception:
            continue
    return latest


# ==========================================================================================
# 3) Helpers Postgres / Kafka
# ==========================================================================================

def postgres_query_rowcounts() -> Tuple[bool, Dict[str, int]]:
    """Rowcounts des tables clés ; -1 si table non accessible."""
    tables = [
        f"{PG_SCHEMA}.employes",
        f"{PG_SCHEMA}.activites_sportives",
        f"{PG_SCHEMA}.beneficiaires_primes_sport",
        f"{PG_SCHEMA}.beneficiaires_journees_bien_etre",
        f"{PG_SCHEMA}.avantages_sportifs",
    ]
    res: Dict[str, int] = {}
    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) AS c FROM {t}")
                res[t] = int(cur.fetchone()["c"])
            except Exception:
                res[t] = -1
        cur.close()
        conn.close()
        return True, res
    except Exception:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        return False, {}

def kafka_check_connectivity_and_lag() -> Tuple[bool, Dict[str, int]]:
    """Ping Kafka + lag par topic si un consumer group est fourni."""
    if not KAFKA_AVAILABLE:
        return False, {}

    # Ping metadata
    try:
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"sds-monitor-ping-{int(time.time())}",
            "enable.auto.commit": False,
            "session.timeout.ms": 6000,
        }
        c = Consumer(conf)
        _ = c.list_topics(timeout=3.0)
        c.close()
        up = True
    except KafkaException:
        return False, {}

    lags: Dict[str, int] = {}
    if KAFKA_GROUP_OPT:
        try:
            conf2 = {
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": KAFKA_GROUP_OPT,
                "enable.auto.commit": False,
                "session.timeout.ms": 6000,
            }
            consumer = Consumer(conf2)
            topics = [t.strip() for t in KAFKA_TOPICS_CSV.split(",") if t.strip()]
            md = consumer.list_topics(timeout=5.0)
            for topic in topics:
                if topic not in md.topics:
                    lags[topic] = -1
                    continue
                topic_lag = 0
                partitions = list(md.topics[topic].partitions.keys())
                for p in partitions:
                    low, high = consumer.get_watermark_offsets((topic, p), timeout=3.0)
                    committed_md = consumer.committed([(topic, p)], timeout=3.0)
                    committed = committed_md[0].offset if committed_md else -1001
                    if committed is None or committed < 0:
                        topic_lag += max(high - low, 0)
                    else:
                        topic_lag += max(high - committed, 0)
                lags[topic] = topic_lag
            consumer.close()
        except Exception:
            pass

    return up, lags


# ==========================================================================================
# 4) Helpers Spark/Delta (fraîcheur checkpoints & _delta_log)
# ==========================================================================================

def checkpoint_freshness_seconds(path: str, mc: Optional[Minio]) -> int:
    """
    Secondes écoulées depuis la dernière modif sous 'path'.
    - local : os.walk
    - s3a:// : MinIO list_objects(last_modified)
    """
    if not path:
        return -1
    if path.lower().startswith("s3a://"):
        if not mc:
            return -1
        bp = s3a_url_to_bucket_prefix(path)
        if not bp:
            return -1
        bucket, prefix = bp
        ts = latest_minio_object_epoch(mc, bucket, prefix.rstrip("/") + "/")
        return int(time.time()) - ts if ts > 0 else -1

    # local FS
    try:
        newest = 0.0
        for root, _, files in os.walk(path):
            for fn in files:
                p = Path(root) / fn
                try:
                    m = p.stat().st_mtime
                    if m > newest:
                        newest = m
                except Exception:
                    continue
        return int(time.time() - newest) if newest > 0.0 else -1
    except Exception:
        return -1

def delta_table_freshness_seconds(table_path: str, mc: Optional[Minio]) -> int:
    """Fraîcheur Delta = secondes depuis le dernier commit _delta_log (s3a:// uniquement)."""
    if not table_path or not table_path.lower().startswith("s3a://") or not mc:
        return -1
    bp = s3a_url_to_bucket_prefix(table_path)
    if not bp:
        return -1
    bucket, prefix = bp
    log_prefix = (prefix.rstrip("/") + "/_delta_log/").lstrip("/")
    ts = latest_minio_object_epoch(mc, bucket, log_prefix)
    return int(time.time()) - ts if ts > 0 else -1


# ==========================================================================================
# 5) Boucle principale (HTTP /metrics)
# ==========================================================================================

def main():
    logger.info(f"📡 Exporter Prometheus démarré sur 0.0.0.0:{MONITOR_PORT}")
    start_http_server(MONITOR_PORT)

    while True:
        # --- MinIO (up + volumétrie par préfixe) ---
        ok_minio = 0
        mc = None
        try:
            mc = minio_client()
            ok_bucket = any(b.name == MINIO_BUCKET for b in mc.list_buckets())
            ok_minio = 1 if ok_bucket else 0
        except Exception:
            ok_minio = 0
            mc = None
        minio_up.set(ok_minio)

        if ok_minio and mc:
            for px in MINIO_PREFIXES:
                try:
                    ok, n, b = list_minio_prefix_stats(mc, MINIO_BUCKET, px)
                    if ok:
                        minio_objects.labels(prefix=px).set(n)
                        minio_bytes.labels(prefix=px).set(b)
                except Exception:
                    pass
            ts_ge = latest_ge_report_epoch(mc, MINIO_BUCKET)
            if ts_ge > 0:
                ge_last_success_ts.set(ts_ge)

        # --- Postgres (up + rowcounts) ---
        ok_pg, counts = postgres_query_rowcounts()
        pg_up.set(1 if ok_pg else 0)
        if counts:
            for t, c in counts.items():
                pg_rowcount.labels(table=t).set(c)

        # --- Kafka (up + lags optionnels) ---
        if KAFKA_AVAILABLE:
            ok_k, lags = kafka_check_connectivity_and_lag()
            kafka_up.set(1 if ok_k else 0)
            if lags:
                for topic, lag in lags.items():
                    kafka_lag.labels(group=KAFKA_GROUP_OPT or "-", topic=topic).set(lag)
        else:
            kafka_up.set(0)

        # --- Spark checkpoints (local/s3a) ---
        spark_ckpt_freshness.labels(job="bronze_activites").set(
            checkpoint_freshness_seconds(CKPT_ACTIVITES, mc)
        )
        spark_ckpt_freshness.labels(job="bronze_qc").set(
            checkpoint_freshness_seconds(CKPT_QC, mc)
        )
        spark_ckpt_freshness.labels(job="primes_jbe").set(
            checkpoint_freshness_seconds(CKPT_PRIMES, mc)
        )

        # --- Delta tables (fraîcheur _delta_log) ---
        delta_freshness.labels(table="bronze_activites_sportives").set(
            delta_table_freshness_seconds(DELTA_PATH_ACTIVITES, mc)
        )
        delta_freshness.labels(table="bronze_primes_jbe").set(
            delta_table_freshness_seconds(DELTA_PATH_PRIMESJBE, mc)
        )

        time.sleep(MONITOR_INTERVAL_S)


# ==========================================================================================
# 6) Entrée
# ==========================================================================================

if __name__ == "__main__":
    main()
