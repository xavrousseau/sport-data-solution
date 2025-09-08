# ==========================================================================================
# Script : etape_08_spark_bronze_controle_qualite.py
# Objet  : Kafka (Debezium JSON) → Bronze Delta (MinIO S3A) avec CONTRÔLE QUALITÉ
#          + Notifications NTFY "style historique" (groupées, capées, anti-spam)
#
# Points clés :
#   - Lecture Kafka (payload Debezium) + filet kafka_ts
#   - Parsing robuste des dates ; **jour & mois dérivés de date_debut**
#   - Nettoyage + enrichissements (vitesse_kmh, duree_min, is_future)
#   - Déduplication par uid (watermark)
#   - QC split → Bronze / Quarantine (Delta)
#   - Notifications NTFY regroupées **avec limite globale** et **max K notifs / employé**
# ==========================================================================================

import os
import sys
import json
import time
from typing import Tuple, List
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, from_json, to_timestamp, to_date, trim, when, lit, date_format,
    regexp_replace, concat_ws, coalesce as fcoalesce, abs as f_abs,
    substring, current_timestamp, row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType
)

# ------------------------------------------------------------------------------------------
# 0) Notifications — on réutilise le helper (texte brut + ressources humaines)
#    Si le helper n'est pas trouvable (ex: chemin différent), on no-op proprement.
# ------------------------------------------------------------------------------------------
sys.path.append("/opt/airflow/scripts")
try:
    from ntfy_helper import (  # type: ignore
        envoyer_message_texte,   # envoi d'un texte brut sur le topic configuré
        LIEUX_POPULAIRES,        # fallback pour anciens messages sans 'lieu'
        EMOJIS_SPORTIFS,         # emojis cohérents
        NTFY_TOPIC,              # topic par défaut centralisé
    )
    NTFY_HELPER_OK = True
except Exception:
    NTFY_HELPER_OK = False

    def envoyer_message_texte(*_args, **_kwargs):
        logger.warning("ntfy_helper introuvable — notifications désactivées (no-op).")

# ------------------------------------------------------------------------------------------
# 1) ENV & LOGGING — charger .env, paramétrer proprement, ne pas logguer de secrets
# ------------------------------------------------------------------------------------------
SENSITIVE_TOKENS = ("PASSWORD", "SECRET", "TOKEN", "KEY", "ACCESS")

def _no_secret_logs(record):
    try:
        return not any(tok in record["message"].upper() for tok in SENSITIVE_TOKENS)
    except Exception:
        return True

def mask(s: str, keep=2) -> str:
    if not s:
        return ""
    return s[:keep] + "***" if len(s) > keep else "***"

logger.remove()
logger.add(sys.stdout, level="INFO", filter=_no_secret_logs)

# Charge .env (Airflow puis local)
try:
    load_dotenv("/opt/airflow/.env", override=True)
except Exception:
    pass
load_dotenv(".env", override=True)

APP_NAME         = os.getenv("APP_NAME_QC", "BronzeIngestionQC")
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sport-redpanda:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "sportdata.sportdata.activites_sportives")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
MAX_OFFSETS      = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "20000"))

DELTA_PATH_ACTIVITES = os.getenv("DELTA_PATH_ACTIVITES", "s3a://sportdata/bronze/activites_sportives")
QUARANTINE_PATH      = os.getenv("DELTA_PATH_QUARANTINE", "s3a://sportdata/quarantine/activites_sportives")
DLQ_PARSE_PATH       = os.getenv("DELTA_PATH_ACTIVITES_DLQ_PARSE", "s3a://sportdata/bronze/_errors/activites_sportives_parse")
CHECKPOINT_PATH      = os.getenv("CHECKPOINT_PATH_DELTA_QC", "s3a://sportdata/bronze/_checkpoints/activites_qc_v4")

MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "http://sport-minio:9000")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", ""))
MINIO_SECRET_KEY   = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", ""))

# Tuning écriture / cadence
COALESCE_N       = int(os.getenv("DELTA_COALESCE_TARGET", "1"))
MAX_RECORDS_FILE = int(os.getenv("DELTA_MAX_RECORDS_PER_FILE", "500000"))
TRIGGER_SECONDS  = int(os.getenv("STREAM_TRIGGER_SECONDS", "30"))

# Notifications (style historique regroupé)
NTFY_ENABLED          = os.getenv("NTFY_ENABLED_ACTIVITES", os.getenv("NTFY_ENABLED", "true")).lower() in {"1","true","yes"}
NTFY_TOPIC_ACTIVITES  = os.getenv("NTFY_TOPIC", NTFY_TOPIC if NTFY_HELPER_OK else "sportdata_activites")
NTFY_MAX_PER_BATCH    = int(os.getenv("NTFY_MAX_PER_BATCH", "200"))   # max lignes envoyées / micro-batch
NTFY_GROUP_SIZE       = int(os.getenv("NTFY_GROUP_SIZE", "10"))       # nb de lignes par notification
NTFY_GROUP_COOLDOWN_S = int(os.getenv("NTFY_GROUP_COOLDOWN_S", "1"))  # pause entre 2 posts (anti-429)
NTFY_DEBOUNCE_SECONDS = int(os.getenv("NTFY_DEBOUNCE_SECONDS", "30")) # mini intervalle entre “vagues”
NTFY_MAX_PER_EMP      = int(os.getenv("NTFY_MAX_PER_EMP", "2"))       # 👈 max notifications / employé / batch
RESUME_LOCK_PATH      = os.path.join(CHECKPOINT_PATH, "_last_ntfy_activites.json")

# QC & logs
STRICT_QC  = os.getenv("STRICT_QC", "false").lower() in {"1","true","yes"}
QUIET_LOGS = os.getenv("QUIET_LOGS", "true").lower() in {"1","true","yes"}

# Bornes d'années pour partition 'mois' (sécurité sur les partitions)
PARTITION_YEAR_MIN = int(os.getenv("PARTITION_YEAR_MIN", "2018"))
PARTITION_YEAR_MAX = int(os.getenv("PARTITION_YEAR_MAX", "2030"))

logger.info(f"Kafka topic={KAFKA_TOPIC} | bootstrap={KAFKA_BOOTSTRAP} | offsets={STARTING_OFFSETS} | maxOffsetsPerTrigger={MAX_OFFSETS}")
logger.info(f"Delta={DELTA_PATH_ACTIVITES} | quarantine={QUARANTINE_PATH} | dlq={DLQ_PARSE_PATH}")
logger.info(f"Checkpoint={CHECKPOINT_PATH} | trigger={TRIGGER_SECONDS}s")
logger.info(f"MinIO access={mask(MINIO_ACCESS_KEY)} | secret={mask(MINIO_SECRET_KEY)} | endpoint={MINIO_ENDPOINT_RAW}")
logger.info(f"QC strict={STRICT_QC} | QUIET_LOGS={QUIET_LOGS} | NTFY_ENABLED={NTFY_ENABLED} | helper_ok={NTFY_HELPER_OK}")

# ------------------------------------------------------------------------------------------
# 2) SparkSession (Delta + S3A) — timezone métier + logs propres
# ------------------------------------------------------------------------------------------
ssl_enabled    = MINIO_ENDPOINT_RAW.lower().startswith("https://")
MINIO_ENDPOINT = MINIO_ENDPOINT_RAW.replace("https://", "").replace("http://", "")

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .config("spark.sql.session.timeZone", "Europe/Paris")
    # Delta Lake
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    # S3A / MinIO
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    # Fichiers & logs
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
    .config("spark.sql.files.maxRecordsPerFile", str(MAX_RECORDS_FILE))
    .config("spark.ui.enabled", "false")
    .config("spark.eventLog.enabled", "false")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR" if QUIET_LOGS else "WARN")

# ------------------------------------------------------------------------------------------
# 3) Schéma Debezium (payload.after) — aligné avec la simulation (champ 'lieu' inclus)
# ------------------------------------------------------------------------------------------
schema_after = StructType([
    StructField("uid",           StringType()),
    StructField("id_salarie",    StringType()),
    StructField("nom",           StringType()),
    StructField("prenom",        StringType()),
    StructField("date",          StringType()),
    StructField("jour",          StringType()),
    StructField("date_debut",    StringType()),
    StructField("type_activite", StringType()),
    StructField("distance_km",   StringType()),
    StructField("temps_sec",     StringType()),
    StructField("commentaire",   StringType()),
    StructField("profil",        StringType()),
    StructField("lieu",          StringType()),
])
schema_env = StructType().add("payload", StructType([
    StructField("op", StringType()),
    StructField("after", schema_after),
    StructField("ts_ms", LongType())
]))

# ------------------------------------------------------------------------------------------
# 4) Helpers de parsing de dates (tolérants) + filet kafka_ts
# ------------------------------------------------------------------------------------------
from pyspark.sql.functions import to_timestamp as ts, to_date as td

ISO_PATTERNS_TS = [
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    "yyyy-MM-dd'T'HH:mm:ssXXX",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSSX",
    "yyyy-MM-dd'T'HH:mm:ssX",
]
DAY_PATTERNS = ["yyyy-MM-dd", "yyyy-M-d", "yyyy/MM/dd", "dd-MM-yyyy", "dd/MM/yyyy", "yyyyMMdd"]

def parse_ts_any(scol):
    """
    Convertit différentes variantes ISO vers timestamp Spark.
    - 'Z' → '+00:00' ; '+0200' → '+02:00'
    """
    fixed = regexp_replace(scol, r"Z$", "+00:00")
    fixed = regexp_replace(fixed, r"([+-]\\d{2})(\\d{2})$", r"\\1:\\2")
    return fcoalesce(*[ts(fixed, p) for p in ISO_PATTERNS_TS])

def parse_day_any(scol):
    """Essaye plusieurs formats jour → date() Spark."""
    return fcoalesce(*[td(scol, p) for p in DAY_PATTERNS])

# ------------------------------------------------------------------------------------------
# 5) Source Kafka → DLQ parse → nettoyage → enrichissement temps → dédup
# ------------------------------------------------------------------------------------------
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .load()
)

parsed = raw.select(
    col("timestamp").alias("kafka_ts"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    from_json(col("value").cast("string"), schema_env).alias("r"),
    col("value").cast("string").alias("raw_json")
)

# DLQ : JSON illisible / schéma KO → évite de bloquer le flux
invalid_parse = (
    parsed.filter(col("r").isNull() | col("r.payload").isNull())
          .select("kafka_ts", "raw_json")
          .withColumn("error_reason", lit("from_json_null_or_schema_mismatch"))
)
dlq_query = (
    invalid_parse.writeStream.format("delta").outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}_dlq")
    .start(DLQ_PARSE_PATH)
)

# Lignes parseables
valid = parsed.filter(col("r").isNotNull() & col("r.payload").isNotNull()) \
              .select("kafka_ts","kafka_topic","kafka_partition","kafka_offset", "r.payload.after.*")

# Nettoyage types/valeurs (distance/temps → numériques propres) + 'lieu' trim
clean = (
    valid
    .filter(col("uid").isNotNull())
    .withColumn("uid", trim(col("uid")))
    .withColumn("id_salarie", col("id_salarie").cast("string"))
    .withColumn(
        "type_activite",
        when((col("type_activite").isNull()) | (trim(col("type_activite")) == ""), lit("Inconnue"))
        .otherwise(trim(col("type_activite")))
    )
    .withColumn(
        "distance_km",
        when(col("distance_km").isNull(), None)
        .otherwise(f_abs(regexp_replace(col("distance_km").cast("string"), ",", ".").cast("double")))
    )
    .withColumn(
        "temps_sec",
        when(col("temps_sec").isNull(), None)
        .otherwise(f_abs(regexp_replace(regexp_replace(col("temps_sec").cast("string"), r"\\s", ""), r"\\D", "").cast("int")))
    )
    .withColumn("lieu", trim(col("lieu")))
    .withColumn("date_debut_raw", col("date_debut"))              # pour audit (optionnel)
    .withColumn("date_debut", parse_ts_any(col("date_debut")))    # parse ISO si possible
)

# Enrichissement temporel :
#  - On **construit date_debut** (ISO → ts) ; si absente, on tente depuis 'jour' (00:00),
#    sinon on laisse null (et 'jour'/'mois' retomberont sur kafka_ts en ultime fallback).
#  - **jour & mois** sont **dérivés de date_debut** (ou kafka_ts si null) → cohérence affichage.
enriched = (
    clean
    # 1) compléter date_debut à partir de 'jour' si besoin
    .withColumn("date_debut",
        fcoalesce(
            col("date_debut"),
            to_timestamp(parse_day_any(col("jour")))
        )
    )
    .withColumn("base_ts", col("date_debut"))

    # 2) 'mois' STRICTEMENT depuis date_debut (fallback kafka_ts seulement si null)
    .withColumn(
        "mois",
        when(col("date_debut").isNotNull(), date_format(col("date_debut"), "yyyy-MM"))
        .otherwise(date_format(col("kafka_ts"), "yyyy-MM"))
    )

    # 3) 'jour' STRICTEMENT depuis date_debut (fallback kafka_ts seulement si null)
    .withColumn(
        "jour",
        when(col("date_debut").isNotNull(), date_format(col("date_debut"), "yyyy-MM-dd"))
        .otherwise(date_format(col("kafka_ts"), "yyyy-MM-dd"))
    )

    # 4) bornage partition (sécurité)
    .withColumn("mois_year", substring(col("mois"), 1, 4).cast("int"))
    .withColumn(
        "mois",
        when((col("mois_year") >= lit(PARTITION_YEAR_MIN)) & (col("mois_year") <= lit(PARTITION_YEAR_MAX)), col("mois"))
        .otherwise(date_format(col("kafka_ts"), "yyyy-MM"))
    )

    # 5) watermark & métriques complémentaires
    .withColumn("event_ts", fcoalesce(col("base_ts"), col("kafka_ts")))
    .withColumn(
        "vitesse_kmh",
        when((col("distance_km").isNotNull()) & (col("temps_sec").isNotNull()) & (col("temps_sec") > 0),
             col("distance_km") / (col("temps_sec") / lit(3600.0))
        )
    )
    .withColumn("duree_min", when(col("temps_sec").isNotNull(), (col("temps_sec")/60).cast("int")))
    .withColumn("is_future", when(col("date_debut").isNotNull() & (col("date_debut") > current_timestamp()), lit(True)).otherwise(lit(False)))

    # 6) flags QC (diagnostic)
    .withColumn(
        "qc_flags",
        concat_ws(",",
            when(col("type_activite") == "Inconnue", lit("default_type")),
            when(col("date_debut").isNull(),          lit("date_missing")),
            when(col("distance_km").isNull(),         lit("distance_null")),
            when(col("temps_sec").isNull(),           lit("temps_null")),
            when(col("is_future"),                    lit("future_date"))
        )
    )
)

# Dédup par uid (tolère les messages tardifs jusqu'à 1 jour)
dedup = enriched.withWatermark("event_ts", "1 day").dropDuplicates(["uid"])

# ------------------------------------------------------------------------------------------
# 6) QC split → Bronze/Quarantine → Notifications groupées (style historique)
# ------------------------------------------------------------------------------------------
def split_valid_invalid_soft(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    QC souple : uid + (distance_km>0 ou temps_sec>0) + pas futur.
    Idéal pour de la démo "tolérante".
    """
    has_uid    = df["uid"].isNotNull()
    has_meas   = (col("distance_km").isNotNull() & (col("distance_km") > 0)) | (col("temps_sec").isNotNull() & (col("temps_sec") > 0))
    not_future = ~col("is_future")
    ok = has_uid & has_meas & not_future
    valid_rows   = df.where(ok)
    invalid_rows = df.where(~ok).withColumn(
        "reason",
        when(~has_uid, lit("no_uid"))
        .when(~has_meas, lit("no_measure"))
        .when(~not_future, lit("future_date"))
        .otherwise(lit("unknown"))
    )
    return valid_rows, invalid_rows

def split_valid_invalid_strict(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    QC strict :
      - champs essentiels présents,
      - non négatifs,
      - pas futur,
      - vitesse_kmh plausible selon grandes familles (fallback générique).
    """
    essentials_ok = (
        df["uid"].isNotNull() &
        df["type_activite"].isNotNull() &
        df["distance_km"].isNotNull() & df["temps_sec"].isNotNull() &
        df["jour"].isNotNull() &
        (df["date_debut"].isNotNull() | df["jour"].rlike(r"^\d{4}-\d{2}-\d{2}$"))
    )
    non_negative_ok = (col("distance_km") >= 0) & (col("temps_sec") >= 0)
    not_future = ~col("is_future")

    # bornes “familles” (km/h)
    velo_like  = col("type_activite").isin("Vélo","Roller","Trottinette","Skateboard","Ski alpin","Ski de fond","Snowboard","Kitesurf")
    run_hike   = col("type_activite").isin("Course à pied","Marche","Randonnée","Marche nordique","Parkour")
    water_row  = col("type_activite").isin("Natation","Aviron","Canoë-kayak","Stand-up paddle","Surf")
    generic_ok = (col("vitesse_kmh") >= 0.5) & (col("vitesse_kmh") <= 60)

    speed_ok = (
        when(velo_like,  (col("vitesse_kmh") >= 6)  & (col("vitesse_kmh") <= 50))
        .when(run_hike,  (col("vitesse_kmh") >= 3)  & (col("vitesse_kmh") <= 20))
        .when(water_row, (col("vitesse_kmh") >= 1.5)& (col("vitesse_kmh") <= 18))
        .otherwise(generic_ok)
    )

    ok = essentials_ok & non_negative_ok & not_future & speed_ok
    valid_rows   = df.where(ok)
    invalid_rows = df.where(~ok).withColumn(
        "reason",
        when(~essentials_ok,   lit("missing_fields"))
        .when(~non_negative_ok,lit("negative_values"))
        .when(~not_future,     lit("future_date"))
        .when(~speed_ok,       lit("speed_out_of_bounds"))
        .otherwise(lit("unknown"))
    )
    return valid_rows, invalid_rows

# ---- Debounce (anti-spam : une "vague" de groupes max toutes NTFY_DEBOUNCE_SECONDS) ----
def _can_send_now() -> bool:
    try:
        with open(RESUME_LOCK_PATH, "r") as f:
            last = json.load(f).get("ts", 0)
    except Exception:
        last = 0
    return (time.time() - last) >= NTFY_DEBOUNCE_SECONDS

def _mark_sent_now():
    try:
        os.makedirs(os.path.dirname(RESUME_LOCK_PATH), exist_ok=True)
        with open(RESUME_LOCK_PATH, "w") as f:
            json.dump({"ts": time.time()}, f)
    except Exception:
        pass

# ---- Construction de phrases "humaines" (jour + semaine + mois FR, emojis & lieu) ----
def _format_messages(rows: List[dict]) -> List[str]:
    """
    Transforme une liste de dicts (prenom, type_activite, distance_km, temps_sec, jour, date_debut, lieu)
    en lignes de notification "humaines", prêtes à être regroupées.
    - Utilise **date_debut** comme source calendrier (fallback 'jour' si absent).
    - Correction “à vélo” (vs “en vélo”).
    """
    def _pick(seq, key: str) -> str:
        if not seq:
            return ""
        return seq[abs(hash(key)) % len(seq)]

    JOURS_FR = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]
    MOIS_FR  = ["janvier","février","mars","avril","mai","juin","juillet","août","septembre","octobre","novembre","décembre"]

    out: List[str] = []
    for r in rows:
        prenom = str(r.get("prenom", "—")).strip()
        sport  = str(r.get("type_activite", "—")).strip().lower()

        # distance (km) et durée (min)
        try:
            d = float(r.get("distance_km") or 0.0)
        except Exception:
            d = 0.0
        dist_txt = f"{d:.1f} km" if d > 0 else "— km"

        try:
            t = int(r.get("temps_sec") or 0)
            mins = max(0, t // 60)
        except Exception:
            mins = 0
        mins_txt = f"{mins} min" if mins > 0 else "— min"

        # emoji
        emoji = _pick(EMOJIS_SPORTIFS, sport + prenom) or "✨"

        # lieu
        lieu_val = (r.get("lieu") or "").strip()
        lieu_txt = f" {lieu_val}" if lieu_val else ""

        # Préfixe calendrier → priorité à date_debut
        prefix_cal = ""
        try:
            raw_dt = r.get("date_debut")
            dt = None
            if raw_dt is not None:
                try:
                    dt = raw_dt.to_pydatetime()  # pandas.Timestamp
                except Exception:
                    pass
            if dt is None:
                import pandas as pd
                dt = pd.to_datetime(raw_dt, errors="coerce")
                if getattr(dt, "to_pydatetime", None):
                    dt = dt.to_pydatetime()
            if not dt:
                raw_day = (r.get("jour") or "").strip()
                if raw_day:
                    dt = datetime.strptime(raw_day, "%Y-%m-%d")

            if dt:
                jour_fr = JOURS_FR[dt.weekday()].capitalize()
                mois_fr = MOIS_FR[dt.month - 1]
                semaine = dt.isocalendar()[1]
                prefix_cal = f"{jour_fr} {dt.day} {mois_fr} (Semaine {semaine:02d}) — "
        except Exception:
            pass

        # “à vélo” plutôt que “en vélo”
        sport_en = "à vélo" if sport == "vélo" else f"en {sport}"

        templates = [
            f"{prefix_cal}{emoji} Bravo {prenom} ! Tu viens de faire {dist_txt} de {sport} en {mins_txt}{lieu_txt}",
            f"{prefix_cal}{emoji} {prenom} a bien transpiré : {dist_txt} en {mins_txt}{lieu_txt}",
            f"{prefix_cal}{emoji} {prenom} s’est donné(e) à fond en {sport}{lieu_txt} ({mins_txt})",
            f"{prefix_cal}{emoji} Belle performance de {prenom} : {dist_txt} parcourus en {mins_txt} !",
            f"{prefix_cal}{emoji} {prenom} garde la forme avec une session de {sport} de {dist_txt}{lieu_txt} 🏞️",
            f"{prefix_cal}{emoji} {prenom} vient de boucler {dist_txt} {sport_en}{lieu_txt}, chapeau 🎩",
            f"{prefix_cal}{emoji} {prenom} enchaîne les défis : {mins_txt} de {sport} pour {dist_txt} !",
            f"{prefix_cal}{emoji} {prenom} ne lâche rien : {dist_txt} de {sport} sous le soleil ☀️",
            f"{prefix_cal}{emoji} {prenom} a bien mérité une pause après {mins_txt} de {sport}{lieu_txt}",
            f"{prefix_cal}{emoji} Excellente session de {sport} pour {prenom} : {dist_txt} parcourus 💥",
        ]

        msg = templates[abs(hash(prenom + sport + str(mins))) % len(templates)]
        out.append(msg)

    return out

def _send_grouped_messages(sample_rows_pdf, total: int):
    """
    Envoie plusieurs notifications “style historique” :
    - jusqu’à NTFY_MAX_PER_BATCH lignes au total,
    - regroupées par NTFY_GROUP_SIZE lignes par message,
    - pause NTFY_GROUP_COOLDOWN_S entre messages (évite HTTP 429).
    """
    if not (NTFY_ENABLED and NTFY_HELPER_OK):
        return

    cap = max(0, min(NTFY_MAX_PER_BATCH, total))
    if cap == 0:
        return

    rows = sample_rows_pdf.head(cap).to_dict(orient="records")
    messages = _format_messages(rows)

    for i in range(0, len(messages), NTFY_GROUP_SIZE):
        bloc = messages[i:i + NTFY_GROUP_SIZE]
        body = "\n".join(bloc)
        header = f"✅ Activités validées : {total} nouvelles entrées — extrait ({i+1}-{i+len(bloc)}):"
        envoyer_message_texte(header + "\n" + body, topic=NTFY_TOPIC_ACTIVITES)
        time.sleep(max(0, NTFY_GROUP_COOLDOWN_S))

# ------------------------------------------------------------------------------------------
# 7) Traitement par micro-batch
# ------------------------------------------------------------------------------------------
def process_batch(df: DataFrame, epoch_id: int):
    """
    Micro-batch :
      1) QC split (souple/strict selon ENV)
      2) Écritures Delta (bronze & quarantine)
      3) Notifications groupées “humaines” (debounce + cap global + cap par employé)
    """
    if df.rdd.isEmpty():
        logger.info(f"[{epoch_id}] batch vide.")
        return

    # 1) QC split
    splitter = split_valid_invalid_strict if STRICT_QC else split_valid_invalid_soft
    valid_rows, invalid_rows = splitter(df)

    n_valid   = valid_rows.count()
    n_invalid = invalid_rows.count()

    # 2) Bronze (append, partition yyyy-MM)
    #    ⚠️ Pour éviter des erreurs de “schema mismatch” dans un tableau déjà créé
    #       (avec des colonnes intermédiaires), on écrit **valid_rows tel quel**
    #       avec mergeSchema=true, partitionBy("mois").
    if n_valid > 0:
        (valid_rows.coalesce(COALESCE_N)
         .write.format("delta").mode("append")
         .option("mergeSchema", "true")
         .option("maxRecordsPerFile", str(MAX_RECORDS_FILE))
         .partitionBy("mois")
         .save(DELTA_PATH_ACTIVITES))

    # Quarantine (append, partition reason/mois)
    if n_invalid > 0:
        (invalid_rows.coalesce(1)
         .write.format("delta").mode("append")
         .option("mergeSchema", "true")
         .partitionBy("reason", "mois")
         .save(QUARANTINE_PATH))

    # Logs de synthèse
    logger.info(f"[{epoch_id}] valid={n_valid} | invalid={n_invalid} → bronze={DELTA_PATH_ACTIVITES}")
    try:
        bd = invalid_rows.groupBy("reason").count().collect()
        if bd:
            logger.warning(f"[{epoch_id}] invalid breakdown: " + " | ".join(f"{r['reason']}={r['count']}" for r in bd))
    except Exception:
        pass

    # 3) Notifications groupées (style “humain”) — anti-spam via debounce + cap par employé
    if NTFY_ENABLED and NTFY_HELPER_OK and n_valid > 0:
        try:
            if _can_send_now():
                # a) on ne garde que les colonnes utiles aux messages
                sel = valid_rows.select("id_salarie","prenom","type_activite","distance_km","temps_sec","jour","date_debut","lieu")

                # b) limiter à NTFY_MAX_PER_EMP / employé (row_number over id_salarie, date_debut desc)
                w = Window.partitionBy("id_salarie").orderBy(col("date_debut").desc_nulls_last())
                capped = sel.withColumn("rn", row_number().over(w)).where(col("rn") <= lit(NTFY_MAX_PER_EMP)).drop("rn")

                # c) cap global du batch
                total = capped.count()
                sample_pdf = capped.limit(min(total, NTFY_MAX_PER_BATCH)).toPandas()

                _send_grouped_messages(sample_pdf, total=total)
                _mark_sent_now()
                logger.info(f"[{epoch_id}] notifications envoyées (≤{NTFY_MAX_PER_BATCH} lignes, ≤{NTFY_MAX_PER_EMP}/employé, groupes de {NTFY_GROUP_SIZE}).")
            else:
                logger.info(f"[{epoch_id}] notifications non envoyées (debounce actif {NTFY_DEBOUNCE_SECONDS}s).")
        except Exception as e:
            logger.warning(f"[{epoch_id}] notifications ignorées (non bloquant) : {e}")

# ------------------------------------------------------------------------------------------
# 8) Démarrage du streaming
# ------------------------------------------------------------------------------------------
logger.info("🚀 Démarrage ingestion Bronze + QC + notifications groupées (style humain & historique).")

main_query = (
    dedup.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)  # ⚠ un seul job actif par checkpoint
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .foreachBatch(process_batch)
    .start()
)

# On garde la requête DLQ vivante (sinon GC possible)
_ = dlq_query

main_query.awaitTermination()
