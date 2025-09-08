# ==========================================================================================
# Script      : etape_05_publier_tables_postgres.py
# Objectif    : Créer/mettre à jour la publication PostgreSQL pour Debezium (CDC)
#               Inclut toutes les tables du schéma ciblé. REPLICA IDENTITY FULL si pas de PK
#               (ou forcé par env). Options: DRY_RUN, includes/excludes, mode STRICT PG15+.
# Auteur      : Xavier Rousseau | août 2025
# ==========================================================================================
import os
from typing import List, Tuple, Set

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from loguru import logger

# ==========================================================================================
# 1) ENV
# ==========================================================================================

try:
    load_dotenv(dotenv_path="/opt/airflow/.env", override=True)
except Exception:
    pass
load_dotenv(dotenv_path=".env", override=True)

REQUIRED_VARS = ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB"]
missing_vars = [var for var in REQUIRED_VARS if not os.getenv(var)]
if missing_vars:
    raise EnvironmentError(f"❌ Variable(s) d’environnement manquante(s) : {', '.join(missing_vars)}")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")  # ⚠️ ne jamais logger cette valeur
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

CDC_PUBLICATION = os.getenv("CDC_PUBLICATION", "debezium_publication")
CDC_SCHEMA = os.getenv("CDC_SCHEMA", "sportdata")

# Options “ops”
DRY_RUN = os.getenv("CDC_DRY_RUN", "false").lower() in {"1", "true", "yes"}
CDC_TABLES_EXCLUDE = {t.strip() for t in os.getenv("CDC_TABLES_EXCLUDE", "").split(",") if t.strip()}
CDC_TABLES_INCLUDE = {t.strip() for t in os.getenv("CDC_TABLES_INCLUDE", "").split(",") if t.strip()}
CDC_SYNC_STRICT = os.getenv("CDC_SYNC_STRICT", "false").lower() in {"1", "true", "yes"}
CDC_FORCE_FULL = {t.strip() for t in os.getenv("CDC_FORCE_FULL", "").split(",") if t.strip()}

# Logs d'état (non sensibles uniquement)
logger.debug(f"📁 Schéma ciblé : {CDC_SCHEMA} | Publication : {CDC_PUBLICATION}")
if DRY_RUN: logger.info("🧪 Mode DRY_RUN actif — aucune modification ne sera exécutée.")
if CDC_TABLES_INCLUDE: logger.debug(f"✅ Inclusions: {sorted(CDC_TABLES_INCLUDE)}")
if CDC_TABLES_EXCLUDE: logger.debug(f"⛔ Exclusions: {sorted(CDC_TABLES_EXCLUDE)}")
if CDC_SYNC_STRICT: logger.debug("🧭 Mode STRICT activé (SET TABLE si PG15+, sinon DROP/ADD).")
if CDC_FORCE_FULL: logger.debug(f"🔐 FULL forcé pour: {sorted(CDC_FORCE_FULL)}")


# ==========================================================================================
# 2) Helpers SQL
# ==========================================================================================

def qident(name: str) -> str:
    """Quote sécurisé pour identifiants SQL (schéma/table)."""
    return '"' + name.replace('"', '""') + '"'

def get_engine() -> Engine:
    # keepalive + statement_timeout pour robustesse réseau
    return create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
        pool_pre_ping=True, future=True,
        connect_args={
            "application_name": "sportdata_cdc_pub",
            "keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 5,
            "options": "-c statement_timeout=60000",
        }
    )

def exec_sql(conn, sql: str, params=None):
    """Exécute (ou log) une instruction SQL en respectant DRY_RUN."""
    if DRY_RUN:
        logger.info(f"[DRY_RUN] {sql}")
        return None
    return conn.execute(text(sql), params or {})

def get_tables_in_schema(engine: Engine, schema: str) -> List[str]:
    query = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(query, {"schema": schema}).fetchall()]

def publication_exists(engine: Engine, publication_name: str) -> bool:
    query = text("SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = :p")
    with engine.connect() as conn:
        return conn.execute(query, {"p": publication_name}).fetchone() is not None

def create_publication(engine: Engine, publication_name: str):
    # Inclut tous les DML + TRUNCATE (Debezium-friendly)
    sql = f"CREATE PUBLICATION {qident(publication_name)} WITH (publish = 'insert, update, delete, truncate');"
    with engine.begin() as conn:
        exec_sql(conn, sql)
    logger.success(f"✅ Publication PostgreSQL créée : {publication_name}")

def get_tables_in_publication(engine: Engine, publication_name: str) -> Set[Tuple[str, str]]:
    query = text("""
        SELECT n.nspname AS schema, c.relname AS table
        FROM pg_publication p
        JOIN pg_publication_rel pr ON p.oid = pr.prpubid
        JOIN pg_class c ON pr.prrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE p.pubname = :p;
    """)
    with engine.connect() as conn:
        return {(r[0], r[1]) for r in conn.execute(query, {"p": publication_name}).fetchall()}

def table_has_primary_key(engine: Engine, schema: str, table: str) -> bool:
    query = text("""
        SELECT 1
        FROM information_schema.table_constraints tc
        WHERE tc.table_schema = :schema
          AND tc.table_name = :table
          AND tc.constraint_type = 'PRIMARY KEY'
        LIMIT 1;
    """)
    with engine.connect() as conn:
        return conn.execute(query, {"schema": schema, "table": table}).fetchone() is not None

def set_replica_identity_full(engine: Engine, schema: str, table: str):
    with engine.begin() as conn:
        exec_sql(conn, f'ALTER TABLE {qident(schema)}.{qident(table)} REPLICA IDENTITY FULL;')
    logger.info(f"🔐 REPLICA IDENTITY FULL appliqué à {schema}.{table}")

def set_replica_identity_default(engine: Engine, schema: str, table: str):
    with engine.begin() as conn:
        exec_sql(conn, f'ALTER TABLE {qident(schema)}.{qident(table)} REPLICA IDENTITY DEFAULT;')
    logger.debug(f"🔐 REPLICA IDENTITY DEFAULT conservé (PK présente) pour {schema}.{table}")

def add_table_to_publication(engine: Engine, publication_name: str, schema: str, table: str):
    with engine.begin() as conn:
        exec_sql(conn, f'ALTER PUBLICATION {qident(publication_name)} ADD TABLE {qident(schema)}.{qident(table)};')
    logger.success(f"➕ Table ajoutée à la publication : {schema}.{table}")

def drop_table_from_publication(engine: Engine, publication_name: str, schema: str, table: str):
    with engine.begin() as conn:
        exec_sql(conn, f'ALTER PUBLICATION {qident(publication_name)} DROP TABLE {qident(schema)}.{qident(table)};')
    logger.info(f"➖ Table retirée de la publication : {schema}.{table}")

def set_publication_tables_strict(engine: Engine, publication_name: str, schema: str, tables: List[str]) -> bool:
    """
    Tente PG15+ : SET TABLE (remplace complètement la liste).
    Retourne True si OK, False si fallback nécessaire.
    """
    try:
        with engine.begin() as conn:
            tables_sql = ", ".join(f"{qident(schema)}.{qident(t)}" for t in tables) or "NONE"
            exec_sql(conn, f"ALTER PUBLICATION {qident(publication_name)} SET TABLE {tables_sql};")
        logger.success("🧭 Publication synchronisée en mode STRICT (SET TABLE).")
        return True
    except Exception as e:
        logger.debug(f"SET TABLE indisponible (PG<15 ?) ou erreur : {e}")
        return False

# ==========================================================================================
# 3) Vérifs serveur & verrou
# ==========================================================================================

def check_pg_logical_capabilities(engine: Engine):
    """Alerte (non bloquant) si wal_level/logical réplication semblent insuffisants."""
    try:
        with engine.connect() as c:
            lv = c.execute(text("SHOW wal_level")).scalar()
            mr = c.execute(text("SHOW max_replication_slots")).scalar()
            sr = c.execute(text("SHOW max_wal_senders")).scalar()
        if str(lv).lower() != "logical":
            logger.warning(f"⚠️ wal_level={lv} (attendu: logical)")
        if int(mr) < 1 or int(sr) < 1:
            logger.warning(f"⚠️ Slots/senders faibles: slots={mr}, senders={sr}")
    except Exception as e:
        logger.debug(f"Check logical replication ignoré: {e}")

from contextlib import contextmanager
@contextmanager
def advisory_lock(engine: Engine, key: int = 934251):
    """Empêche deux runs concurrents (non bloquant si DRY_RUN)."""
    if DRY_RUN:
        yield
        return
    with engine.begin() as conn:
        got = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": key}).scalar()
        if not got:
            raise RuntimeError("Un autre job tient déjà le verrou (advisory lock).")
        try:
            yield
        finally:
            conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})

# ==========================================================================================
# 4) Pipeline
# ==========================================================================================

def initialiser_publication_postgres():
    logger.info("=== Initialisation de la publication PostgreSQL Debezium ===")
    engine = get_engine()
    logger.debug(f"🧩 Connexion PostgreSQL → {POSTGRES_USER}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

    check_pg_logical_capabilities(engine)

    with advisory_lock(engine):
        # Publication
        try:
            if publication_exists(engine, CDC_PUBLICATION):
                logger.info(f"📌 Publication déjà existante : {CDC_PUBLICATION}")
            else:
                create_publication(engine, CDC_PUBLICATION)
        except Exception as e:
            logger.error(f"❌ Erreur publication (existence/création) : {e}")
            raise

        # Schéma présent ?
        with engine.connect() as conn:
            if not conn.execute(
                text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s"),
                {"s": CDC_SCHEMA}
            ).fetchone():
                logger.error(f"❌ Le schéma '{CDC_SCHEMA}' n’existe pas dans la base PostgreSQL.")
                return

        # Tables candidates (incl/excl)
        toutes = get_tables_in_schema(engine, CDC_SCHEMA)
        if CDC_TABLES_INCLUDE:
            toutes = [t for t in toutes if t in CDC_TABLES_INCLUDE]
        if CDC_TABLES_EXCLUDE:
            toutes = [t for t in toutes if t not in CDC_TABLES_EXCLUDE]

        deja = get_tables_in_publication(engine, CDC_PUBLICATION)
        nouvelles = [t for t in toutes if (CDC_SCHEMA, t) not in deja]
        a_retirer = [(s, t) for (s, t) in deja if s == CDC_SCHEMA and t not in toutes]

        if not nouvelles:
            logger.info("✅ Aucune nouvelle table à publier (ajouts).")
        else:
            logger.info(f"📋 Tables à publier : {nouvelles}")

        # Mode STRICT : refléter EXACTEMENT la liste (PG15+ SET TABLE sinon DROP/ADD)
        if CDC_SYNC_STRICT:
            ok = set_publication_tables_strict(engine, CDC_PUBLICATION, CDC_SCHEMA, toutes)
            if not ok:
                for s, t in a_retirer:
                    drop_table_from_publication(engine, CDC_PUBLICATION, s, t)

        # Publier les nouvelles tables
        for table in nouvelles:
            # Volumétrie (info)
            with engine.connect() as conn:
                nb_lignes = conn.execute(
                    text(f"SELECT COUNT(*) FROM {qident(CDC_SCHEMA)}.{qident(table)}")
                ).scalar()
            if nb_lignes == 0:
                logger.warning(f"⚠️ Table vide : {CDC_SCHEMA}.{table}")
            else:
                logger.info(f"🔢 {CDC_SCHEMA}.{table} → {nb_lignes} ligne(s)")

            # REPLICA IDENTITY
            try:
                if table in CDC_FORCE_FULL:
                    set_replica_identity_full(engine, CDC_SCHEMA, table)
                elif table_has_primary_key(engine, CDC_SCHEMA, table):
                    set_replica_identity_default(engine, CDC_SCHEMA, table)
                else:
                    set_replica_identity_full(engine, CDC_SCHEMA, table)
            except Exception as e:
                logger.warning(f"⚠️ Impossible de définir REPLICA IDENTITY sur {CDC_SCHEMA}.{table} : {e}")

            # Ajout à la publication
            try:
                add_table_to_publication(engine, CDC_PUBLICATION, CDC_SCHEMA, table)
            except Exception as e:
                logger.warning(f"ℹ️ Ajout ignoré pour {CDC_SCHEMA}.{table} (probablement déjà présent) : {e}")

        # Résumé final
        tables_finales = sorted(get_tables_in_publication(engine, CDC_PUBLICATION))
        logger.info("📦 État final des tables publiées :")
        with engine.connect() as conn:
            for sch, tab in tables_finales:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {qident(sch)}.{qident(tab)}")).scalar()
                logger.info(f"📊 {sch}.{tab} → {count} ligne(s)")
        logger.success(f"🎯 {len(tables_finales)} table(s) publiées dans '{CDC_PUBLICATION}'")
        logger.success("🎯 Synchronisation de publication PostgreSQL terminée.")

# ==========================================================================================
# 5) Entrée CLI
# ==========================================================================================

def main():
    logger.info("🚀 Démarrage du script de publication PostgreSQL via CDC (Debezium)")
    initialiser_publication_postgres()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Erreur publication PostgreSQL : {e}")
        raise
