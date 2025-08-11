# ==========================================================================================
# Script      : etape_05_publier_tables_postgres.py
# Objectif    : Créer ou mettre à jour la publication PostgreSQL pour Debezium (CDC)
#               Inclut toutes les tables du schéma ciblé + REPLICA IDENTITY FULL
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger

# ==========================================================================================
# 1. Chargement des variables d’environnement (.env monté dans /opt/airflow/.env)
# ==========================================================================================

load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

REQUIRED_VARS = ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB"]
missing_vars = [var for var in REQUIRED_VARS if os.getenv(var) is None]
if missing_vars:
    raise EnvironmentError(f"❌ Variable(s) d’environnement manquante(s) : {', '.join(missing_vars)}")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

CDC_PUBLICATION = os.getenv("CDC_PUBLICATION", "debezium_publication")
CDC_SCHEMA = os.getenv("CDC_SCHEMA", "sportdata")
logger.debug(f"📁 Schéma ciblé pour publication : {CDC_SCHEMA}")

# ==========================================================================================
# 2. Fonctions utilitaires pour Debezium
# ==========================================================================================

def get_tables_sportdata(engine):
    """
    Récupère la liste des tables présentes dans le schéma cible.
    """
    query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{CDC_SCHEMA}' AND table_type = 'BASE TABLE'
    """
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(text(query)).fetchall()]

def publication_exists(engine, publication_name):
    """
    Vérifie si une publication PostgreSQL existe déjà.
    """
    query = f"""
        SELECT 1
        FROM pg_catalog.pg_publication
        WHERE pubname = '{publication_name}'
    """
    with engine.connect() as conn:
        return conn.execute(text(query)).fetchone() is not None

def create_publication(engine, publication_name):
    """
    Crée une publication vide dans PostgreSQL.
    """
    query = f"CREATE PUBLICATION {publication_name};"
    with engine.connect() as conn:
        conn.execute(text(query))
        logger.success(f"✅ Publication PostgreSQL créée : {publication_name}")

def set_replica_identity(engine, schema, table):
    """
    Applique REPLICA IDENTITY FULL à une table pour permettre la capture sans clé primaire.
    """
    query = f'ALTER TABLE "{schema}"."{table}" REPLICA IDENTITY FULL;'
    with engine.connect() as conn:
        conn.execute(text(query))
        logger.info(f"🔐 REPLICA IDENTITY FULL appliqué à {schema}.{table}")

def add_table_to_publication(engine, publication_name, schema, table):
    """
    Ajoute une table à une publication PostgreSQL existante.
    """
    query = f'ALTER PUBLICATION {publication_name} ADD TABLE "{schema}"."{table}";'
    with engine.connect() as conn:
        conn.execute(text(query))
        logger.success(f"➕ Table ajoutée à la publication : {schema}.{table}")

def get_tables_already_published(engine, publication_name):
    """
    Récupère les tables déjà associées à une publication PostgreSQL.
    """
    query = f"""
        SELECT n.nspname AS schema, c.relname AS table
        FROM pg_publication p
        JOIN pg_publication_rel pr ON p.oid = pr.prpubid
        JOIN pg_class c ON pr.prrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE p.pubname = '{publication_name}';
    """
    with engine.connect() as conn:
        return [(row[0], row[1]) for row in conn.execute(text(query)).fetchall()]

# ==========================================================================================
# 3. Pipeline principal : création ou mise à jour de la publication
# ==========================================================================================

# ==========================================================================================
# 3. Pipeline principal : création ou mise à jour de la publication
# ==========================================================================================

def initialiser_publication_postgres():
    logger.info("=== Initialisation de la publication PostgreSQL Debezium ===")

    engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    logger.debug(f"🧩 Connexion PostgreSQL → {POSTGRES_USER}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

    # Vérification/création publication
    try:
        if publication_exists(engine, CDC_PUBLICATION):
            logger.info(f"📌 Publication déjà existante : {CDC_PUBLICATION}")
        else:
            create_publication(engine, CDC_PUBLICATION)
    except Exception as e:
        logger.error(f"❌ Erreur lors de la vérification/création de la publication : {e}")
        raise

    # Vérification schéma cible
    query_schema = f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{CDC_SCHEMA}'"
    with engine.connect() as conn:
        if not conn.execute(text(query_schema)).fetchone():
            logger.error(f"❌ Le schéma '{CDC_SCHEMA}' n’existe pas dans la base PostgreSQL.")
            return

    # Tables disponibles et déjà publiées
    toutes_tables = get_tables_sportdata(engine)
    deja_publiees = get_tables_already_published(engine, CDC_PUBLICATION)
    nouvelles_tables = [t for t in toutes_tables if (CDC_SCHEMA, t) not in deja_publiees]

    # Affichage des nouvelles tables
    if not nouvelles_tables:
        logger.info("✅ Aucune nouvelle table à publier : publication déjà à jour.")
    else:
        logger.info(f"📋 Tables à publier : {nouvelles_tables}")

    # Publication des nouvelles tables
    for table in nouvelles_tables:
        # Count avant publication
        query_count = f'SELECT COUNT(*) FROM "{CDC_SCHEMA}"."{table}"'
        with engine.connect() as conn:
            nb_lignes_avant = conn.execute(text(query_count)).scalar()

        if nb_lignes_avant == 0:
            logger.warning(f"⚠️ Table vide : {CDC_SCHEMA}.{table}")
        else:
            logger.info(f"🔢 {CDC_SCHEMA}.{table} → {nb_lignes_avant} ligne(s) avant publication")

        # Publication
        set_replica_identity(engine, CDC_SCHEMA, table)
        add_table_to_publication(engine, CDC_PUBLICATION, CDC_SCHEMA, table)

        # Count après publication
        with engine.connect() as conn:
            nb_lignes_apres = conn.execute(text(query_count)).scalar()

        # Affichage différentiel
        if nb_lignes_apres == nb_lignes_avant:
            logger.success(f"✅ Publication confirmée : {table} ({nb_lignes_apres} lignes)")
        else:
            logger.warning(f"📈 Différence détectée après publication : {table} → {nb_lignes_apres} lignes (vs {nb_lignes_avant})")

    # Résumé
    tables_finales = get_tables_already_published(engine, CDC_PUBLICATION)
    logger.info("📦 État final des tables publiées :")
    for sch, tab in sorted(tables_finales):
        query_count = f'SELECT COUNT(*) FROM "{sch}"."{tab}"'
        with engine.connect() as conn:
            count = conn.execute(text(query_count)).scalar()
        logger.info(f"📊 {sch}.{tab} → {count} ligne(s)")

    logger.success(f"🎯 {len(tables_finales)} table(s) publiées dans la publication '{CDC_PUBLICATION}'")

    logger.success("🎯 Synchronisation de publication PostgreSQL terminée.")

# ==========================================================================================
# 4. Point d’entrée CLI ou Airflow
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
