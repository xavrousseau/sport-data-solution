# ==========================================================================================
# Script      : ajouter_tables_publication.py
# Objectif    : Créer ou mettre à jour la publication PostgreSQL pour Debezium (CDC)
#               Inclut toutes les tables du schéma ciblé + REPLICA IDENTITY FULL
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger

# ==========================================================================================
# 1. Chargement de l'environnement (.env Docker monté dans /opt/airflow/.env)
# ==========================================================================================
load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

# Vérification défensive des variables PostgreSQL
REQUIRED_VARS = ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB"]
missing_vars = [var for var in REQUIRED_VARS if os.getenv(var) is None]
if missing_vars:
    raise EnvironmentError(f"❌ Variable(s) d’environnement manquante(s) : {', '.join(missing_vars)}")

# Connexion PostgreSQL
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

DB_CONN_STRING = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Paramètres de publication
CDC_PUBLICATION = os.getenv("CDC_PUBLICATION", "debezium_publication")
CDC_SCHEMA = os.getenv("CDC_SCHEMA", "sportdata")

# ==========================================================================================
# 2. Fonctions de gestion des publications
# ==========================================================================================

def get_tables_sportdata(engine):
    """Retourne la liste des tables du schéma sportdata (hors tables système)."""
    query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{CDC_SCHEMA}' AND table_type = 'BASE TABLE'
    """
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(text(query)).fetchall()]

def publication_exists(engine, publication_name):
    """Vérifie si la publication PostgreSQL existe déjà."""
    query = f"""
        SELECT 1
        FROM pg_catalog.pg_publication
        WHERE pubname = '{publication_name}'
    """
    with engine.connect() as conn:
        return conn.execute(text(query)).fetchone() is not None

def create_publication(engine, publication_name):
    """Crée une nouvelle publication PostgreSQL."""
    query = f"CREATE PUBLICATION {publication_name};"
    with engine.connect() as conn:
        conn.execute(text(query))
        logger.success(f"✅ Publication PostgreSQL créée : {publication_name}")

def set_replica_identity(engine, schema, table):
    """Active REPLICA IDENTITY FULL pour suivre tous les changements (nécessaire pour Debezium)."""
    query = f'ALTER TABLE "{schema}"."{table}" REPLICA IDENTITY FULL;'
    with engine.connect() as conn:
        conn.execute(text(query))
        logger.info(f"🔐 REPLICA IDENTITY FULL appliqué à {schema}.{table}")

def add_table_to_publication(engine, publication_name, schema, table):
    """Ajoute une table à la publication PostgreSQL."""
    query = f'ALTER PUBLICATION {publication_name} ADD TABLE "{schema}"."{table}";'
    with engine.connect() as conn:
        conn.execute(text(query))
        logger.success(f"➕ Table ajoutée à la publication : {schema}.{table}")

def get_tables_already_published(engine, publication_name):
    """Liste les tables déjà incluses dans une publication."""
    query = f"""
        SELECT c.relname
        FROM pg_publication p
        JOIN pg_publication_rel pr ON p.oid = pr.prpubid
        JOIN pg_class c ON pr.prrelid = c.oid
        WHERE p.pubname = '{publication_name}';
    """
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(text(query)).fetchall()]

# ==========================================================================================
# 3. Pipeline principal
# ==========================================================================================

def initialiser_publication_postgres():
    logger.info("=== Initialisation de la publication PostgreSQL Debezium ===")
    engine = create_engine(DB_CONN_STRING)

    # Création ou vérification de la publication
    if publication_exists(engine, CDC_PUBLICATION):
        logger.info(f"📌 Publication déjà existante : {CDC_PUBLICATION}")
    else:
        create_publication(engine, CDC_PUBLICATION)

    # Liste des tables à publier
    toutes_tables = get_tables_sportdata(engine)
    deja_publiees = get_tables_already_published(engine, CDC_PUBLICATION)
    nouvelles_tables = [t for t in toutes_tables if t not in deja_publiees]

    if not nouvelles_tables:
        logger.info("✅ Aucune nouvelle table à publier. Publication à jour.")
    else:
        logger.info(f"📋 Tables à publier : {nouvelles_tables}")
        for table in nouvelles_tables:
            set_replica_identity(engine, CDC_SCHEMA, table)
            add_table_to_publication(engine, CDC_PUBLICATION, CDC_SCHEMA, table)

    logger.success("🎯 Synchronisation de publication PostgreSQL terminée.")

# ==========================================================================================
# 4. Entrée du script
# ==========================================================================================

if __name__ == "__main__":
    try:
        initialiser_publication_postgres()
    except Exception as e:
        logger.error(f"❌ Erreur publication PostgreSQL : {e}")
        raise
