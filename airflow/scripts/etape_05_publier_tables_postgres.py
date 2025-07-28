# ==========================================================================================
# Script      : etape_05_publier_tables_postgres
# Objectif    : Créer ou mettre à jour la publication PostgreSQL pour Debezium (CDC)
#               Inclut toutes les tables du schéma ciblé + REPLICA IDENTITY FULL
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger
import warnings
warnings.filterwarnings("ignore", message=".*SQLAlchemy 2.0.*")

# ==========================================================================================
# 1. Chargement de l’environnement (.env Docker monté dans /opt/airflow/.env)
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
# 2. Fonctions de gestion des publications
# ==========================================================================================

def get_tables_sportdata(engine):
    """
    Récupère la liste des tables dans le schéma cible.
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
    Vérifie si la publication existe déjà dans PostgreSQL.
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
    Crée une nouvelle publication PostgreSQL vide.
    """
    query = f"CREATE PUBLICATION {publication_name};"
    with engine.connect() as conn:
        conn.execute(text(query))
        logger.success(f"✅ Publication PostgreSQL créée : {publication_name}")

def set_replica_identity(engine, schema, table):
    """
    Applique REPLICA IDENTITY FULL à une table pour permettre le CDC sans clé primaire.
    """
    query = f'ALTER TABLE "{schema}"."{table}" REPLICA IDENTITY FULL;'
    with engine.connect() as conn:
        conn.execute(text(query))
        logger.info(f"🔐 REPLICA IDENTITY FULL appliqué à {schema}.{table}")

def add_table_to_publication(engine, publication_name, schema, table):
    """
    Ajoute une table à une publication existante.
    """
    query = f'ALTER PUBLICATION {publication_name} ADD TABLE "{schema}"."{table}";'
    with engine.connect() as conn:
        conn.execute(text(query))
        logger.success(f"➕ Table ajoutée à la publication : {schema}.{table}")

def get_tables_already_published(engine, publication_name):
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
# 3. Pipeline principal
# ==========================================================================================

def initialiser_publication_postgres():
    logger.info("=== Initialisation de la publication PostgreSQL Debezium ===")
    engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    logger.debug(f"🧩 Connexion PostgreSQL → {POSTGRES_USER}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

    # Vérification ou création de la publication
    if publication_exists(engine, CDC_PUBLICATION):
        logger.info(f"📌 Publication déjà existante : {CDC_PUBLICATION}")
    else:
        create_publication(engine, CDC_PUBLICATION)

    # Récupération des tables dans le schéma
    toutes_tables = get_tables_sportdata(engine)

    if not toutes_tables:
        logger.warning(f"⚠️ Aucune table trouvée dans le schéma '{CDC_SCHEMA}'. Vérifiez que le schéma est bien renseigné et que les tables sont bien créées dans PostgreSQL.")
        return

    # Filtrage des tables non encore publiées
    deja_publiees = get_tables_already_published(engine, CDC_PUBLICATION)
    nouvelles_tables = [t for t in toutes_tables if (CDC_SCHEMA, t) not in deja_publiees]

    if not nouvelles_tables:
        logger.info("✅ Publication déjà à jour : toutes les tables du schéma sont déjà incluses.")
    else:
        logger.info(f"📋 Tables à publier : {nouvelles_tables}")
        for table in nouvelles_tables:
            set_replica_identity(engine, CDC_SCHEMA, table)
            add_table_to_publication(engine, CDC_PUBLICATION, CDC_SCHEMA, table)

    # Résumé final
    tables_finales = get_tables_already_published(engine, CDC_PUBLICATION)
    for sch, tab in tables_finales:
        logger.info(f"📌 Table publiée : {sch}.{tab}")
        logger.success(f"✅ {len(tables_finales)} table(s) publiées dans la publication '{CDC_PUBLICATION}'")
    logger.success("🎯 Synchronisation de publication PostgreSQL terminée.")

# ==========================================================================================
# 4. Entrée pour exécution directe ou via Airflow
# ==========================================================================================

def main():
    """Point d’entrée pour le DAG Airflow"""
    initialiser_publication_postgres()

if __name__ == "__main__":
    try:
        main()
        logger.info("🚀 Démarrage du script de publication PostgreSQL via CDC (Debezium)")

    except Exception as e:
        logger.error(f"❌ Erreur publication PostgreSQL : {e}")
        raise
