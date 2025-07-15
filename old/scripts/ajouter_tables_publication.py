"""
Script pour ajouter automatiquement toutes les tables du schéma 'sportdata'
dans la publication Debezium 'debezium_publication'.

✔ Crée la publication si elle n'existe pas.
✔ Ajoute les tables automatiquement (évite les doublons).
✔ Active REPLICA IDENTITY FULL pour permettre le DELETE.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

# Connexion à PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", 5432),
    user=os.getenv("POSTGRES_USER", "user"),
    password=os.getenv("POSTGRES_PASSWORD", "password"),
    dbname=os.getenv("POSTGRES_DB", "sportdata")
)

SCHEMA = "sportdata"
PUBLICATION = "debezium_publication"

def publication_exists():
    """Vérifie si la publication existe déjà."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s;", (PUBLICATION,))
        return cur.fetchone() is not None

def create_publication():
    """Crée une publication vide avec insert/update/delete."""
    with conn.cursor() as cur:
        print(f"🛠️ Création de la publication '{PUBLICATION}'...")
        cur.execute(f"CREATE PUBLICATION {PUBLICATION} WITH (publish = 'insert, update, delete');")

def get_tables_in_schema():
    """Liste toutes les tables du schéma cible."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """, (SCHEMA,))
        return [row['table_name'] for row in cur.fetchall()]

def get_already_published_tables():
    """Liste les tables déjà présentes dans la publication."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT tablename
            FROM pg_publication_tables
            WHERE schemaname = %s AND pubname = %s;
        """, (SCHEMA, PUBLICATION))
        return [row['tablename'] for row in cur.fetchall()]

def add_table_to_publication(table):
    """Ajoute une table à la publication."""
    with conn.cursor() as cur:
        print(f"➕ Ajout de {SCHEMA}.{table} à la publication...")
        cur.execute(f'ALTER PUBLICATION {PUBLICATION} ADD TABLE {SCHEMA}."{table}";')

def set_replica_identity_full():
    """Configure REPLICA IDENTITY FULL pour toutes les tables du schéma."""
    with conn.cursor() as cur:
        print("🔐 Configuration REPLICA IDENTITY FULL pour chaque table...")
        tables = ["employes", "activites_sportives", "beneficiaires_primes_sport"]
        for table in tables:
            cur.execute(f'ALTER TABLE {SCHEMA}.{table} REPLICA IDENTITY FULL;')

def main():
    print(f"🔍 Vérification de l'existence de la publication '{PUBLICATION}'...")
    if not publication_exists():
        create_publication()
        conn.commit()
        print(f"✅ Publication '{PUBLICATION}' créée avec succès.")
    else:
        print(f"✅ Publication '{PUBLICATION}' déjà existante.")

    # Appliquer REPLICA IDENTITY FULL sur toutes les tables concernées
    set_replica_identity_full()
    conn.commit()

    print("🔍 Vérification des tables à publier...")
    all_tables = get_tables_in_schema()
    already_published = get_already_published_tables()
    to_add = [t for t in all_tables if t not in already_published]

    if not to_add:
        print("✅ Toutes les tables sont déjà publiées.")
    else:
        for table in to_add:
            add_table_to_publication(table)
        conn.commit()
        print(f"✅ {len(to_add)} table(s) ajoutée(s) à la publication.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Erreur : {e}")
    finally:
        conn.close()
