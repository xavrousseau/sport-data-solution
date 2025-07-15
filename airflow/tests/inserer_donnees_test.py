"""
Script de test : insertion d'une ligne dans chaque table du schéma `sportdata`
pour déclencher la création automatique des topics Debezium dans Redpanda.
"""

import os
import psycopg2
from dotenv import load_dotenv
from datetime import date

# Charger les variables d’environnement
load_dotenv()

# Connexion PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)

cur = conn.cursor()

# Insertion dans employes
cur.execute("""
    INSERT INTO sportdata.employes (
        id_salarie, nom, prenom, adresse_du_domicile,
        moyen_de_deplacement, distance_km, eligible, motif_exclusion
    ) VALUES (
        90001, 'Test', 'Employé', '123 rue des tests, 34000 Montpellier',
        'marche', 2.0, TRUE, NULL
    );
""")

# Insertion dans activities_sportives
cur.execute("""
    INSERT INTO sportdata.activities_sportives (
        id_salarie, date_debut, sport_type, distance_m, temps_s, commentaire
    ) VALUES (
        90001, %s, 'course à pied', 3200, 1100, 'test automatique'
    );
""", (date.today(),))

# Insertion dans beneficiaires_primes_sport
cur.execute("""
    INSERT INTO sportdata.beneficiaires_primes_sport (
        id_salarie, nom, prenom, adresse_du_domicile,
        moyen_de_deplacement, distance_km, eligible, motif_exclusion,
        activite_origine, activite_clean, prime_eligible, prime_montant_eur
    ) VALUES (
        90001, 'Test', 'Employé', '123 rue des tests, 34000 Montpellier',
        'marche', 2.0, TRUE, NULL,
        'course à pied', 'course', TRUE, 200
    );
""")

conn.commit()
cur.close()
conn.close()

print("✅ Données de test insérées dans les 3 tables du schéma sportdata.")
