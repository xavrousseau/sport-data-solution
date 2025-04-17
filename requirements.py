# === Traitement des données ===
pandas==2.2.2               # Librairie pour manipuler des DataFrames, compatible avec Python 3.11
numpy==1.26.4               # Numpy requis par pandas, utilisé pour les opérations numériques
openpyxl==3.1.2             # Pour lire/écrire les fichiers Excel .xlsx

# === Accès web, chargement distant ===
requests==2.31.0            # Pour faire des requêtes HTTP (ex. télécharger les fichiers S3)

# === Gestion de variables d'environnement ===
python-dotenv==1.0.1        # Pour charger automatiquement les variables définies dans .env

# === Bases de données ===
sqlalchemy==2.0.29          # ORM pour interagir avec DuckDB ou PostgreSQL
psycopg2-binary==2.9.9      # Driver PostgreSQL
duckdb==0.10.1              # Base de données embarquée, idéale pour les analytics locales

# === API externes ===
slack_sdk==3.26.2           # SDK officiel pour envoyer des messages via Slack API
geopy==2.4.1                # Pour calculer des distances ou géocoder les adresses

# === Sécurité / chiffrement ===
cryptography==42.0.4        # Nécessaire pour générer la Fernet key pour Airflow

# === Qualité des données (optionnel mais recommandé) ===
great_expectations==0.18.10 # Framework de tests automatisés sur des datasets pandas ou SQL
