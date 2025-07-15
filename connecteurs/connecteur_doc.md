{
  // ============================================================================
  // Fichier : sportdata-activites-connector.json
  // Objectif : Configuration Debezium pour capture CDC des activités sportives
  // Service cible : PostgreSQL (table public.activites_sportives)
  // Auteur : Xavier Rousseau | Juin 2025
  // ============================================================================
  "name": "sportdata-activites-connector",   // Nom du connecteur Debezium
  "config": {
    // --- Classe du connecteur PostgreSQL ---
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

    // --- Connexion à la base ---
    "database.hostname": "sport-postgres",   // Nom du service Docker/Postgres
    "database.port": "5432",                 // Port PostgreSQL
    "database.user": "user",                 // Utilisateur (à sécuriser en prod)
    "database.password": "password",         // Mot de passe (à sécuriser en prod)
    "database.dbname": "sportdata",          // Nom de la base cible
    "database.server.name": "dbserver1",     // Préfixe logique des topics Kafka (requis)

    // --- Plugin de réplication ---
    "plugin.name": "pgoutput",               // Plugin par défaut pour PG >= 10

    // --- Tables surveillées (CDC) ---
    "table.include.list": "public.activites_sportives", // Seule la table activités

    // --- Replication slot & publication ---
    "slot.name": "debezium_slot",            // Nom du slot logique PG
    "publication.name": "debezium_pub",      // Nom de la publication PG

    // --- Sécurité (dev seulement) ---
    "database.allowPublicKeyRetrieval": "true",
    "database.sslmode": "disable",

    // --- Transformation : dé-nestage des événements CDC (un seul niveau) ---
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",

    // --- Conversion des clés/valeurs (JSON allégé, pas de schemas AVRO) ---
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}



# Invoke-WebRequest -Uri "http://localhost:8083/connectors" `
#  -Method Post `
# -Headers @{ "Content-Type" = "application/json" } `
#  -InFile "F:\1.Boulot\03_Github\sport-data-solution\connecteurs\connecteur.json"
