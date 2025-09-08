{
  // ============================================================================
  // Fichier : sportdata_connector.json
  // Objectif : Configuration Debezium complète pour capture CDC (4 tables)
  // Tables suivies : employes, activites_sportives, beneficiaires_primes_sport, beneficiaires_journees_bien_etre
  // Auteur : Xavier Rousseau | Mise à jour : Juillet 2025
  // ============================================================================
  "name": "sportdata-connector",   // Nom du connecteur Debezium (unique)

  "config": {
    // --- Classe de connecteur utilisée (PostgreSQL) ---
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

    // --- Connexion à PostgreSQL ---
    "database.hostname": "sport-postgres",         // Nom du service Docker
    "database.port": "5432",                       // Port PostgreSQL
    "database.user": "user",                       // Identifiants de connexion
    "database.password": "password",
    "database.dbname": "sportdata",                // Nom de la base
    "database.server.name": "sportdata",           // Préfixe logique → topics = sportdata.schema.table

    // --- Plugin de réplication ---
    "plugin.name": "pgoutput",                     // pgoutput = plugin logique de réplication (PG ≥ 10)

    // --- Slot logique & publication ---
    "slot.name": "sportdata_slot",                 // Slot logique déjà créé
    "publication.name": "debezium_publication",    // Publication contenant les 4 tables

    // --- Filtrage : schema + tables suivies ---
    "schema.include.list": "sportdata",            // Nom du schema ciblé
    "table.include.list": "sportdata.employes,sportdata.activites_sportives,sportdata.beneficiaires_primes_sport,sportdata.beneficiaires_journees_bien_etre",

    // --- Nom des topics Kafka générés ---
    "topic.prefix": "sportdata",                   // Chaque topic sera : sportdata.schema.table

    // --- Mode de sortie : données en clair, format JSON plat ---
    "transforms": "unwrap",                        // Applique une transformation de "dénestage"
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true",   // Supprime les tombstones Kafka inutiles

    // --- Format JSON allégé (sans schéma AVRO) ---
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",

    // --- Gestion des décimales : utile pour Spark et BI ---
    "decimal.handling.mode": "double",

    // --- Mode dev/local uniquement ---
    "database.sslmode": "disable",
    "database.allowPublicKeyRetrieval": "true"
  }
}
