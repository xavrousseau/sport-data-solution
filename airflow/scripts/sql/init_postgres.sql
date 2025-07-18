-- Création du schéma sportdata (optionnel si "public" déjà utilisé)
CREATE SCHEMA IF NOT EXISTS sportdata;

-- Table des employés (issue des données RH nettoyées)
CREATE TABLE IF NOT EXISTS sportdata.employes (
    id_salarie INT PRIMARY KEY,
    nom TEXT,
    prenom TEXT,
    adresse_du_domicile TEXT,
    moyen_de_deplacement TEXT,
    distance_km FLOAT,
    eligible BOOLEAN,
    motif_exclusion TEXT
);

-- Table des activités sportives (simulées ou récupérées via API)
CREATE TABLE IF NOT EXISTS sportdata.activites_sportives (
    id SERIAL PRIMARY KEY,
    id_salarie INT,
    date_debut TIMESTAMP,
    sport_type TEXT,
    distance_m INT,
    temps_s INT,
    commentaire TEXT
);

-- Table des bénéficiaires (croisement RH/Sport + montant prime)
CREATE TABLE IF NOT EXISTS sportdata.beneficiaires_primes_sport (
    id_salarie INT,
    nom TEXT,
    prenom TEXT,
    sport_type TEXT,
    distance_m INT,
    prime_eligible BOOLEAN,
    prime_montant_eur FLOAT
);
