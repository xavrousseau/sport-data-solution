-- ==========================================================================================
-- Script       : 00_init_postgres.sql 
-- Objectif     : Création du schéma et des tables pour Sport Data Solution
-- Auteur       : Xavier Rousseau | Août 2025
-- ==========================================================================================

CREATE SCHEMA IF NOT EXISTS sportdata;

-- ==========================================================================================
-- 2. Table des employés (RH)
-- ==========================================================================================
CREATE TABLE IF NOT EXISTS sportdata.employes (
    id_salarie            VARCHAR PRIMARY KEY,
    nom                   TEXT,
    prenom                TEXT,
    adresse_du_domicile   TEXT,
    moyen_de_deplacement  TEXT,
    mode_normalise        VARCHAR,              -- <== attendu par le script
    distance_km           DOUBLE PRECISION,
    eligible              BOOLEAN,
    motif_exclusion       TEXT,
    salaire_brut_annuel   INT,
    deplacement_sportif   BOOLEAN DEFAULT FALSE,
    date_mise_a_jour      TIMESTAMP DEFAULT NOW()  -- <== mis à jour dans l’UPSERT
);

-- ==========================================================================================
-- 3. Table des activités sportives simulées (type Strava)
-- ==========================================================================================
CREATE TABLE IF NOT EXISTS sportdata.activites_sportives (
    uid UUID PRIMARY KEY,
    id_salarie VARCHAR,               -- aligne avec employes.id_salarie
    nom TEXT,
    prenom TEXT,
    date TIMESTAMP,
    jour DATE,
    date_debut TIMESTAMP,
    type_activite TEXT,
    distance_km DOUBLE PRECISION,     -- cohérence de type
    temps_sec INT,
    commentaire TEXT,
    profil TEXT,
    lieu TEXT
    -- Optionnel : FOREIGN KEY (id_salarie) REFERENCES sportdata.employes(id_salarie)
);

-- ==========================================================================================
-- 4. Table des bénéficiaires de primes sportives
-- ==========================================================================================
CREATE TABLE IF NOT EXISTS sportdata.beneficiaires_primes_sport (
    id_salarie VARCHAR,
    nom TEXT,
    prenom TEXT,
    salaire_brut_annuel INT,
    nb_activites INT,
    prime_eligible BOOLEAN,
    prime_montant_eur DOUBLE PRECISION,
    date_prime DATE
);

-- ==========================================================================================
-- 5. Table des bénéficiaires de journées bien-être
-- ==========================================================================================
CREATE TABLE IF NOT EXISTS sportdata.beneficiaires_journees_bien_etre (
    id_salarie VARCHAR,
    nb_activites INT,
    nb_journees_bien_etre INT
);
