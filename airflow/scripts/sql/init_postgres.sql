-- ==========================================================================================
-- Script       : init_postgres.sql
-- Objectif     : Création du schéma et des 4 tables utilisées dans le projet Sport Data Solution
-- Auteur       : Xavier Rousseau | Juillet 2025
-- ==========================================================================================

-- ==========================================================================================
-- 1. Création du schéma principal
-- ==========================================================================================
CREATE SCHEMA IF NOT EXISTS sportdata;

-- ==========================================================================================
-- 2. Table des employés (RH)
-- Objectif : Suivre les salariés éligibles à un déplacement sportif domicile-travail
-- ==========================================================================================
CREATE TABLE IF NOT EXISTS sportdata.employes (
    id_salarie INT PRIMARY KEY,
    nom TEXT,
    prenom TEXT,
    adresse_du_domicile TEXT,
    moyen_de_deplacement TEXT,
    distance_km FLOAT,
    eligible BOOLEAN,
    motif_exclusion TEXT,
    salaire_brut_annuel INT,
    deplacement_sportif BOOLEAN DEFAULT FALSE
);

-- ==========================================================================================
-- 3. Table des activités sportives simulées (type Strava)
-- Objectif : Stocker les activités simulées générées par le script Python
-- ==========================================================================================
CREATE TABLE IF NOT EXISTS sportdata.activites_sportives (
    uid UUID PRIMARY KEY,
    id_salarie INT,
    nom TEXT,
    prenom TEXT,
    date TIMESTAMP,               -- Date et heure complète ISO (UTC)
    jour DATE,                    -- Date (YYYY-MM-DD)
    date_debut TIMESTAMP,         -- Date complète du début de l’activité
    type_activite TEXT,
    distance_km FLOAT,            -- Exprimé en kilomètres
    temps_sec INT,                -- Durée en secondes
    commentaire TEXT,
    profil TEXT                   -- Nouveau : profil sportif (Sédentaire, Occasionnel, etc.)
);

-- ==========================================================================================
-- 4. Table des bénéficiaires de primes sportives
-- Objectif : Stocker les salariés éligibles à une prime (5% du salaire)
-- ==========================================================================================
CREATE TABLE IF NOT EXISTS sportdata.beneficiaires_primes_sport (
    id_salarie INT,
    nom TEXT,
    prenom TEXT,
    salaire_brut_annuel INT,
    nb_activites INT,
    prime_eligible BOOLEAN,
    prime_montant_eur FLOAT,
    date_prime DATE
);

-- ==========================================================================================
-- 5. Table des bénéficiaires de journées bien-être
-- Objectif : Identifier les salariés ayant réalisé ≥15 activités
-- ==========================================================================================
CREATE TABLE IF NOT EXISTS sportdata.beneficiaires_journees_bien_etre (
    id_salarie INT,
    nb_activites INT,
    nb_journees_bien_etre INT
);
