-- ==========================================================================================
-- Script       : 99_test_cdc.sql
-- Objectif : Insérer des données de test dans les tables surveillées par Debezium
--               et forcer la création automatique des topics Kafka
-- Auteur     : Xavier Rousseau | Août 2025

-- ==========================================================================================

-- 1. Sélection du schéma cible
SET search_path TO sportdata;

-- ==========================================================================================
-- 🟢 INSERTIONS DE TEST
-- ==========================================================================================

-- 🧍 Table employes
INSERT INTO sportdata.employes (
    id_salarie, nom, prenom, adresse_du_domicile,
    moyen_de_deplacement, mode_normalise, distance_km,
    eligible, motif_exclusion, salaire_brut_annuel,
    deplacement_sportif
) VALUES (
    '999', 'Kafka', 'Jean', '123 avenue Debezium',
    'vélo', 'vélo', 14.5,
    true, '', 32000,
    true
);

-- 🏃‍♂️ Table activites_sportives
INSERT INTO sportdata.activites_sportives (
    uid, id_salarie, nom, prenom, date, jour, date_debut,
    type_activite, distance_km, temps_sec, commentaire, profil, lieu
) VALUES (
    '00000000-0000-0000-0000-00000000cafe', '999', 'Kafka', 'Jean',
    '2025-07-28 09:00:00', '2025-07-28', '2025-07-28 09:00:00',
    'course', 5.0, 1600, 'Test CDC Kafka OK', 'Régulier', 'Test'
);

-- 💰 Table beneficiaires_primes_sport
INSERT INTO sportdata.beneficiaires_primes_sport (
    id_salarie, nom, prenom, salaire_brut_annuel, nb_activites,
    prime_eligible, prime_montant_eur, date_prime
) VALUES (
    '999', 'Kafka', 'Jean', 32000, 18,
    true, 1600.0, '2025-07-28'
);

-- 🌿 Table beneficiaires_journees_bien_etre
INSERT INTO sportdata.beneficiaires_journees_bien_etre (
    id_salarie, nb_activites, nb_journees_bien_etre
) VALUES (
    '999', 25, 2
);
