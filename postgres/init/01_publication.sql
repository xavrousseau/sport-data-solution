-- ==========================================================================================
-- Script       : 01_publication.sql 
-- Objectif     : Création de la publication pour Debezium (CDC)
-- Auteur       : Xavier Rousseau | Août 2025
-- ==========================================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_publication') THEN
        CREATE PUBLICATION debezium_publication FOR TABLE
            sportdata.employes,
            sportdata.activites_sportives,
            sportdata.beneficiaires_primes_sport,
            sportdata.beneficiaires_journees_bien_etre;
    END IF;
END$$;
