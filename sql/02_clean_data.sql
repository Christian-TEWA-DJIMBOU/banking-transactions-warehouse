-- ÉTAPE 1 : Vérifier l'état des données brutes

-- 1.1 Compter les lignes totales
SELECT 'staging_clients' AS table_name, COUNT(*) AS nb_lignes FROM staging_clients
UNION ALL
SELECT 'staging_transactions', COUNT(*) FROM staging_transactions;

-- 1.2 Voir les différents formats de genre (on sait qu'ils sont incohérents)
SELECT genre, COUNT(*) AS nb_occurences
FROM staging_clients
GROUP BY genre
ORDER BY nb_occurences DESC;

-- 1.3 Compter les emails vides ou invalides
SELECT
    COUNT(*) AS total_clients,
    SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) AS emails_vides,
    SUM(CASE WHEN email LIKE '%@' AND email NOT LIKE '%@%.%' THEN 1 ELSE 0 END) AS emails_invalides
FROM staging_clients;

-- 1.4 Compter les dates de transaction invalides
SELECT
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN date_transaction = '2025-13-45' THEN 1 ELSE 0 END) AS dates_invalides,
    SUM(CASE WHEN CAST(montant AS DECIMAL) < 0 THEN 1 ELSE 0 END) AS montants_negatifs
FROM staging_transactions;

-- 1.5 Compter les transactions orphelines (client inexistant)
SELECT COUNT(*) AS transactions_orphelines
FROM staging_transactions t
LEFT JOIN staging_clients c ON t.client_id = c.client_id
WHERE c.client_id IS NULL;


-- ÉTAPE 2 : Créer une table de clients nettoyés
DROP TABLE IF EXISTS staging_clients_clean;

CREATE TABLE staging_clients_clean AS
SELECT DISTINCT ON (nom, prenom, date_naissance)
    -- Garder le plus petit client_id pour les doublons
    CAST(client_id AS INTEGER) AS client_id,

    -- Nom et prénom : première lettre en majuscule, reste en minuscule
    INITCAP(TRIM(nom)) AS nom,
    INITCAP(TRIM(prenom)) AS prenom,

    -- Date de naissance : convertir en DATE, NULL si invalide
    CASE
        WHEN date_naissance IS NOT NULL
             AND date_naissance ~ '^\d{4}-\d{2}-\d{2}$'
        THEN CAST(date_naissance AS DATE)
        ELSE NULL
    END AS date_naissance,

    -- Genre : standardiser tous les formats vers 'Homme' ou 'Femme'
    CASE
        WHEN genre IN ('Homme', 'H', 'M', 'Masculin', 'Male') THEN 'Homme'
        WHEN genre IN ('Femme', 'F', 'Feminin', 'Female') THEN 'Femme'
        ELSE NULL
    END AS genre,

    -- Email : garder seulement les emails valides (contenant @ et .)
    CASE
        WHEN email IS NOT NULL
             AND email LIKE '%@%.%'
        THEN LOWER(TRIM(email))
        ELSE NULL
    END AS email,

    -- Téléphone : garder tel quel, juste un trim
    TRIM(telephone) AS telephone,

    -- Ville : première lettre en majuscule
    INITCAP(TRIM(ville)) AS ville,

    -- Segment : garder tel quel
    TRIM(segment) AS segment

FROM staging_clients
WHERE client_id IS NOT NULL
ORDER BY nom, prenom, date_naissance, CAST(client_id AS INTEGER);


-- ÉTAPE 3 : Créer une table de transactions nettoyées
DROP TABLE IF EXISTS staging_transactions_clean;

CREATE TABLE staging_transactions_clean AS
SELECT
    CAST(t.transaction_id AS INTEGER) AS transaction_id,
    CAST(t.client_id AS INTEGER) AS client_id,

    -- Agence : nettoyer les espaces
    TRIM(t.nom_agence) AS nom_agence,
    TRIM(t.ville_agence) AS ville_agence,
    TRIM(t.region_agence) AS region_agence,

    -- Date : convertir en DATE
    CAST(t.date_transaction AS DATE) AS date_transaction,

    -- Type et canal : nettoyer les espaces
    TRIM(t.type_transaction) AS type_transaction,

    -- Montant : convertir en DECIMAL, prendre la valeur absolue si négatif
    ABS(CAST(t.montant AS DECIMAL(12,2))) AS montant,

    TRIM(t.canal) AS canal,
    TRIM(t.statut) AS statut,

    -- Frais : convertir en DECIMAL, 0 si vide
    CASE
        WHEN t.frais IS NOT NULL AND t.frais != ''
        THEN CAST(t.frais AS DECIMAL(8,2))
        ELSE 0.00
    END AS frais

FROM staging_transactions t

-- Joindre avec les clients nettoyés pour éliminer les orphelins
INNER JOIN staging_clients_clean c
    ON CAST(t.client_id AS INTEGER) = c.client_id

-- Exclure les dates invalides
WHERE t.date_transaction ~ '^\d{4}-\d{2}-\d{2}$'
  AND t.date_transaction != '2025-13-45'

-- Exclure les montants NULL
  AND t.montant IS NOT NULL;


-- ÉTAPE 4 : Vérification après nettoyage
-- 4.1 Compter les lignes avant/après
SELECT 'Clients avant nettoyage' AS etape, COUNT(*) AS nb FROM staging_clients
UNION ALL
SELECT 'Clients après nettoyage', COUNT(*) FROM staging_clients_clean
UNION ALL
SELECT 'Transactions avant nettoyage', COUNT(*) FROM staging_transactions
UNION ALL
SELECT 'Transactions après nettoyage', COUNT(*) FROM staging_transactions_clean;

-- 4.2 Vérifier que les genres sont maintenant propres
SELECT genre, COUNT(*) AS nb_occurences
FROM staging_clients_clean
GROUP BY genre
ORDER BY nb_occurences DESC;

-- 4.3 Vérifier qu'il n'y a plus de montants négatifs
SELECT
    MIN(montant) AS montant_min,
    MAX(montant) AS montant_max,
    ROUND(AVG(montant), 2) AS montant_moyen
FROM staging_transactions_clean;

-- 4.4 Vérifier qu'il n'y a plus de transactions orphelines
SELECT COUNT(*) AS orphelines_restantes
FROM staging_transactions_clean t
LEFT JOIN staging_clients_clean c ON t.client_id = c.client_id
WHERE c.client_id IS NULL;
