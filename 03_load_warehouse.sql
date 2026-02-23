-- ÉTAPE 1 : Vider les tables du warehouse
TRUNCATE TABLE fact_transactions;
TRUNCATE TABLE dim_client CASCADE;
TRUNCATE TABLE dim_agence CASCADE;
TRUNCATE TABLE dim_date CASCADE;


-- ÉTAPE 2 : Charger dim_date
INSERT INTO dim_date (
    date_complete,
    jour,
    mois,
    nom_mois,
    trimestre,
    annee,
    jour_semaine,
    est_weekend
)
SELECT
    -- La date elle-même
    d::DATE AS date_complete,

    EXTRACT(DAY FROM d) AS jour,

    -- Extraire le numéro du mois (1 à 12)
    EXTRACT(MONTH FROM d) AS mois,

    -- Nom du mois en français
    CASE EXTRACT(MONTH FROM d)
        WHEN 1 THEN 'Janvier'
        WHEN 2 THEN 'Fevrier'
        WHEN 3 THEN 'Mars'
        WHEN 4 THEN 'Avril'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Juin'
        WHEN 7 THEN 'Juillet'
        WHEN 8 THEN 'Aout'
        WHEN 9 THEN 'Septembre'
        WHEN 10 THEN 'Octobre'
        WHEN 11 THEN 'Novembre'
        WHEN 12 THEN 'Decembre'
    END AS nom_mois,

    EXTRACT(QUARTER FROM d) AS trimestre,

    EXTRACT(YEAR FROM d) AS annee,

    CASE EXTRACT(DOW FROM d)
        WHEN 0 THEN 'Dimanche'
        WHEN 1 THEN 'Lundi'
        WHEN 2 THEN 'Mardi'
        WHEN 3 THEN 'Mercredi'
        WHEN 4 THEN 'Jeudi'
        WHEN 5 THEN 'Vendredi'
        WHEN 6 THEN 'Samedi'
    END AS jour_semaine,

    CASE
        WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS est_weekend

FROM generate_series('2024-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) AS d;


-- ÉTAPE 3 : Charger dim_client
INSERT INTO dim_client (
    client_id,
    nom,
    prenom,
    date_naissance,
    genre,
    email,
    telephone,
    ville,
    segment
)
SELECT
    client_id,
    nom,
    prenom,
    date_naissance,
    genre,
    email,
    telephone,
    ville,
    segment
FROM staging_clients_clean;


-- ÉTAPE 4 : Charger dim_agence
INSERT INTO dim_agence (
    nom_agence,
    ville,
    region
)
SELECT DISTINCT
    nom_agence,
    ville_agence,
    region_agence
FROM staging_transactions_clean
ORDER BY nom_agence;


-- ÉTAPE 5 : Charger fact_transactions
    transaction_id,
    client_id,
    agence_id,
    date_id,
    montant,
    type_transaction,
    canal,
    statut,
    frais
)
SELECT
    t.transaction_id,
    t.client_id,

    a.agence_id,

    d.date_id,

    t.montant,
    t.type_transaction,
    t.canal,
    t.statut,
    t.frais

FROM staging_transactions_clean t

-- JOIN pour trouver l'agence_id correspondant
INNER JOIN dim_agence a
    ON t.nom_agence = a.nom_agence
    AND t.ville_agence = a.ville

INNER JOIN dim_date d
    ON t.date_transaction = d.date_complete;


-- ÉTAPE 6 : Vérification du chargement
SELECT 'dim_date' AS table_name, COUNT(*) AS nb_lignes FROM dim_date
UNION ALL
SELECT 'dim_client', COUNT(*) FROM dim_client
UNION ALL
SELECT 'dim_agence', COUNT(*) FROM dim_agence
UNION ALL
SELECT 'fact_transactions', COUNT(*) FROM fact_transactions;

-- Vérifier un échantillon de fact_transactions avec les dimensions
SELECT
    f.transaction_id,
    c.nom || ' ' || c.prenom AS client,
    c.segment,
    a.nom_agence,
    a.region,
    d.date_complete,
    d.nom_mois,
    d.trimestre,
    f.montant,
    f.type_transaction,
    f.canal,
    f.statut,
    f.frais
FROM fact_transactions f
JOIN dim_client c ON f.client_id = c.client_id
JOIN dim_agence a ON f.agence_id = a.agence_id
JOIN dim_date d ON f.date_id = d.date_id
LIMIT 10;
