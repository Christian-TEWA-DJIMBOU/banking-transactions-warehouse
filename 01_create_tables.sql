
-- ÉTAPE 1 : Suppression des tables existantes
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_client;
DROP TABLE IF EXISTS dim_agence;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS staging_clients;
DROP TABLE IF EXISTS staging_transactions;


-- ÉTAPE 2 : Tables STAGING
-- Table staging pour les clients (CSV brut)
CREATE TABLE staging_clients (
    client_id           VARCHAR(20),
    nom                 VARCHAR(100),
    prenom              VARCHAR(100),
    date_naissance      VARCHAR(20),
    genre               VARCHAR(20),
    email               VARCHAR(150),
    telephone           VARCHAR(30),
    ville               VARCHAR(100),
    segment             VARCHAR(50)
);

-- Table staging pour les transactions (CSV brut)
CREATE TABLE staging_transactions (
    transaction_id      VARCHAR(20),
    client_id           VARCHAR(20),
    nom_agence          VARCHAR(150),
    ville_agence        VARCHAR(100),
    region_agence       VARCHAR(100),
    date_transaction    VARCHAR(20),
    type_transaction    VARCHAR(50),
    montant             VARCHAR(20),
    canal               VARCHAR(30),
    statut              VARCHAR(20),
    frais               VARCHAR(20)
);



-- ÉTAPE 3 : Tables du WAREHOUSE
-- Schéma en étoile avec contraintes

-- Contient toutes les dates de la période 2024-2025
CREATE TABLE dim_date (
    date_id         SERIAL PRIMARY KEY,
    date_complete   DATE NOT NULL UNIQUE,
    jour            INTEGER NOT NULL,
    mois            INTEGER NOT NULL,
    nom_mois        VARCHAR(20) NOT NULL,
    trimestre       INTEGER NOT NULL,
    annee           INTEGER NOT NULL,
    jour_semaine    VARCHAR(20) NOT NULL,
    est_weekend     BOOLEAN NOT NULL
);

-- Clients nettoyés et dédoublonnés
CREATE TABLE dim_client (
    client_id       INTEGER PRIMARY KEY,
    nom             VARCHAR(100) NOT NULL,
    prenom          VARCHAR(100) NOT NULL,
    date_naissance  DATE,
    genre           VARCHAR(10),
    email           VARCHAR(150),
    telephone       VARCHAR(20),
    ville           VARCHAR(100),
    segment         VARCHAR(50) NOT NULL
);

-- Agences extraites des transactions
CREATE TABLE dim_agence (
    agence_id       SERIAL PRIMARY KEY,
    nom_agence      VARCHAR(150) NOT NULL,
    ville           VARCHAR(100) NOT NULL,
    region          VARCHAR(100) NOT NULL
);

-- Faits : fact_transactions
-- Transactions propres avec clés étrangères
CREATE TABLE fact_transactions (
    transaction_id      INTEGER PRIMARY KEY,
    client_id           INTEGER NOT NULL REFERENCES dim_client(client_id),
    agence_id           INTEGER NOT NULL REFERENCES dim_agence(agence_id),
    date_id             INTEGER NOT NULL REFERENCES dim_date(date_id),
    montant             DECIMAL(12,2) NOT NULL,
    type_transaction    VARCHAR(50) NOT NULL,
    canal               VARCHAR(30) NOT NULL,
    statut              VARCHAR(20) NOT NULL,
    frais               DECIMAL(8,2) DEFAULT 0.00
);


-- ÉTAPE 4 : Index pour optimiser les requêtes
-- Index sur la table de faits (accélère les JOIN)
CREATE INDEX idx_fact_client ON fact_transactions(client_id);
CREATE INDEX idx_fact_agence ON fact_transactions(agence_id);
CREATE INDEX idx_fact_date ON fact_transactions(date_id);
CREATE INDEX idx_fact_type ON fact_transactions(type_transaction);
CREATE INDEX idx_fact_canal ON fact_transactions(canal);

-- Index sur les dimensions (accélère les filtres)
CREATE INDEX idx_date_annee_mois ON dim_date(annee, mois);
CREATE INDEX idx_client_segment ON dim_client(segment);
CREATE INDEX idx_client_ville ON dim_client(ville);
CREATE INDEX idx_agence_region ON dim_agence(region);
