-- Description : Requêtes analytiques pour l'aide à la décision



-- 1. Montant total des transactions par segment client
SELECT
    c.segment,
    COUNT(f.transaction_id) AS nb_transactions,
    ROUND(SUM(f.montant), 2) AS montant_total,
    ROUND(AVG(f.montant), 2) AS montant_moyen,
    ROUND(SUM(f.frais), 2) AS frais_total
FROM fact_transactions f
JOIN dim_client c ON f.client_id = c.client_id
GROUP BY c.segment
ORDER BY montant_total DESC;


-- 2. Performance des agences par région
SELECT
    a.region,
    a.nom_agence,
    COUNT(f.transaction_id) AS nb_transactions,
    ROUND(SUM(f.montant), 2) AS montant_total,
    ROUND(AVG(f.montant), 2) AS montant_moyen,
    COUNT(DISTINCT f.client_id) AS nb_clients_uniques
FROM fact_transactions f
JOIN dim_agence a ON f.agence_id = a.agence_id
GROUP BY a.region, a.nom_agence
ORDER BY a.region, montant_total DESC;


-- 3. Évolution mensuelle des transactions
SELECT
    d.annee,
    d.mois,
    d.nom_mois,
    COUNT(f.transaction_id) AS nb_transactions,
    ROUND(SUM(f.montant), 2) AS montant_total,
    ROUND(AVG(f.montant), 2) AS montant_moyen
FROM fact_transactions f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.annee, d.mois, d.nom_mois
ORDER BY d.annee, d.mois;


-- 4. Répartition par type de transaction et canal
SELECT
    f.type_transaction,
    f.canal,
    COUNT(f.transaction_id) AS nb_transactions,
    ROUND(SUM(f.montant), 2) AS montant_total,
    ROUND(100.0 * COUNT(f.transaction_id) / SUM(COUNT(f.transaction_id)) OVER (), 2) AS pourcentage
FROM fact_transactions f
GROUP BY f.type_transaction, f.canal
ORDER BY nb_transactions DESC;


-- 5. Top 10 clients par montant total
SELECT
    c.client_id,
    c.nom || ' ' || c.prenom AS client,
    c.segment,
    c.ville,
    COUNT(f.transaction_id) AS nb_transactions,
    ROUND(SUM(f.montant), 2) AS montant_total,
    ROUND(SUM(f.frais), 2) AS frais_total
FROM fact_transactions f
JOIN dim_client c ON f.client_id = c.client_id
GROUP BY c.client_id, c.nom, c.prenom, c.segment, c.ville
ORDER BY montant_total DESC
LIMIT 10;


-- 6. Comparaison semaine vs weekend
SELECT
    CASE WHEN d.est_weekend THEN 'Weekend' ELSE 'Semaine' END AS periode,
    COUNT(f.transaction_id) AS nb_transactions,
    ROUND(SUM(f.montant), 2) AS montant_total,
    ROUND(AVG(f.montant), 2) AS montant_moyen
FROM fact_transactions f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.est_weekend
ORDER BY d.est_weekend;


-- 7. Évolution trimestrielle par région
SELECT
    d.annee,
    d.trimestre,
    a.region,
    COUNT(f.transaction_id) AS nb_transactions,
    ROUND(SUM(f.montant), 2) AS montant_total
FROM fact_transactions f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_agence a ON f.agence_id = a.agence_id
GROUP BY d.annee, d.trimestre, a.region
ORDER BY d.annee, d.trimestre, montant_total DESC;


-- 8. Taux d'échec par canal
SELECT
    f.canal,
    COUNT(f.transaction_id) AS total,
    SUM(CASE WHEN f.statut = 'Echouee' THEN 1 ELSE 0 END) AS nb_echecs,
    ROUND(100.0 * SUM(CASE WHEN f.statut = 'Echouee' THEN 1 ELSE 0 END) / COUNT(f.transaction_id), 2) AS taux_echec_pct
FROM fact_transactions f
GROUP BY f.canal
ORDER BY taux_echec_pct DESC;


-- 9. Analyse par tranche d'âge des clients
SELECT
    CASE
        WHEN AGE(CURRENT_DATE, c.date_naissance) < INTERVAL '25 years' THEN '18-24'
        WHEN AGE(CURRENT_DATE, c.date_naissance) < INTERVAL '35 years' THEN '25-34'
        WHEN AGE(CURRENT_DATE, c.date_naissance) < INTERVAL '50 years' THEN '35-49'
        WHEN AGE(CURRENT_DATE, c.date_naissance) < INTERVAL '65 years' THEN '50-64'
        ELSE '65+'
    END AS tranche_age,
    COUNT(f.transaction_id) AS nb_transactions,
    ROUND(AVG(f.montant), 2) AS montant_moyen,
    ROUND(SUM(f.montant), 2) AS montant_total
FROM fact_transactions f
JOIN dim_client c ON f.client_id = c.client_id
WHERE c.date_naissance IS NOT NULL
GROUP BY tranche_age
ORDER BY tranche_age;


-- 10. Classement des agences avec RANK (Window Function)
    a.nom_agence,
    a.region,
    ROUND(SUM(f.montant), 2) AS montant_total,
    COUNT(f.transaction_id) AS nb_transactions,
    RANK() OVER (ORDER BY SUM(f.montant) DESC) AS rang_montant,
    RANK() OVER (ORDER BY COUNT(f.transaction_id) DESC) AS rang_volume
FROM fact_transactions f
JOIN dim_agence a ON f.agence_id = a.agence_id
GROUP BY a.nom_agence, a.region
ORDER BY rang_montant;
