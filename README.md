# Banking Transactions Data Warehouse

Data Warehouse pour l'analyse des transactions bancaires. Pipeline ETL complet : génération de données, chargement en staging, nettoyage, modélisation en schéma en étoile et requêtes analytiques.

## Contexte

Une banque dispose de deux systèmes qui ne communiquent pas : un CRM (gestion clients) et un système transactionnel. Les données sont exportées en CSV avec des erreurs (doublons, formats incohérents, valeurs manquantes). Ce projet construit un Data Warehouse qui centralise, nettoie et organise ces données pour permettre aux analystes de répondre à des questions business.

## Architecture

```
┌──────────────────┐     ┌──────────────────┐
│  source_clients  │     │source_transactions│
│     (.csv)       │     │     (.csv)        │
└────────┬─────────┘     └────────┬──────────┘
         │                        │
         ▼                        ▼
┌──────────────────────────────────────────┐
│           STAGING (PostgreSQL)            │
│  staging_clients  staging_transactions   │
│  → Données brutes, tout en VARCHAR       │
└────────────────────┬─────────────────────┘
                     │ Nettoyage SQL
                     ▼
┌──────────────────────────────────────────┐
│         WAREHOUSE (Schéma en étoile)     │
│                                          │
│   dim_client ──┐                         │
│                ├── fact_transactions      │
│   dim_agence ──┤                         │
│                │                         │
│   dim_date ────┘                         │
└────────────────────┬─────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────┐
│          REQUÊTES ANALYTIQUES            │
│  Performance agences, segments clients,  │
│  tendances mensuelles, taux d'échec...   │
└──────────────────────────────────────────┘
```

## Technologies

| Technologie | Usage |
|---|---|
| Python 3.10 | Génération de données, chargement |
| PostgreSQL 13 | Base de données du warehouse |
| Faker | Génération de données réalistes |
| psycopg2 | Connexion Python ↔ PostgreSQL |
| Docker | Conteneurisation de l'environnement |
| SQL | Modélisation, nettoyage, analyses |

## Structure du projet

```
banking-transactions-warehouse/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── scripts/
│   ├── generate_source_data.py    # Génération des CSV sources
│   ├── load_to_staging.py         # Chargement CSV → staging
│   └── run_pipeline.py            # Exécution du pipeline complet
└── sql/
    ├── 01_create_tables.sql       # Tables staging + warehouse
    ├── 02_clean_data.sql          # Nettoyage des données
    ├── 03_load_warehouse.sql      # Chargement dans le schéma en étoile
    └── 04_analytics.sql           # 10 requêtes analytiques
```

## Lancement rapide

```bash
docker-compose up --build
```

Cette commande lance PostgreSQL et exécute automatiquement le pipeline complet :
1. Génération de 500 clients et 5000 transactions avec des erreurs volontaires
2. Création des tables staging et warehouse
3. Chargement des CSV dans le staging
4. Nettoyage (doublons, genres incohérents, emails invalides, dates corrompues)
5. Chargement dans le schéma en étoile
6. Vérification du résultat

## Étapes réalisées

### 1. Génération des données sources

Script Python utilisant Faker pour générer deux fichiers CSV simulant des exports de systèmes bancaires. Des erreurs sont injectées volontairement pour simuler des données réelles :
- Genres dans 10 formats différents (M, F, Homme, Femme, Male, Female, H...)
- 10% d'emails vides, 5% invalides
- 3% de doublons clients
- 3% de transactions orphelines (client inexistant)
- 2% de dates invalides, 2% de montants négatifs

### 2. Modélisation en schéma en étoile

Tables staging sans contraintes (tout en VARCHAR) pour accepter les données brutes, puis tables warehouse avec types stricts, clés étrangères et index :
- **dim_client** : informations clients nettoyées
- **dim_agence** : agences extraites des transactions
- **dim_date** : 731 jours générés automatiquement (2024-2025)
- **fact_transactions** : transactions avec clés étrangères vers les 3 dimensions

### 3. Nettoyage des données

Requêtes SQL de diagnostic puis correction :
- Standardisation des genres vers Homme/Femme
- Validation des emails avec pattern matching
- Suppression des doublons avec DISTINCT ON
- Élimination des transactions orphelines via INNER JOIN
- Conversion des montants négatifs avec ABS()

### 4. Requêtes analytiques

10 requêtes pour l'aide à la décision :

| # | Analyse | Concepts SQL |
|---|---|---|
| 1 | Montant total par segment client | GROUP BY, SUM, AVG |
| 2 | Performance des agences par région | COUNT(DISTINCT), multi-GROUP BY |
| 3 | Évolution mensuelle | JOIN dim_date, ORDER BY chronologique |
| 4 | Répartition type × canal | Window Function OVER() |
| 5 | Top 10 clients | LIMIT, concatenation \|\| |
| 6 | Semaine vs Weekend | CASE WHEN sur booléen |
| 7 | Évolution trimestrielle par région | Triple JOIN |
| 8 | Taux d'échec par canal | SUM(CASE WHEN) conditionnel |
| 9 | Analyse par tranche d'âge | AGE(), INTERVAL |
| 10 | Classement des agences | RANK() Window Function |

## Problèmes rencontrés et solutions

**Données sales multi-format** : Les genres étaient dans 10 formats différents. Solution : CASE WHEN avec liste exhaustive de correspondances.

**Transactions orphelines** : 3% des transactions référençaient des clients inexistants. Solution : INNER JOIN avec la table clients nettoyée pour les éliminer automatiquement.

**Dates invalides** : Certaines dates comme '2025-13-45' empêchaient le CAST en DATE. Solution : Filtrage par regex PostgreSQL avant la conversion.

**Ordre de chargement** : Les clés étrangères imposent de charger les dimensions avant la table de faits. Solution : Pipeline séquentiel strict (dim_date → dim_client → dim_agence → fact_transactions).

## Auteur

**Christian TEWA DJIMBOU** — Étudiant en Data Engineering à l'EFREI Paris
