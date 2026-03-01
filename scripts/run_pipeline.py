"""
  Banking Transactions Data Warehouse
  Script : run_pipeline.py
  Description : Exécute le pipeline ETL complet
"""

import os
import sys
import time
import psycopg2

sys.path.insert(0, os.path.dirname(__file__))
from generate_source_data import generer_clients, generer_transactions, sauvegarder_csv
from load_to_staging import connecter_db, charger_csv, compter_lignes, afficher_apercu

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'banking_dw'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
SQL_DIR = os.path.join(os.path.dirname(__file__), '..', 'sql')


def attendre_postgres(max_tentatives=30):
    """Attend que PostgreSQL soit prêt avant de continuer."""
    for i in range(max_tentatives):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("PostgreSQL est prêt.")
            return True
        except psycopg2.OperationalError:
            print(f"  Attente de PostgreSQL... ({i+1}/{max_tentatives})")
            time.sleep(2)
    print("PostgreSQL n'est pas disponible.")
    return False


def executer_sql(conn, chemin):
    """Exécute un fichier SQL."""
    with open(chemin, 'r', encoding='utf-8') as f:
        contenu = f.read()
    cursor = conn.cursor()
    cursor.execute(contenu)
    conn.commit()
    cursor.close()
    print(f"  OK : {os.path.basename(chemin)}")


def main():
    print("=" * 55)
    print("  BANKING TRANSACTIONS DATA WAREHOUSE")
    print("  Pipeline ETL complet")
    print("=" * 55)

    # 1. Attendre PostgreSQL
    print("\n[1/6] Connexion a PostgreSQL...")
    if not attendre_postgres():
        sys.exit(1)
    conn = psycopg2.connect(**DB_CONFIG)

    # 2. Generer les donnees sources
    print("\n[2/6] Generation des donnees sources...")
    os.makedirs(DATA_DIR, exist_ok=True)

    clients = generer_clients()
    colonnes_clients = [
        'client_id', 'nom', 'prenom', 'date_naissance',
        'genre', 'email', 'telephone', 'ville', 'segment'
    ]
    sauvegarder_csv(clients, os.path.join(DATA_DIR, 'source_clients.csv'), colonnes_clients)

    transactions = generer_transactions(clients)
    colonnes_transactions = [
        'transaction_id', 'client_id', 'nom_agence', 'ville_agence',
        'region_agence', 'date_transaction', 'type_transaction',
        'montant', 'canal', 'statut', 'frais'
    ]
    sauvegarder_csv(transactions, os.path.join(DATA_DIR, 'source_transactions.csv'), colonnes_transactions)

    # 3. Creer les tables
    print("\n[3/6] Creation des tables...")
    executer_sql(conn, os.path.join(SQL_DIR, '01_create_tables.sql'))

    # 4. Charger dans le staging
    print("\n[4/6] Chargement dans le staging...")
    from psycopg2 import sql as psql
    cursor = conn.cursor()
    cursor.execute(psql.SQL("TRUNCATE TABLE {}").format(psql.Identifier('staging_clients')))
    cursor.execute(psql.SQL("TRUNCATE TABLE {}").format(psql.Identifier('staging_transactions')))
    conn.commit()
    cursor.close()

    charger_csv(conn, os.path.join(DATA_DIR, 'source_clients.csv'), 'staging_clients', colonnes_clients)
    charger_csv(conn, os.path.join(DATA_DIR, 'source_transactions.csv'), 'staging_transactions', colonnes_transactions)

    # 5. Nettoyer et charger dans le warehouse
    print("\n[5/6] Nettoyage et chargement dans le warehouse...")
    executer_sql(conn, os.path.join(SQL_DIR, '02_clean_data.sql'))
    executer_sql(conn, os.path.join(SQL_DIR, '03_load_warehouse.sql'))

    # 6. Verification finale
    print("\n[6/6] Verification finale...")
    for table in ['dim_date', 'dim_client', 'dim_agence', 'fact_transactions']:
        nb = compter_lignes(conn, table)
        print(f"  {table:25s} : {nb} lignes")

    conn.close()

    print("\n" + "=" * 55)
    print("  Pipeline termine avec succes !")
    print("=" * 55)


if __name__ == '__main__':
    main()
