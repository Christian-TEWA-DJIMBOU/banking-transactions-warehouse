import csv
import psycopg2
from psycopg2 import sql

# Configuration de la connexion PostgreSQL
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'banking_dw',
    'user': 'postgres',
    'password': 'postgres'
}

# Chemins vers les fichiers CSV sources
CSV_CLIENTS = 'data/source_clients.csv'
CSV_TRANSACTIONS = 'data/source_transactions.csv'


# Fonction : Connexion à PostgreSQL
def connecter_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connexion à PostgreSQL réussie.")
        return conn
    except psycopg2.Error as e:
        print(f"Erreur de connexion : {e}")
        raise


# Fonction : Exécuter un fichier SQL
def executer_fichier_sql(conn, chemin_fichier):
    with open(chemin_fichier, 'r', encoding='utf-8') as f:
        contenu_sql = f.read()

    cursor = conn.cursor()
    cursor.execute(contenu_sql)
    conn.commit()
    cursor.close()
    print(f"Fichier SQL exécuté : {chemin_fichier}")


# Fonction : Vider une table staging
def vider_table(conn, nom_table):
    cursor = conn.cursor()
    cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(
        sql.Identifier(nom_table)
    ))
    conn.commit()
    cursor.close()
    print(f"Table {nom_table} vidée.")


# Fonction : Charger un CSV dans une table
def charger_csv(conn, chemin_csv, nom_table, colonnes):

    cursor = conn.cursor()

    # Compteurs pour le suivi
    lignes_inserees = 0
    lignes_erreur = 0

    # Construction de la requête INSERT
    placeholders = ', '.join(['%s'] * len(colonnes))
    noms_colonnes = ', '.join(colonnes)
    requete = f"INSERT INTO {nom_table} ({noms_colonnes}) VALUES ({placeholders})"

    # Lecture du CSV et insertion ligne par ligne
    with open(chemin_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for ligne in reader:
            try:
                valeurs = []
                for col in colonnes:
                    valeur = ligne[col]
                    if valeur == '':
                        valeurs.append(None)
                    else:
                        valeurs.append(valeur)

                cursor.execute(requete, valeurs)
                lignes_inserees += 1

            except Exception as e:
                lignes_erreur += 1
                print(f"  Erreur ligne {lignes_inserees + lignes_erreur} : {e}")

    conn.commit()
    cursor.close()

    print(f"  {lignes_inserees} lignes insérées dans {nom_table}")
    if lignes_erreur > 0:
        print(f"  {lignes_erreur} lignes en erreur")

    return lignes_inserees


# Fonction : Afficher un aperçu des données
def afficher_apercu(conn, nom_table, limite=5):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {nom_table} LIMIT {limite}")
    lignes = cursor.fetchall()

    # Récupérer les noms de colonnes
    noms_colonnes = [desc[0] for desc in cursor.description]

    print(f"\n  Aperçu de {nom_table} ({limite} premières lignes) :")
    print(f"  {noms_colonnes}")
    for ligne in lignes:
        print(f"  {list(ligne)}")

    cursor.close()


# Fonction : Compter les lignes d'une table
def compter_lignes(conn, nom_table):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {nom_table}")
    count = cursor.fetchone()[0]
    cursor.close()
    return count


# Exécution principale
if __name__ == '__main__':

    print("=" * 55)
    print("  Chargement des données dans les tables staging")
    print("=" * 55)

    #Connexion à PostgreSQL
    print("\n[1/5] Connexion à PostgreSQL...")
    conn = connecter_db()

    #Création des tables (exécute le fichier SQL)
    print("\n[2/5] Création des tables...")
    executer_fichier_sql(conn, 'sql/01_create_tables.sql')

    #Vider les tables staging (au cas où)
    print("\n[3/5] Nettoyage des tables staging...")
    vider_table(conn, 'staging_clients')
    vider_table(conn, 'staging_transactions')

    #Chargement des CSV
    print("\n[4/5] Chargement des fichiers CSV...")

    colonnes_clients = [
        'client_id', 'nom', 'prenom', 'date_naissance',
        'genre', 'email', 'telephone', 'ville', 'segment'
    ]
    print(f"\n  Chargement de {CSV_CLIENTS}...")
    charger_csv(conn, CSV_CLIENTS, 'staging_clients', colonnes_clients)

    colonnes_transactions = [
        'transaction_id', 'client_id', 'nom_agence', 'ville_agence',
        'region_agence', 'date_transaction', 'type_transaction',
        'montant', 'canal', 'statut', 'frais'
    ]
    print(f"\n  Chargement de {CSV_TRANSACTIONS}...")
    charger_csv(conn, CSV_TRANSACTIONS, 'staging_transactions', colonnes_transactions)

    #Vérification
    print("\n[5/5] Vérification du chargement...")
    nb_clients = compter_lignes(conn, 'staging_clients')
    nb_transactions = compter_lignes(conn, 'staging_transactions')

    print(f"\n  staging_clients     : {nb_clients} lignes")
    print(f"  staging_transactions : {nb_transactions} lignes")

    afficher_apercu(conn, 'staging_clients')
    afficher_apercu(conn, 'staging_transactions')

    # Fermeture de la connexion
    conn.close()
    print("\n")
    print("  Chargement terminé avec succès !")
