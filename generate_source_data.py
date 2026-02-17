#Fichier python pour generer automatiquement des données que nous allons inserer dans le Warehouse
#Insersion d'erreur volontaire pour rentres ces données plausible qui sera traité durant le projet

import csv
import random
from datetime import datetime, timedelta
from faker import Faker


fake = Faker('fr_FR')

# Pour avoir les mêmes données à chaque exécution
Faker.seed(42)
random.seed(42)

# Configuration
NB_CLIENTS = 500
NB_TRANSACTIONS = 5000
OUTPUT_CLIENTS = 'data/source_clients.csv'
OUTPUT_TRANSACTIONS = 'data/source_transactions.csv'


# Données de référence
SEGMENTS = ['Particulier', 'Premium', 'Professionnel', 'Etudiant', 'Senior']

AGENCES = [
    {'nom': 'Agence Paris Opéra', 'ville': 'Paris', 'region': 'Île-de-France', 'code_postal': '75009', 'directeur': 'Marie Laurent'},
    {'nom': 'Agence Lyon Bellecour', 'ville': 'Lyon', 'region': 'Auvergne-Rhône-Alpes', 'code_postal': '69002', 'directeur': 'Pierre Dumont'},
    {'nom': 'Agence Marseille Vieux-Port', 'ville': 'Marseille', 'region': 'Provence-Alpes-Côte d\'Azur', 'code_postal': '13001', 'directeur': 'Sophie Martin'},
    {'nom': 'Agence Toulouse Capitole', 'ville': 'Toulouse', 'region': 'Occitanie', 'code_postal': '31000', 'directeur': 'Jean Moreau'},
    {'nom': 'Agence Bordeaux Centre', 'ville': 'Bordeaux', 'region': 'Nouvelle-Aquitaine', 'code_postal': '33000', 'directeur': 'Claire Dubois'},
    {'nom': 'Agence Nantes Commerce', 'ville': 'Nantes', 'region': 'Pays de la Loire', 'code_postal': '44000', 'directeur': 'Thomas Bernard'},
    {'nom': 'Agence Strasbourg Kléber', 'ville': 'Strasbourg', 'region': 'Grand Est', 'code_postal': '67000', 'directeur': 'Anne Petit'},
    {'nom': 'Agence Lille Flandres', 'ville': 'Lille', 'region': 'Hauts-de-France', 'code_postal': '59000', 'directeur': 'François Leroy'},
    {'nom': 'Agence Rennes République', 'ville': 'Rennes', 'region': 'Bretagne', 'code_postal': '35000', 'directeur': 'Isabelle Roux'},
    {'nom': 'Agence Nice Masséna', 'ville': 'Nice', 'region': 'Provence-Alpes-Côte d\'Azur', 'code_postal': '06000', 'directeur': 'Philippe Garcia'},
]

TYPES_TRANSACTION = ['Virement', 'Retrait', 'Depot', 'Paiement carte', 'Prelevement']
CANAUX = ['Guichet', 'En ligne', 'Mobile', 'GAB']
STATUTS = ['Effectuee', 'En cours', 'Echouee']

# Formats de genre volontairement incohérents (erreur simulée)
GENRES_PROPRES = ['Homme', 'Femme']
GENRES_SALES = ['Homme', 'Femme', 'M', 'F', 'Masculin', 'Feminin', 'H', 'Male', 'Female', '']


# Fonction : Générer les clients
def generer_clients():
    """
    Génère une liste de clients avec des erreurs volontaires :
    - Genres dans des formats incohérents
    - Emails parfois vides ou invalides
    - Doublons volontaires
    - Dates de naissance parfois manquantes
    """
    clients = []

    for i in range(1, NB_CLIENTS + 1):
        prenom = fake.first_name()
        nom = fake.last_name()

        genre = random.choice(GENRES_SALES)

        # Email : 10% de chance d'être vide, 5% d'être invalide
        tirage_email = random.random()
        if tirage_email < 0.10:
            email = ''
        elif tirage_email < 0.15:
            email = f"{prenom}{nom}@"  # email invalide
        else:
            email = f"{prenom.lower()}.{nom.lower()}@{random.choice(['gmail.com', 'yahoo.fr', 'outlook.com', 'hotmail.fr'])}"

        telephone = fake.phone_number()

        if random.random() < 0.08:
            date_naissance = ''
        else:
            date_naissance = fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d')

        ville = fake.city()

        segment = random.choice(SEGMENTS)

        clients.append({
            'client_id': i,
            'nom': nom,
            'prenom': prenom,
            'date_naissance': date_naissance,
            'genre': genre,
            'email': email,
            'telephone': telephone,
            'ville': ville,
            'segment': segment,
        })

    #Injection de doublons (3% des clients)
    nb_doublons = int(NB_CLIENTS * 0.03)
    for _ in range(nb_doublons):
        client_original = random.choice(clients)
        doublon = client_original.copy()
        # Le doublon a un ID différent mais les mêmes infos
        doublon['client_id'] = len(clients) + 1
        clients.append(doublon)

    return clients


# Fonction : Générer les transactions
def generer_transactions(clients):
    """
    Génère une liste de transactions avec des erreurs volontaires :
    - Montants négatifs (erreur)
    - Dates invalides
    - Transactions orphelines (client_id inexistant)
    - Frais parfois manquants
    """
    transactions = []
    client_ids = [c['client_id'] for c in clients]

    # Période : transactions sur 2 ans (2024-2025)
    date_debut = datetime(2024, 1, 1)
    date_fin = datetime(2025, 12, 31)
    nb_jours = (date_fin - date_debut).days

    for i in range(1, NB_TRANSACTIONS + 1):

        # Client : 3% de chance d'être orphelin (client inexistant)
        if random.random() < 0.03:
            client_id = random.randint(9000, 9999)  # ID qui n'existe pas
        else:
            client_id = random.choice(client_ids)

        # Agence
        agence = random.choice(AGENCES)

        # Date : 2% de chance d'être invalide
        if random.random() < 0.02:
            date_transaction = '2025-13-45'  # date invalide
        else:
            jours_aleatoires = random.randint(0, nb_jours)
            date_transaction = (date_debut + timedelta(days=jours_aleatoires)).strftime('%Y-%m-%d')

        # Type de transaction
        type_transaction = random.choice(TYPES_TRANSACTION)

        # Montant : 2% de chance d'être négatif (erreur)
        montant = round(random.uniform(10, 15000), 2)
        if random.random() < 0.02:
            montant = -montant

        # Canal
        canal = random.choice(CANAUX)

        # Statut : pondéré (90% effectuée, 5% en cours, 5% échouée)
        statut = random.choices(STATUTS, weights=[90, 5, 5])[0]

        # Frais : entre 0 et 25€, 5% de chance d'être vide
        if random.random() < 0.05:
            frais = ''
        else:
            frais = round(random.uniform(0, 25), 2)

        transactions.append({
            'transaction_id': i,
            'client_id': client_id,
            'nom_agence': agence['nom'],
            'ville_agence': agence['ville'],
            'region_agence': agence['region'],
            'date_transaction': date_transaction,
            'type_transaction': type_transaction,
            'montant': montant,
            'canal': canal,
            'statut': statut,
            'frais': frais,
        })

    return transactions


# Fonction : Sauvegarder en CSV
def sauvegarder_csv(donnees, chemin, colonnes):
    """Sauvegarde une liste de dictionnaires en fichier CSV."""
    with open(chemin, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=colonnes)
        writer.writeheader()
        for ligne in donnees:
            writer.writerow(ligne)
    print(f"Fichier généré : {chemin} ({len(donnees)} lignes)")


# Exécution principale
if __name__ == '__main__':

    print("=" * 50)
    print("Génération des données sources bancaires")
    print("=" * 50)

    #Générer les clients
    print("\n[1/3] Génération des clients...")
    clients = generer_clients()

    colonnes_clients = [
        'client_id', 'nom', 'prenom', 'date_naissance',
        'genre', 'email', 'telephone', 'ville', 'segment'
    ]
    sauvegarder_csv(clients, OUTPUT_CLIENTS, colonnes_clients)

    #Générer les transactions
    print("[2/3] Génération des transactions...")
    transactions = generer_transactions(clients)

    colonnes_transactions = [
        'transaction_id', 'client_id', 'nom_agence', 'ville_agence',
        'region_agence', 'date_transaction', 'type_transaction',
        'montant', 'canal', 'statut', 'frais'
    ]
    sauvegarder_csv(transactions, OUTPUT_TRANSACTIONS, colonnes_transactions)

    #Résumé
    print("\n[3/3] Résumé :")
    print(f"  - {len(clients)} clients générés (dont doublons)")
    print(f"  - {len(transactions)} transactions générées")
    print(f"  - Erreurs injectées : genres incohérents, emails invalides,")
    print(f"    dates invalides, montants négatifs, transactions orphelines")
    print("\nFichiers prêts dans le dossier data/")
