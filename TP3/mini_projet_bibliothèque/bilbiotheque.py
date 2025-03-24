import json
import boto3
from botocore.client import Config
from botocore.exceptions import *
import datetime
import uuid

import pandas as pd




# Configure AWS credentials (dummy values in this case)
boto3.setup_default_session(
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key=' wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKE',
    region_name='us-west-2'
)

def create_dynamodb_resource():
    """Crée une ressource DynamoDB connectée à l'instance locale."""
    return boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

def check_table_exists(dynamodb, table_name):
    # Utiliser le client DynamoDB pour lister les tables
    client = dynamodb.meta.client
    # Initialisation pour la pagination
    paginator = client.get_paginator('list_tables')
    page_iterator = paginator.paginate()
    # Parcourir toutes les tables pour voir si table_name existe
    for page in page_iterator:
        if table_name in page['TableNames']:
            return True
    return False

def create_table(dynamodb, table_name, id):
    """Crée une table DynamoDB."""
    
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': id,
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': id,
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        table.wait_until_exists()
        print(f"Table {table_name} created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table {table_name} already exists.")
        else:
            raise

def insert_item(dynamodb, table_name, item):
    """Insère un élément dans la table DynamoDB."""
    table = dynamodb.Table(table_name)
    table.put_item(Item=item)
    print(f"Item inserted: {item}")

def get_item(dynamodb, table_name, key):
    """Récupère un élément de la table DynamoDB."""
    table = dynamodb.Table(table_name)
    response = table.get_item(Key=key)
    return response.get('Item')

def init():
    """Point d'entrée du script."""
    dynamodb = create_dynamodb_resource()

    table_name = 'livres'
    # Créer la table
    if not check_table_exists(dynamodb, table_name):
        create_table(dynamodb, table_name, 'isbn')
        # Insérer et récupérer un élément
        with open('./livres.json', 'r') as file:
            data = json.load(file)
            for item in data:
                insert_item(dynamodb, table_name, item)

    table_name = 'Emprunts'
    # Créer la table
    if not check_table_exists(dynamodb, table_name):
        create_table(dynamodb, table_name, 'emprunt_id')

    user_interface(dynamodb)
    scan_all_items(dynamodb, table_name)

def create_livre(dynamodb, table_name, isbn, titre, auteur, annee_publication):
    livre = {
        'isbn': isbn,
        'titre': titre,
        'auteur': auteur,
        'annee_publication': annee_publication,
        'disponible': True
    }
    insert_item(dynamodb, table_name, livre)


def read_livre(dynamodb, table_name, key, value):

    if key == None or key == "" or value == None or value == "":
        return scan_all_items(dynamodb, table_name)

    table = dynamodb.Table(table_name)
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr(key).eq(value)
    )
    livres = response['Items']
    return livres

    

def update_livre(dynamodb, table_name, isbn, key, value):

    response = dynamodb.Table(table_name).update_item(
        Key={"isbn": isbn},
        UpdateExpression="set #name = :n",
        ExpressionAttributeNames={
            "#name": key,
        },
        ExpressionAttributeValues={
            ":n": value,
        },
        ReturnValues="UPDATED_NEW",
    )

def delete_livre(dynamodb, table_name, isbn):
    response = dynamodb.Table(table_name).delete_item(
        Key={
            'isbn': isbn
        }
    )


def scan_all_items(dynamodb,table_name):
    # Initialisation du client DynamoDB
    table = dynamodb.Table(table_name)
    print("Scanning table...")
    # Scan de la table
    response = table.scan()
    # Récupération des éléments
    items = response['Items']
    # Affichage des éléments

    # Gérer la pagination si la réponse est paginée
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        nextitems = response['Items']
        for item in nextitems:
            items.append(item)

    return items

def emprunter_livre (dynamodb, isbn, utilisateur) :

    # Ajouter un emprunt
    
    # Mettre le livre comme non disponible
    deja_emprunte = read_livre(dynamodb, 'livres', 'isbn', isbn)
    print(deja_emprunte)
    if deja_emprunte[0]['disponible'] == False :
        print("Livre déjà emprunté")
        return
    else :
        table_name = 'Emprunts'
        x = datetime.datetime.now()
        item = {
        'emprunt_id': str(uuid.uuid4()),
        'isbn': isbn,
        'utilisateur': utilisateur,
        'date_emprunt': x.strftime("%Y-%m-%d"),
        'date_retour': None,
        }
        insert_item(dynamodb, table_name, item)
        update_livre(dynamodb, 'livres', isbn, 'disponible', False)


def get_user_emprunts(dynamodb, table_name, utilisateur) :


    table = dynamodb.Table(table_name)
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr('utilisateur').eq(utilisateur)
    )
    emprunts = response['Items']

    table = dynamodb.Table('livres')
    liste_livre = []
    for emprunt in emprunts :
        response = read_livre(dynamodb, 'livres', 'isbn', emprunt['isbn'])
        liste_livre.append(response[0])

    return liste_livre


def rendre_livre(dynamodb, isbn) :

    update_livre(dynamodb, 'livres', isbn, 'disponible', True)

    x = datetime.datetime.now()

    table = dynamodb.Table('Emprunts')
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr('isbn').eq(isbn) & boto3.dynamodb.conditions.Attr('date_retour').eq(None)
    )
    emprunt_livre = response['Items'][0]
    print(emprunt_livre)

    response = dynamodb.Table('Emprunts').update_item(
        Key={"emprunt_id": emprunt_livre['emprunt_id']},
        UpdateExpression="set #name = :n",
        ExpressionAttributeNames={
            "#name": 'date_retour',
        },
        ExpressionAttributeValues={
            ":n": x.strftime("%Y-%m-%d"),
        },
        ReturnValues="UPDATED_NEW",
    )


def livre_par_auteur(dynamodb, auteur) :

    return read_livre(dynamodb, 'livres', 'auteur', auteur)

def emprunt_depasses(dynamodb, jours = 30) :

    items = scan_all_items(dynamodb, 'Emprunts')

    emprunts_depasses = []
    for item in items :
        item_date = datetime.datetime.strptime(item['date_emprunt'], '%Y-%m-%d').date()
        if item_date + datetime.timedelta(days=jours) < datetime.datetime.now().date() :
            emprunts_depasses.append(item)

    return emprunts_depasses

def print_items(items) :
    df = pd.DataFrame(items)
    print(df) 

def user_interface(dynamodb) :

    print("------- Bienvenue dans la bibliothèque")
    while True :
        choix = input("1. Ajouter un livre\n2. Lire un livre\n3. Emprunter un livre\n4. Rendre un livre\n5. Voir les emprunts d'un utilisateur\n6. Voir les livres d'un auteur\n7. Voir les emprunts dépassés\n8. Voir tous les emprunts\n9. Quitter\n")
        if choix == '1' :
            isbn = input("ISBN : ")
            titre = input("Titre : ")
            auteur = input("Auteur : ")
            annee_publication = input("Année de publication : ")
            create_livre(dynamodb, 'livres', isbn, titre, auteur, annee_publication)
        elif choix == '2' :
            key = input("Clé de recherche (par défaut : tous les livres): ")
            if key != "" :
                value = input("Valeur de recherche : ")
                print_items(read_livre(dynamodb, 'livres', key, value))
            else :
                print_items(read_livre(dynamodb, 'livres', None, None))
        elif choix == '3' :
            isbn = input("ISBN du livre à emprunter : ")
            utilisateur = input("Utilisateur : ")
            emprunter_livre(dynamodb, isbn, utilisateur)
        elif choix == '4' :
            isbn = input("ISBN du livre à rendre : ")
            rendre_livre(dynamodb,  isbn)
        elif choix == '5' :
            utilisateur = input("Utilisateur : ")
            print_items(get_user_emprunts(dynamodb, 'Emprunts', utilisateur))
        elif choix == '6' :
            auteur = input("Auteur : ")
            print_items(livre_par_auteur(dynamodb, auteur))
        elif choix == '7' :
            print_items(emprunt_depasses(dynamodb))
        elif choix == '8' :
            print_items(scan_all_items(dynamodb, 'Emprunts'))
        elif choix == '9' :
            break
        else :
            print("Choix invalide")



if __name__ == '__main__':
    init()