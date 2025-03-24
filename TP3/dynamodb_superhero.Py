import boto3
from botocore.client import Config
from botocore.exceptions import *
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

def create_table(dynamodb, table_name):
    """Crée une table DynamoDB."""
    
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
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

def main():
    """Point d'entrée du script."""
    dynamodb = create_dynamodb_resource()
    table_name = 'Superheros'
    # Créer la table
    if not check_table_exists(dynamodb, table_name):
        create_table(dynamodb, table_name)
        # Insérer et récupérer un élément
        item = {'id': '1','name': 'A-Bomb','slug': '1-a-bomb','powerstats': {'intelligence': 38,'strength': 100,'speed': 17,'durability': 80,'power': 24,'combat': 64},'gender': 'Male','race': 'Human','height': "203 cm",'weight': "441 kg",'eyeColor': 'Yellow','hairColor': 'No Hair','placeOfBirth': 'Scarsdale, Arizona','firstAppearance': 'Hulk Vol 2 #2 (April, 2008) (as A-Bomb)','publisher': 'Marvel Comics','alignment': 'good','work': 'Musician, adventurer, author; formerly talk show host'}
        insert_item(dynamodb, table_name, item)
        item = {"id": '2',"nom": "Abe Sapien","intelligence": 88,"force": 28,"rapidité": 35," durabilité ": 65,"pouvoir": 100,"combat": 85}
        insert_item(dynamodb, table_name, item)
    retrieved_item = get_item(dynamodb, table_name, {'id': '1'})
    # print(f"Retrieved item: {retrieved_item}")
    scan_all_items(dynamodb, table_name)

    print("Trouver Abe Sapiens...")
    print(find_heroes(dynamodb, table_name,'nom','Abe Sapien'))

def scan_all_items(dynamodb,table_name):
    # Initialisation du client DynamoDB
    table = dynamodb.Table(table_name)
    print("Scanning table...")
    # Scan de la table
    response = table.scan()
    # Récupération des éléments
    items = response['Items']
    # Affichage des éléments
    for item in items:
        print(item)
    # Gérer la pagination si la réponse est paginée
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items = response['Items']
        for item in items:
            print(item)

def find_heroes(dynamodb,table,attribut, valeur):
    # Select the SuperHeroes table
    table = dynamodb.Table(table)
    # Scan for heroes with the specified power
    response = table.scan(
    FilterExpression=boto3.dynamodb.conditions.Attr(attribut).eq(valeur)
    )
    # Extract the items (heroes) from the response
    heroes = response['Items']
    # Return the list of heroes
    return heroes

if __name__ == '__main__':
    main()