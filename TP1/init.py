import json
import pymongo
import numpy
import pandas
import matplotlib

# Étape 1: Connexion à la base de données MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')

db = client['TP_01_new'] # Nom de la base de données
collection = db['SuperHeros'] # Nom de la collection

# Étape 2: Importer les données des super-héros dans MongoDB
# 1. Préparer les données JSON
with open('SuperHerosComplet.json', 'r') as file:
    data = json.load(file)
    # 2. Insérer les données dans MongoDB
    result = collection.insert_many(data)
    print('Inserted {} documents'.format(len(result.inserted_ids)))

# 3. Vérifier les insertions
print(collection.find_one())