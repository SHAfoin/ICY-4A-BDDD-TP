# Étape 1: Connexion à la base de données MongoDB
import pandas as pd
import pymongo
import matplotlib.pyplot as plt


client = pymongo.MongoClient('mongodb://localhost:27017/')

db = client['TP_01_new'] # Nom de la base de données
collection = db['SuperHeros'] # Nom de la collection

# CREER INDEX
# collection.create_index([("name", pymongo.ASCENDING), ("powerstats.intelligence", pymongo.ASCENDING), ("biography.publisher", pymongo.ASCENDING)])

# DATAFRAME PANDA
# data = list(collection.find())
# df = pd.DataFrame(data)
# print(df) 

# ANALYSE DE DONNEES AVEC PANDA
def supprimer_colonnes_inutiles (data) :
    for x in data :
        x.pop('id')
        x.pop('work')
        x.pop('connections')
        x.pop('images')

        if x['appearance']['height'] :
            x['height'] = x['appearance']['height']
        if x['appearance']['weight'] :
            x['weight'] = x['appearance']['weight']
        x.pop('appearance')

        x['publisher'] = x['biography']['publisher']
        x.pop('biography')

        if x['powerstats']['intelligence'] != None :
            x['intelligence'] = x['powerstats']['intelligence']
        if x['powerstats']['strength'] != None :
            x['strength'] = x['powerstats']['strength']
        if x['powerstats']['speed'] != None :
            x['speed'] = x['powerstats']['speed']
        if x['powerstats']['durability'] != None :
            x['durability'] = x['powerstats']['durability']
        if x['powerstats']['power'] != None :
            x['power'] = x['powerstats']['power']
        if x['powerstats']['combat'] != None :
            x['combat'] = x['powerstats']['combat']
        x.pop('powerstats')

def valeurs_manquantes (colonnesSupprimees, data) :
    for x in data :
        if colonnesSupprimees :
            if type(x['height']) != float and (x['height'] == None or x['height'].includes('-') ):
                x.pop('height')
            if type(x['weight']) != float and (x['weight'] == None or x['weight'].includes('-') ) :
                x.pop('weight')
            if x['intelligence'] == None or (x['intelligence'] < 0 or x['intelligence'] > 100) :
                x.pop('intelligence')
            if x['strength'] == None or (x['strength'] < 0 or x['strength'] > 100) :
                x.pop('strength')
            if x['speed'] == None  or (x['speed'] < 0 or x['speed'] > 100) :
                x.pop('speed')
            if x['durability'] == None  or (x['durability'] < 0 or x['durability'] > 100) :
                x.pop('durability')
            
            if x['power'] == None  or (x['power'] < 0 or x['power'] > 100) :
                x.pop('power')
            if x['combat'] == None  or (x['combat'] < 0 or x['combat'] > 100) :
                x.pop('combat')
            if x['publisher'] == None or x['publisher'] == '' :
                x.pop('publisher')
        else :
            if x['appearance']['height'].includes('-') or x['appearance']['height'] == None:
                x['appearance'].pop('height')
            if x['appearance']['weight'].includes('-') or x['appearance']['weight'] == None :
                x['appearance'].pop('weight')
            if x['powerstats']['intelligence'] == None or (x['powerstats']['intelligence'] < 0 or x['powerstats']['intelligence'] > 100) :
                x['powerstats'].pop('intelligence')
            if x['powerstats']['strength'] == None or (x['powerstats']['strength'] < 0 or x['powerstats']['strength'] > 100) :
                x['powerstats'].pop('strength')
            if x['powerstats']['speed'] == None  or (x['powerstats']['speed'] < 0 or x['powerstats']['speed'] > 100) :
                x['powerstats'].pop('speed')
            if x['powerstats']['durability'] == None  or (x['powerstats']['durability'] < 0 or x['powerstats']['durability'] > 100) :
                x['powerstats'].pop('durability')
            if x['powerstats']['power'] == None  or (x['powerstats']['power'] < 0 or x['powerstats']['power'] > 100) :
                x['powerstats'].pop('power')
            if x['powerstats']['combat'] == None  or (x['powerstats']['combat'] < 0 or x['powerstats']['combat'] > 100) :
                x['powerstats'].pop('combat')
            if x['biography']['publisher'] == None or x['biography']['publisher'] == '' :
                x['biography'].pop('publisher')

def normalisation  (colonnesSupprimees, data): 
   
    for x in data :
        if colonnesSupprimees :
            if x['height'] :
                x['height'] = x['height'][1].split(' ')[0]
                try:
                    x['height'] = float(x['height'])
                except:
                    x['height'] = None
            if x['weight'] :
                x['weight'] = x['weight'][1].split(' ')[0]
                try:
                    x['weight'] = float(x['weight'])
                except:
                    x['weight'] = None
        else :
            if x['appearance']['height'] :
                x['appearance']['height'] = x['appearance']['height'][1].split(' ')[0]
                try:
                    x['appearance']['height'] = float(x['appearance']['height'])
                except:
                    x['appearance']['height'] = None
            if x['appearance']['weight'] :
                x['appearance']['weight'] = x['appearance']['weight'][1].split(' ')[0]
                try:
                    x['appearance']['weight'] = float(x['appearance']['weight'])
                except:
                    x['appearance']['weight'] = None

data = list(collection.find())

colonnesSupprimees = False
print("NORMALISATION DES DONNEES\n----------------------------")
    
term = input("Voulez vous supprimer les colonnes inutiles ? (1: Oui, 0: Non)")
if term == "1":
    supprimer_colonnes_inutiles(data)
    colonnesSupprimees = True

term = input("Voulez vous normaliser les données ? (1: Oui, 0: Non)")
if term == "1":
    normalisation(colonnesSupprimees,data)

term = input("Voulez vous supprimer les valeurs manquantes ? (1: Oui, 0: Non)")
if term == "1":
    valeurs_manquantes(colonnesSupprimees,data)

print("Normalisation terminée, affichage d'un échantillon :\n----------------------------")

df = pd.DataFrame(data)
print(df) 

# EXPLORATION DES DONNEES
# Moyenne
df2 = df["height"].mean()
print("Moyenne de la taille : ", df2)
# Median
df3 = df["height"].median()
print("Médiane de la taille : ", df3)
# Variance
df4 = df["height"].var()
print("Variance de la taille : ", df4)

# VISUALISATION DES DONNEES
# Plot de l'intelligence et la force
plot = df[['intelligence', 'speed']].plot(title="Dataframe")
plt.show()

plot = df.where((df['publisher'] == "Marvel Comics") | (df['publisher'] == "Dark Horse Comics")).groupby("publisher").size().plot(kind="bar",title="Publisher")
plt.show()

db.command({
    "create" : "Smart SuperHeros",
    "viewOn" : "SuperHeros",
    "pipeline" : [
        {
            "$match" : {
                "powerstats.intelligence" : {
                    "$gte" : 50,
                }
            }
        },
        {
            "$project" : {
                "name" : 1,
            }
        }
    ]})

db.command({
    "create" : "SuperHeros par force",
    "viewOn" : "SuperHeros",
    "pipeline" : [
        {   
            "$sort" : {
                'powerstats.strength' : 1,
            }
        },
        {   
            "$project" : {
                'powerstats.strength' : 1,
                "name" : 1,
            }
        }
    ]})


avg_intell = df["intelligence"].mean()
print("Moyenne de l'intelligence : ", avg_intell)

db.command({
    "create" : "True Smart SuperHeros",
    "viewOn" : "SuperHeros",
    "pipeline" : [
        {
            "$match" : {
                "powerstats.intelligence" : {
                    "$gte" : avg_intell,
                }
            }
        },
        {
            "$project" : {
                "name" : 1,
                'powerstats.intelligence' : 1,
            }
        }
    ]})

collection2 = db["True Smart SuperHeros"] # Nom de la collection
data = list(collection2.find())
print(data)
df = pd.DataFrame(data)
plot = df.plot(title="Dataframe")
plt.show()
