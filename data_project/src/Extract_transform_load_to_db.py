# coding: utf-8

# import des modules necessaires au bon fonctionnement de l'application
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime


# connexion à la base de données postgreSQL via psycopg2 pour DDL
conn = psycopg2.connect(
    host = "localhost",
    user = "postgres",
    password = "admin",
    database = "test_DB"
)

# connexion à la base de données postgreSQL via sqlalchemy pour DML
conn_2 = "postgresql://postgres:admin@localhost:5432/test_DB"
con_engine = create_engine(conn_2)

# création d'un curseur pour permettre l'exécution des requêtes sur les tables de la bases client
cur = conn.cursor()

# création de la table client s'il elle existe pas
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS client (
    nom VARCHAR(50) NOT NULL, 
    prenom VARCHAR(50) NOT NULL,
    age INT NOT NULL, 
    nom_prenom VARCHAR(100) NOT NULL,
    date_traitement VARCHAR(100) NOT NULL,
    date_ingestion timestamp NOT NULL,
    date_dernire_modif timestamp NOT NULL,
    PRIMARY KEY(nom, prenom)
    )
    """
)

conn.commit()

# Extraction des données
path = "D:/data_engineer/python/data/output/client_tous_magasins.csv"
df = pd.read_csv(path)

# Transformation : Ajout des colonnes date_ingestion et date_dernire_modif
df_temp = df.copy()
df_temp['date_ingestion'] = datetime.now()
df_temp['date_dernire_modif'] = datetime.now()

# Chargement des données en mode ajout dans la table client
df_temp.to_sql('client',con_engine, if_exists='append', index=False)


# vérification de la présence des données insérés dans la table client
sql = "SELECT * FROM client"
df = pd.read_sql(sql, con_engine)
print(df.head())
cur.execute(
    """
    SELECT COUNT(*) as nombre_record 
    FROM CLIENT
    """
)
print(cur.fetchone())

# Fermeture du curseur et la connexion de type DML
cur.close()
conn.close()