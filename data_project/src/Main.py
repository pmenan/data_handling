# coding: utf-8
from utils.fonctions import *
import random
import pandas as pd

print("***********************************************************************************************************")
print("*                              Manipulation de données avec python - pandas                               *")
print("***********************************************************************************************************")

# Chargement du jeu de donnée 
#df = load_csv_data('~/OneDrive/Bureau/TALEND/DOSSIER_TALEND/Inputs/employes.csv')

# Affichage des dimension du Data 
#data_frame_shap(df)

# Description et information sur les données
#data_frame_description(df)

# création d'un nouveau data df_2 sur la base de df.
# la colonne 'age' est ajoutée, rempli avec des valeurs random compris entre 16 et 98 ans
#df_2 = add_random_col(df, 'age', 16, 98)

# vérfication des distincts valeurs de la colonne age
#print(df_2['age'].value_counts())

# chargements des données dans Amazon s3 bucket
#load_data_into_aws_s3_bucket(df_2, 'source-data-1234', 'fichier_client.csv')
path = '~/OneDrive/Bureau/TALEND/DOSSIER_TALEND/Inputs/employes.csv'
column = 'age'
random_value1 = 16
random_value2 = 98
s3_source = 'destination-data-1234'
key = 'fichier_client.csv'
etl_function(path, column, random_value1, random_value2, s3_source, key)