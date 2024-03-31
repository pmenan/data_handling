# coding: utf-8
from utils.fonctions import *
import random
import pandas as pd
from datetime import datetime
from configparser import ConfigParser

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

######################################################################################################################################
###################### Résolution avec programmation fonctionnelle ###################################################################
######################################################################################################################################

#lecture du fichier de configuration
parser = ConfigParser() 
parser.read('D:/git_repo/data_handling/data_project/src/config_load_to_aws.ini')

# Récupération des paramètres [extract_data] 
path = parser.get('extract_data', 'file_source_path')

# Récupération des paramètres [transform]
random_value1 = parser.get('transform', 'random_value1')
random_value1 = int(random_value1)
random_value2 = parser.get('transform', 'random_value2')
random_value2 = int(random_value2)
column = parser.get('transform', 'column')

# Récupération des paramètres [load_data_in_s3]
s3_destination = parser.get('load_data_in_s3', 's3_destination')
file_name_to_load = parser.get('load_data_in_s3', 'file_name_to_load')
file_format = parser.get('load_data_in_s3', 'file_format')

# Date et heure du traitement 
now = datetime.now()
date_time_str = now.strftime("%Y%m%d_%H%M%S")
key = file_name_to_load+'_'+date_time_str+'.'+file_format

#Appel fonction ETL => Extract from local, transform and load into aws s3 Bucjet Data Lake
etl_function(path, column, random_value1, random_value2, s3_destination, key)