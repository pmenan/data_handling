# coding: utf-8

# import des modules 
from utils.elt_functions import *
from datetime import datetime
from configparser import ConfigParser

#lecture du fichier de configuration
parser = ConfigParser() 
parser.read('D:/git_repo/data_handling/data_project/src/config_load_to_local.ini')

# Récupération des paramètres [extract_data] 
path_to_extract = parser.get('extract_data', 'path_to_extract')
file_mask = parser.get('extract_data', 'file_mask')
fiel_sep = parser.get('extract_data', 'fiel_sep')
path_to_archive = parser.get('extract_data', 'path_to_archive')

# Récupération des paramètres [transform]
col_cal_to_add = parser.get('transform', 'col_cal_to_add')
col_to_concat1 = parser.get('transform', 'col_to_concat1')
col_to_concat2 = parser.get('transform', 'col_to_concat2')
sort_column = parser.get('transform', 'sort_column')
col_to_add = parser.get('transform', 'col_to_add')


# Récupération des paramètres [load_data_into_local_dir]
path_to_load = parser.get('load_data_into_local_dir', 'path_to_load')
file_name = parser.get('load_data_into_local_dir', 'file_name')

# Fonction etl d'Extraction, de transformation et de chargement de données.
def etl(path_to_extract, path_to_archive, file_mask, fiel_sep, col_cal_to_add, col_to_concat1, col_to_concat2, 
        sort_column, col_to_add, value_to_add, path_to_load, file_name):
    #Extraction
    df = data_extraction(path_to_extract, file_mask, fiel_sep)
    print("Extraction des données          -------> OK\n")
    
    #Transformation
    df1 = data_transformation(df, col_cal_to_add, col_to_concat1, col_to_concat2, sort_column, col_to_add, value_to_add)
    print("Transformation des données      -------> OK\n")
    
    #Charegement
    load_data(df1, path_to_load, file_name)
    print("Chargement des données          -------> OK\n")
   
    #Archivage des fichiers traités
    move_source_file(path_to_extract, path_to_archive)
    print("Archivage des fichiers traités   ------> OK\n")
    
    #Suppression des fichiers traités du répertoire source
    #delete_input_file(path_to_extract)
    #print("Suppression des fichiers traités ------> OK\n")


# Date et heure de début du traitement 
now = datetime.now()
debut_traitement = now.strftime("%Y%m%d-%H:%M:%S")
print(f"Début traitement : {debut_traitement} \n")

# Appel à la fonction etl
value_to_add = now.strftime("%Y%m%d-%H%M%S")
etl(path_to_extract, path_to_archive, file_mask, fiel_sep, col_cal_to_add, col_to_concat1, col_to_concat2, 
    sort_column, col_to_add, value_to_add, path_to_load, file_name)

# Date et heure de fin du traitement 
fin_traitement = now.strftime("%Y%m%d-%H:%M:%S")
print(f"Fin traitement : {fin_traitement} \n")