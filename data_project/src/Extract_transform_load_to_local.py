# coding: utf-8

# import des modules 
from utils.elt_functions import *
from utils.transformation_functions import *
import random
import pandas as pd
from datetime import datetime
from configparser import ConfigParser


# Fonction etl d'Extraction, de transformation et de chargement de données.
def etl(path_to_extract, file_mask, fiel_sep, col_cal_to_add, col_to_concat1, col_to_concat2, sort_column, col_to_add, value_to_add, path_to_load, file_name):
    #Extraction
    df = data_extraction(path_to_extract, file_mask, fiel_sep)
    #Transformation
    df1 = data_transformation(df, col_cal_to_add, col_to_concat1, col_to_concat2, sort_column, col_to_add, value_to_add)
    #Charegement
    load_data(df1, path_to_load, file_name)

# Appel à la fonction etl
path_to_extract = "../data/input"
file_mask = "*.csv"
fiel_sep = ";" 
col_cal_to_add = 'nom_prenom', 
col_to_concat1 = "nom"
col_to_concat2 = "prenom" 
sort_column = "age"
col_to_add = "date_traitement"
path_to_load = "../data/output/"
file_name = "client_tous_magasins.csv" 
now = datetime.now()
value_to_add = now.strftime("%Y%m%d-%H%M%S")
etl(path_to_extract, file_mask, fiel_sep, col_cal_to_add, col_to_concat1, col_to_concat2, sort_column, col_to_add, value_to_add, path_to_load, file_name)