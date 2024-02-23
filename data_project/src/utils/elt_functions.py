# coding: utf-8

# import des lib necessaires aux fonctionnement des fonctions
import os
import glob
import pandas as pd
from datetime import datetime
from utils.transformation_functions import *

# Extraction des données
def data_extraction(path, file_mask, fiel_sep):
    """
    @path : String --> chemin d'accès aux fichiers (exemple : "../data/input/")
    @file_mask : String --> masque des des fichiers prendre en compte pour le traitement (exemple : "*.csv")
    @file_sep : Char --> séparateur de champs (exemple ";")

    return : un DataFramme pandas contenant les données de tous les fichiers correspondant à @file_mask
    """
    try :
        file_liste = glob.glob(path+"/*.csv")
        frames = []
        df = ""
        if len(file_liste) == 0:
            print("pas de fichier trouvé")
        elif len(file_liste) == 1:
            df = pd.read_csv(file_liste, fiel_sep, index_col=False)
        else:
            for i in range(0, len(file_liste)): 
                df_i = pd.read_csv(file_liste[i], sep = fiel_sep, index_col=False) 
                frames.append(df_i)
            df = pd.concat(frames)
    except TypeError:
        print("Erreur : vérifier le nombre d'arguement passés à la fonction")
    return df

# Transformation des données
def data_transformation(df, col_cal_to_add, col_to_concat1, col_to_concat2, sort_column, col_to_add, value_to_add):
    df1 = add_contact_colums(df, col_cal_to_add, col_to_concat1, col_to_concat2)
    df2 = sort_columns(df1, sort_column)
    df3 = make_Stringvalue_upper(df2)
    df4 = add_column(df3, col_to_add, value_to_add)
    return df4

# Chargement des données dans le répertoire cible
def load_data(df, path_to_load, file_name):
    """
    @df : DataFrame --> contenant les données
    @path_to_load : String --> Répertoire ou stocker les données après transformation
    @file_name : String ---> Nom du fichier cible
    """
    df.to_csv(path_to_load+file_name, index=False)