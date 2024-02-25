# coding: utf-8

# import des lib necessaires aux fonctionnement des fonctions
import os
import glob
import pandas as pd
from datetime import datetime


#################
# Fonctions ETL #
#################

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


###############################
# Fonctions de transformation #
###############################
    
# Ajouter une colonne à partir de deux colonnes du DataFrame d'entrée
def add_contact_colums(df, col_cal_to_add, col_to_concat1, col_to_concat2):
    """
    @df : DataFrame --> contenant les données
    @col_cal_to_add : String --> nouvelle colonne à ajouté au DataFrame 
    @col_to_concat1 : String --> 1ere colonne à concatener pour construire @col_to_add
    @col_to_concat2 : String --> 2eme colonne à concatener pour construire @col_to_add

    Return : Un nouveau DataFrame avec la nouvelle colonne @col_to_add
    """
    col = df.columns
    df_transform = df.copy()
    # Ajout de la colonne col_to_add
    if col_to_concat1 in col and col_to_concat2 in col:
        df_transform[col_cal_to_add] = df[col_to_concat1]+" "+df[col_to_concat2]
    else:
        print(f"Revoir les données, le DataFrame d'entré ne contient pas les colonnes {col_to_concat1} et {col_to_concat1}")
    return df_transform

# Ajouter une colonne avec valeur constante au DataFrame d'entrée
def add_column(df, col_to_add, value_to_add):
    """
    @df : DataFrame --> contenant les données
    @col_to_add : String --> colonne à ajouter
    @value_to_add : Any --> Valeur à affecter à la colonne (nb : si 41, toutes les valeurs de la colonne @col_to_add seront 41)

    return : un nouveau DataFrame avec la nouvelle colonne
    """
    df_transform = df.copy()
    df_transform[col_to_add] = value_to_add
    return df_transform

# Trier un DataFrame en fonction d'une colonne
def sort_columns(df, sort_column):
    """
    @df : DataFrame --> contenant les données
    @sort_column : String --> colonne servant de critère de tri

    return : un nouveau DataFrame trié
    """
    if sort_column in df.columns:
        df_transform = df.sort_values(by=[sort_column], ascending=True)
    else:
        print(f"revoir les données, la colonne {sort_column} n'est pas un colonne du DataFrame d'entrée")
    return df_transform 

# Mettre les valeurs des colonnes de type String en majuscule
def make_Stringvalue_upper(df):
    """
    @df : DataFrame --> contenant les données
    return : un nouveau DataFrame avec les valeurs des colonnes de string en Majuscule
    """
    df_transform = df.copy()
    cols = df_transform.select_dtypes(include=['object']).columns
    for col in cols:
        df_transform[col] = df_transform[col].apply(lambda x: x.upper())
    return df_transform