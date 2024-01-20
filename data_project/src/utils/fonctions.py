# coding: utf-8

import pandas as pd
import random
import logging

def load_csv_data(path:str):
    """
    crèe un fichier csv à partir du paramètre path fourni en paramète

    param:
        path : chemin vers le fichier à traiter
    
    Prerequis : 
        Le delimiter de champ doit être ","
        La première colonne du fichier doit être l'entête.

    return:
        Affiche les 5 premières lignes du dataFrame si pas d'erreur
        retourne false si une erreur se produit
    """
    try:
        df = pd.read_csv(path)
    except NameError:
        print("Erreur : path non valide")
        return False
    print("Chargement Ok : \n")
    print(df.head())
    print("\n")
    return df

def data_frame_shap(df):
    """
    Affiche les dimensions du DataFrame (nombre de lignes et nombre de colones)

    return: 
        ne retourne rien
    """
    print(f"Dimension :  {df.shape}")
    print("\n")

def data_frame_description(df):
    """
    Affiche la description de chaque colonne du dataFrame 
    et l'ensemble des informations concernant les données du DataFrame

    param:
        df : DataFrame à décrire
    
    Prerequis :
        df doit être un dataFrame

    return: 
        True si aucune erreur rencontrée
        False si le paramètre n'est pas un DataFrame
    """
    try:
        print("Description des colonnes du DataFrame\n")
        print(df.describe(include='all'))
        print("\n")
        print("Type des colones et nombre de valeur non null pour chaque variable\n")
        print(df.info())
    except:
        print("Erreur : cette fonction prend en paramètre un DataFrame")
        return False
    return True



