# coding: utf-8

import pandas as pd
import random
import logging
import boto3
from io import StringIO
from botocore.exceptions import ClientError

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

def add_random_col(df, column:str, random_value1:int, random_value2:int):
    """
    Ajoute une colonne au DataFrame et lui affecte des valeurs entières aléatoires

    param:
        df : DataFrame auquel ajouter la nouvelle colonne
        column : colonne à ajouter au DataFrame
        random_value1 : valeur minimale pour la colonne
        random_value2 : valeur maximale pour la colonne
   
    Prerequis :
        df doit être un dataFrame
        column doit être un string
        random_value1 doit être un intier
        random_value2 doit un être un entier


    return: 
        True si aucune erreur rencontrée
        False si le paramètre n'est pas un DataFrame
    """
    # initalisation des valeurs de la colonne à 0
    df[column] = 0
    
    # mise à jour des valeurs de la colonne avec les valeurs aléatoires
    try:
        for index in range(0, df.shape[0]):
            df[column][index] = random.randint(random_value1,random_value2)
    except:
        print("Erreur : veillez vérifier le type des paramètres renseignés")
        return False
    print(df.head())
    return df

def load_data_into_aws_s3_bucket(df, s3_source:str, key:str):
    """
    Chargement des données dans s3 Bucket

    param:
        df : DataFrame qui contient les données à uploader dans s3
        s3_source : nom ndu target bucket
        key : nom du fichier source
   
    Prerequis :
        df doit être un dataFrame
        s3_source doit être un string
        key doit être un string
        random_value2 doit un être un entier

    return: 
        True si aucune erreur rencontrée
        False si le paramètre n'est pas un DataFrame
    """
    try:
        s3 = boto3.resource('s3')
        target_bucket = s3.Bucket(s3_source)
        out_buffer = StringIO()
        df.to_csv(out_buffer, index = False)
        target_bucket.put_object(Body = out_buffer.getvalue(), Key = key)
    except ClientError as e:
        logging.error(e)
        return False
    print ("Chargement OK")
    return True