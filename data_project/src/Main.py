# coding: utf-8
from utils.fonctions import *

print("***********************************************************************************************************")
print("*                              Manipulation de données avec python - pandas                               *")
print("***********************************************************************************************************")

# Chargement du jeu de donnée 
df = load_csv_data('~/OneDrive/Bureau/TALEND/DOSSIER_TALEND/Inputs/employes.csv')

# Affichage des dimension du Data 
data_frame_shap(df)

# Description et information sur les données
data_frame_description(df)
