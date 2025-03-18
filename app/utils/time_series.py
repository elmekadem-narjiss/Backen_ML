from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

app = FastAPI()

# Modèle Pydantic pour la validation des données envoyées à l'API
class EnergyDataInput(BaseModel):
    data: dict

def load_energy_consumption_data(file_path: str):
    """Charge et nettoie les données à partir d'un fichier CSV."""
    if not file_path or not file_path.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Erreur : Chemin de fichier invalide ou format incorrect.")

    try:
        df = pd.read_csv(file_path, sep=",")
        
        # Vérification et conversion du timestamp
        if 'Timestamp' in df.columns:
            df["timestamp"] = pd.to_datetime(df["Timestamp"], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            df.drop(columns=["Timestamp"], inplace=True)
        else:
            raise HTTPException(status_code=400, detail="Erreur : La colonne 'Timestamp' est absente du fichier.")

        # Suppression des lignes contenant des NaN
        df.dropna(axis=0, how='any', inplace=True)

        # Suppression des colonnes non numériques (catégorielles)
        df = df.select_dtypes(include=[np.number])

        print("Données chargées et nettoyées :")
        print(df.head())
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors du chargement des données : {e}")


# Chemin vers le fichier CSV
energy_consumption_path = "D:/PFE/DataSet/Energy_consumption.csv"

