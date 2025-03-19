from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt

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
            # Conversion de la colonne 'Timestamp' en format datetime, en gardant les erreurs en 'NaT'
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            
            # Vérifiez que la colonne 'Timestamp' a été correctement convertie
            if df["Timestamp"].isnull().all():
                raise HTTPException(status_code=400, detail="Erreur : La colonne 'Timestamp' contient des valeurs invalides.")
        else:
            raise HTTPException(status_code=400, detail="Erreur : La colonne 'Timestamp' est absente du fichier.")
        
        # Extraction de l'année, du mois, du jour et de l'heure si nécessaire
        df['Year'] = df['Timestamp'].dt.year
        df['Month'] = df['Timestamp'].dt.month
        df['Day'] = df['Timestamp'].dt.day
        df['Hour'] = df['Timestamp'].dt.hour
        
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
file_path = "D:/PFE/DataSet/Energy_consumption.csv"