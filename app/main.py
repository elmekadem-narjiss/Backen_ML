from fastapi import FastAPI, HTTPException
from pathlib import Path
from app.utils.time_series import load_energy_consumption_data
from app.services.prediction_service import apply_arima_model
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA

app = FastAPI()


# Définir le chemin du fichier de données
DATASET_DIR = Path("D:/PFE/DataSet")
FILE_CSV = DATASET_DIR / "Energy_consumption.csv"
data_cache = None  # Stocke les données après /load-data

@app.get("/")
def root():
    return {"message": "Bienvenue dans le service de prédiction"}


@app.get("/load-data")
def load_data():
    """
    Route pour charger et afficher les données avec la reconstruction de la colonne 'Timestamp' à partir des autres colonnes.
    """
    global data_cache  # Permet de modifier la variable globale

    try:
        # Vérifier si le fichier existe
        if not FILE_CSV.exists():
            raise HTTPException(status_code=404, detail="Fichier introuvable. Vérifiez le chemin.")

        # Charger les données
        df = load_energy_consumption_data(str(FILE_CSV))

        # Déboguer : Afficher les premières lignes et les colonnes du DataFrame
        print("Premières lignes du DataFrame après chargement :")
        print(df.head())

        print("Colonnes du DataFrame après chargement :")
        print(df.columns)

        # Vérifier si les colonnes 'Year', 'Month', 'Day', 'Hour' existent
        if not all(col in df.columns for col in ['Year', 'Month', 'Day', 'Hour']):
            raise HTTPException(status_code=400, detail="Erreur : Les colonnes 'Year', 'Month', 'Day', 'Hour' sont manquantes.")

        # Reconstituer la colonne 'Timestamp' à partir des colonnes 'Year', 'Month', 'Day', 'Hour'
        df['Timestamp'] = pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour']])

        # Vérifier si la colonne 'Timestamp' a bien été reconstituée
        if 'Timestamp' not in df.columns:
            raise HTTPException(status_code=400, detail="Erreur : La colonne 'Timestamp' est absente après la reconstitution.")

        # Stocker les données chargées
        data_cache = df  # Enregistrer les données dans la variable globale

        # Déboguer : Afficher les types de données des colonnes pour voir si la colonne 'Timestamp' est correcte
        print("Types de données des colonnes après reconstitution de 'Timestamp' :")
        print(df.dtypes)

        # Convertir le DataFrame en dictionnaire (incluant la colonne 'Timestamp' et autres colonnes utiles)
        data_dict = df[['Timestamp', 'Temperature', 'Humidity', 'SquareFootage', 'Occupancy', 'RenewableEnergy', 'EnergyConsumption']].head().to_dict(orient="records")

        # Retourner les données au format JSON
        return {"data": data_dict}

    except Exception as e:
        # Déboguer : Afficher l'exception complète pour aider à identifier le problème
        print(f"Exception capturée : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du chargement des données : {e}")


@app.get("/forecast")
def forecast_data():
    """
    Route pour effectuer des prévisions sur les données de consommation d'énergie.
    """
    global data_cache  # On suppose que les données sont déjà chargées dans `data_cache`

    if data_cache is None:
        raise HTTPException(status_code=400, detail="Les données doivent d'abord être chargées via /load-data.")

    try:
        # Appliquer ARIMA sur les données chargées
        forecast_df = apply_arima_model(data_cache, steps=30)

        # Retourner les 10 dernières lignes (y compris les prévisions)
        return {"forecast": forecast_df.tail(10).to_dict(orient="records")}  # Utilisation de 'orient="records"' pour une réponse lisible

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la génération des prévisions : {e}")