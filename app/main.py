from fastapi import FastAPI, HTTPException
from pathlib import Path
from app.utils.time_series import load_energy_consumption_data ,save_data_to_influxdb
from app.services.prediction_service import apply_arima_model,save_predictions_to_postgres,get_influx_data,connect_postgresql
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from pydantic import BaseModel
import os

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
    global data_cache  # Permet de modifier la variable globale

    try:
        print("📂 Vérification de l'existence du fichier CSV...")
        if not os.path.exists(str(FILE_CSV)):  # Vérifier si le fichier existe
            raise HTTPException(status_code=404, detail="Fichier introuvable. Vérifiez le chemin.")

        # Charger les données
        df = load_energy_consumption_data(str(FILE_CSV))

        print("🔍 Vérification des premières lignes du DataFrame...")
        print(df.head())

        # Vérification des colonnes nécessaires
        if not all(col in df.columns for col in ['Year', 'Month', 'Day', 'Hour']):
            raise HTTPException(status_code=400, detail="Erreur : Les colonnes 'Year', 'Month', 'Day', 'Hour' sont manquantes.")

        # Reconstituer la colonne 'Timestamp'
        df['Timestamp'] = pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour']], errors='coerce')

        if 'Timestamp' not in df.columns or df['Timestamp'].isnull().all():
            raise HTTPException(status_code=400, detail="Erreur : La colonne 'Timestamp' est absente ou contient des valeurs invalides.")

        # Enregistrer les données
        data_cache = df

        # Sauvegarder dans InfluxDB
        save_data_to_influxdb(df)

        # Retourner les données sous forme de dictionnaire
        data_dict = df[['Timestamp', 'Temperature', 'Humidity', 'SquareFootage', 'Occupancy', 'RenewableEnergy', 'EnergyConsumption']].head().to_dict(orient="records")
        return {"data": data_dict}

    except Exception as e:
        print(f"❌ Exception capturée : {e}")
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
    


class PredictionRequest(BaseModel):
    steps: int = 30

@app.post("/predict")
async def predict(request: PredictionRequest):
    """Endpoint pour récupérer les données, faire une prédiction et enregistrer les résultats."""
    
    # Étape 1 : Charger les données depuis InfluxDB
    data = get_influx_data()
    
    print("Données récupérées :", data)  # Ajout pour voir les données
    
    # Vérification
    if "energyConsumption" not in data.columns:
        raise ValueError("Les données récupérées ne contiennent pas 'energyConsumption'.")

    # Étape 2 : Appliquer le modèle ARIMA pour prédire
    forecast_df = apply_arima_model(data, steps=request.steps)

    # Étape 3 : Enregistrer les prévisions dans PostgreSQL
    save_predictions_to_postgres(forecast_df)

    return {"message": "Les prévisions ont été générées et enregistrées avec succès."}




@app.get("/predictions")
async def get_predictions():
    """Endpoint pour récupérer les prévisions enregistrées dans PostgreSQL"""
    conn = connect_postgresql()
    cursor = conn.cursor()
    
    cursor.execute("SELECT timestamp, forecast FROM predictions ORDER BY timestamp DESC")
    data = cursor.fetchall()

    cursor.close()
    conn.close()

    if not data:
        return {"message": "Aucune prévision disponible."}

    predictions = [{"timestamp": row[0], "forecast": row[1]} for row in data]
    return {"predictions": predictions}