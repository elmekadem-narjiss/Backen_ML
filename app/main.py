from fastapi import FastAPI, HTTPException
from pathlib import Path
from app.utils.time_series import load_energy_consumption_data ,save_data_to_influxdb
from app.services.prediction_service import apply_arima_model,save_predictions_to_postgres,get_influx_data,connect_postgresql
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from pydantic import BaseModel
import os
from app.services.enrich_data import add_time_features,load_data_from_postgres,save_to_influx
from app.config.config import INFLUX_URL, INFLUX_ORG,INFLUX_TOKEN,INFLUX_BUCKET
from influxdb_client import InfluxDBClient
import logging
#from app.services.lstm_model import get_influxdb_client,load_data_from_influx,prepare_data_for_lstm,train_lstm


#from app.services.lstm_model import get_influxdb_client,load_data_from_influx,train_lstm,prepare_data_for_lstm


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

        # Charger les données et récupérer le nombre de lignes
        df, nombre_de_lignes = load_energy_consumption_data(str(FILE_CSV))

        print(f"✅ {nombre_de_lignes} lignes chargées après nettoyage.")
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

        # Retourner les données sous forme de dictionnaire avec le nombre de lignes
        data_dict = df[['Timestamp', 'Temperature', 'Humidity', 'SquareFootage', 'Occupancy', 'RenewableEnergy', 'EnergyConsumption']].head().to_dict(orient="records")
        return {"nombre_de_lignes": nombre_de_lignes, "data": data_dict}

    except Exception as e:
        print(f"❌ Exception capturée : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du chargement des données : {e}")
    




# Configuration du logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

@app.get("/forecast")
def forecast_data():
    """
    Route pour effectuer des prévisions et les enregistrer dans PostgreSQL.
    """
    global data_cache  # Supposons que les données sont déjà chargées

    if data_cache is None:
        raise HTTPException(status_code=400, detail="Les données doivent d'abord être chargées via /load-data.")

    # Nettoyage des colonnes
    data_cache.columns = data_cache.columns.str.strip()
    logging.debug(f"Noms des colonnes après nettoyage : {list(data_cache.columns)}")

    # Trouver la colonne 'energyConsumption'
    column_name = next((col for col in data_cache.columns if col.lower() == "energyconsumption"), None)

    if column_name is None:
        logging.error("Erreur : la colonne 'energyConsumption' est absente des données.")
        raise HTTPException(status_code=400, detail="Les données doivent contenir la colonne 'energyConsumption'.")

    if data_cache[column_name].isnull().any():
        logging.error("Erreur : La colonne 'energyConsumption' contient des valeurs manquantes.")
        raise HTTPException(status_code=400, detail="La colonne 'energyConsumption' contient des valeurs manquantes.")

    try:
        # Appliquer ARIMA pour générer des prévisions
        forecast_df = apply_arima_model(data_cache.rename(columns={column_name: "energyConsumption"}), steps=120)
        logging.info(f"Nombre de prédictions générées : {len(forecast_df)}")

        # Renommer les colonnes pour correspondre à la base de données
        forecast_df = forecast_df.rename(columns={"date": "Timestamp", "predicted": "Forecast"})

        # Sauvegarder dans PostgreSQL
        save_predictions_to_postgres(forecast_df)

        return {"message": "Prévisions générées et enregistrées avec succès.", "forecast": forecast_df.tail(10).to_dict(orient="records")}

    except Exception as e:
        logging.exception("Erreur lors de la génération des prévisions")
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


#Vlaue in  Postgres 
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




@app.get("/sync-postgres-to-influx")
def sync_postgres_to_influx():
    """
    Endpoint pour synchroniser les données de PostgreSQL vers InfluxDB.
    """
    try:
        # Charger les données depuis PostgreSQL
        df = load_data_from_postgres()

        if df.empty:
            raise HTTPException(status_code=404, detail="Aucune donnée trouvée dans PostgreSQL.")

        # Ajouter les variables temporelles
        df = add_time_features(df)

        # Afficher le nombre total de lignes après l'ajout des variables temporelles
        logging.debug(f"Nombre total de lignes après ajout des variables temporelles : {len(df)}")

        # Sauvegarder les données dans InfluxDB
        save_to_influx(df)

        # Retourner un aperçu des nouvelles données
        return {
            "message": "Données synchronisées avec succès.",
            "preview": df.head().to_dict(orient="records"),
            "total_rows": len(df)  # Ajouter le nombre total de lignes traitées
        }

    except Exception as e:
        logging.error(f"Erreur lors de la synchronisation : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur lors de la synchronisation : {e}")


@app.get("/get-influx-data")
def get_influx_data():
    """
    Endpoint pour récupérer les données enregistrées dans InfluxDB et compter le nombre de lignes.
    """
    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()

        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: 0)
          |> filter(fn: (r) => r._measurement == "energy_data")
        '''
        tables = query_api.query(query, org=INFLUX_ORG)

        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    "timestamp": record.get_time(),
                    "forecast": record.get_value(),
                    "field": record.get_field()
                })

        client.close()

        nombre_de_lignes = len(results)  # ✅ Nombre total de lignes récupérées

        if not results:
            return {"message": "Aucune donnée trouvée dans InfluxDB.", "nombre_de_lignes": 0}

        return {"nombre_de_lignes": nombre_de_lignes, "data": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération des données InfluxDB : {e}")



