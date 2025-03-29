from fastapi import FastAPI, HTTPException
import pandas as pd
from influxdb_client import InfluxDBClient
from statsmodels.tsa.arima.model import ARIMA
import psycopg2
import logging
from app.config.config import INFLUX_URL, INFLUX_ORG, INFLUX_TOKEN, PG_DBNAME, PG_USER, PG_PASSWORD, PG_HOST, PG_PORT
from statsmodels.tsa.stattools import adfuller

# Configuration du logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()

def connect_postgresql():
    """Connexion à PostgreSQL"""
    try:
        logging.debug("Tentative de connexion à PostgreSQL...")
        conn = psycopg2.connect(
            dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
        )
        logging.info("Connexion à PostgreSQL réussie.")
        return conn
    except Exception as e:
        logging.error(f"Erreur PostgreSQL : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")

def get_influx_data():
    """Récupérer les données depuis InfluxDB"""
    try:
        logging.debug("Connexion à InfluxDB...")
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query = '''
        from(bucket: "energy_data")
          |> range(start: 0)
          |> filter(fn: (r) => r._measurement == "environment_data")
          |> filter(fn: (r) => r._field == "energyConsumption")
        '''
        logging.debug("Exécution de la requête InfluxDB...")
        result = client.query_api().query(query)

        data = []
        for table in result:
            for record in table.records:
                data.append({"Timestamp": record.get_time(), "energyConsumption": record.get_value()})

        if not data:
            logging.warning("Aucune donnée récupérée depuis InfluxDB.")

        df = pd.DataFrame(data)
        logging.info(f"Données InfluxDB récupérées : {df.shape[0]} lignes.")
        return df
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des données InfluxDB : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur InfluxDB : {e}")
    


def apply_arima_model(data, steps=1000):
    """Applique ARIMA et génère des prévisions"""
    try:
        logging.debug(f"Nombre de jours de prévision demandés : {steps}")

        if 'energyConsumption' not in data.columns:
            raise ValueError("Les données doivent contenir la colonne 'energyConsumption'.")

        if data.empty:
            raise ValueError("Les données ne peuvent pas être vides.")

        data['Timestamp'] = pd.to_datetime(data['Timestamp'])
        data = data.sort_values(by='Timestamp')
        data = data.set_index('Timestamp').asfreq('D')  # Assurer une fréquence correcte

        logging.info(f"Nombre de points de données pour ARIMA : {len(data)}")

        series = data['energyConsumption']

        model = ARIMA(series, order=(10, 1, 0))
        model_fit = model.fit()

        logging.debug(f"Génération des prévisions pour {steps} jours...")
        forecast = model_fit.forecast(steps=steps)

        logging.info(f"Nombre de valeurs prédites : {len(forecast)}")  # Vérification

        forecast_dates = pd.date_range(start=data.index[-1], periods=steps + 1, freq='D')[1:]

        logging.debug(f"Dates de prévision générées : {forecast_dates}")  # Vérification

        forecast_df = pd.DataFrame({'Timestamp': forecast_dates, 'Forecast': forecast.values})

        logging.info(f"Prévisions générées avec succès pour {steps} jours.")
        return forecast_df
    except Exception as e:
        logging.error(f"Erreur lors de l'application du modèle ARIMA : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur ARIMA : {e}")



def save_predictions_to_postgres(predictions_df):
    """Enregistre les prévisions dans PostgreSQL"""
    try:
        conn = connect_postgresql()
        cursor = conn.cursor()
        logging.debug("Insertion des prévisions dans PostgreSQL...")

        for _, row in predictions_df.iterrows():
            cursor.execute("INSERT INTO predictions (timestamp, forecast) VALUES (%s, %s)", (row['Timestamp'], row['Forecast']))

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Les prévisions ont été enregistrées dans PostgreSQL avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement des prévisions : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
