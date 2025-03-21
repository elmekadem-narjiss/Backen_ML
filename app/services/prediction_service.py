from fastapi import FastAPI, HTTPException
import pandas as pd
from influxdb_client import InfluxDBClient
from statsmodels.tsa.arima.model import ARIMA
import psycopg2
import os
from pydantic import BaseModel
from app.config.config import INFLUX_URL, INFLUX_ORG,INFLUX_TOKEN,PG_DBNAME,PG_USER,PG_PASSWORD,PG_HOST,PG_PORT


app = FastAPI()

def connect_postgresql():
    """Connexion à PostgreSQL"""
    try:
        conn = psycopg2.connect(
            dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")

def get_influx_data():
    """Récupérer les données depuis InfluxDB"""
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query = '''
    from(bucket: "energy_data")
      |> range(start: 0)
      |> filter(fn: (r) => r._measurement == "environment_data")
      |> filter(fn: (r) => r._field == "energyConsumption")
    '''
    result = client.query_api().query(query)
    
    data = []
    for table in result:
        for record in table.records:
            data.append({"Timestamp": record.get_time(), "energyConsumption": record.get_value()})
    
    return pd.DataFrame(data)

def apply_arima_model(data, steps=30):
    """Applique ARIMA et génère des prévisions"""
    if 'energyConsumption' not in data.columns:
        raise ValueError("Les données doivent contenir la colonne 'energyConsumption'.")

    if data.empty:
        raise ValueError("Les données ne peuvent pas être vides.")

    data['Timestamp'] = pd.to_datetime(data['Timestamp'])
    data = data.sort_values(by='Timestamp')

    series = data.set_index('Timestamp')['energyConsumption']
    
    model = ARIMA(series, order=(10, 1, 0))
    model_fit = model.fit()

    forecast = model_fit.forecast(steps=steps)
    forecast_dates = pd.date_range(start=data['Timestamp'].iloc[-1], periods=steps + 1, freq='D')[1:]
    forecast_df = pd.DataFrame({'Timestamp': forecast_dates, 'Forecast': forecast})

    return forecast_df

def save_predictions_to_postgres(predictions_df):
    """Enregistre les prévisions dans PostgreSQL"""
    conn = connect_postgresql()
    cursor = conn.cursor()

    for _, row in predictions_df.iterrows():
        cursor.execute("INSERT INTO predictions (timestamp, forecast) VALUES (%s, %s)", (row['Timestamp'], row['Forecast']))

    conn.commit()
    cursor.close()
    conn.close()