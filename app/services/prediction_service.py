from fastapi import FastAPI, HTTPException
import pandas as pd
from influxdb_client import InfluxDBClient
from statsmodels.tsa.arima.model import ARIMA
import psycopg2
import logging
from app.config.config import INFLUX_URL, INFLUX_ORG, INFLUX_TOKEN, PG_DBNAME, PG_USER, PG_PASSWORD, PG_HOST, PG_PORT
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.statespace.sarimax import SARIMAX
import numpy as np

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
    





def check_stationarity(series):
    """
    Vérifie si la série temporelle est stationnaire en utilisant le test de Dickey-Fuller.
    """
    result = adfuller(series)
    p_value = result[1]
    # Retourne True si la p-value est inférieure à 0.05, ce qui indique une série stationnaire
    return p_value < 0.05





def apply_arima_model(data, steps=1000):
    """
    Applique un modèle SARIMAX pour les prévisions en respectant les dates du dataset.
    Gère correctement les timestamps et les variables exogènes (température et humidité).
    """
    try:
        # Nettoyage des colonnes (enlever les espaces et rendre tout en minuscules)
        data.columns = data.columns.str.strip().str.lower()

        # Vérifier si 'energyconsumption' existe et le renommer
        if 'energyconsumption' in data.columns:
            data.rename(columns={'energyconsumption': 'energyproduced'}, inplace=True)

        # Vérifier que 'timestamp' est bien au format datetime
        data['timestamp'] = pd.to_datetime(data['timestamp'], errors='coerce')

        # Vérifier la dernière date du dataset
        last_timestamp = data['timestamp'].max()
        if pd.isna(last_timestamp):
            logging.error("Erreur : Aucune date valide détectée.")
            raise HTTPException(status_code=400, detail="Aucune date valide détectée.")

        logging.debug(f"Dernière date détectée : {last_timestamp}")

        # Générer un index de prévision basé sur la dernière date connue
        future_index = pd.date_range(start=last_timestamp + pd.Timedelta(days=1), periods=steps, freq='D')

        # Vérifier que les colonnes nécessaires sont présentes
        required_columns = ['timestamp', 'temperature', 'humidity', 'energyproduced']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            logging.error(f"Colonnes manquantes : {missing_columns}")
            raise HTTPException(status_code=400, detail=f"Colonnes manquantes : {missing_columns}")

        # Supprimer les lignes contenant des valeurs manquantes
        data.dropna(subset=required_columns, inplace=True)

        # Définir la variable cible et les variables exogènes
        y = data['energyproduced']
        exog = data[['temperature', 'humidity']]

        # Modèle SARIMAX avec exogènes
        model = SARIMAX(y, exog=exog, order=(1, 1, 1), seasonal_order=(1, 1, 1, 12))
        model_fit = model.fit(maxiter=1000, method='powell')

        # Générer des valeurs exogènes futures (si elles ne sont pas prédites)
        future_temperature = np.random.uniform(data['temperature'].min(), data['temperature'].max(), steps)
        future_humidity = np.random.uniform(data['humidity'].min(), data['humidity'].max(), steps)

        # Créer un DataFrame pour les variables exogènes futures
        future_exog = pd.DataFrame({'temperature': future_temperature, 'humidity': future_humidity})

        # Faire les prévisions avec les exogènes futures
        forecast = model_fit.forecast(steps=steps, exog=future_exog)

        # Construire le DataFrame final avec les prévisions et exogènes
        forecast_df = pd.DataFrame({
            "Timestamp": future_index,
            "Temperature": future_temperature,
            "Humidity": future_humidity,
            "EnergyProduced": forecast.values
        })

        # Formatage de la réponse JSON
        forecast_json = {
            "nombre_de_lignes": len(forecast_df),
            "data": forecast_df.to_dict(orient="records")
        }

        # Vérification avant d'envoyer à PostgreSQL
        if "data" in forecast_json and isinstance(forecast_json["data"], list):
            save_predictions_to_postgres(forecast_json)
        else:
            logging.error("Format invalide de forecast_json avant sauvegarde")
            raise HTTPException(status_code=500, detail="Format invalide de forecast_json avant sauvegarde")

        return forecast_json

    except Exception as e:
        logging.error(f"Erreur lors de l'application du modèle ARIMA : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur ARIMA : {e}")



def save_predictions_to_postgres(forecast_json):
    """
    Sauvegarde des prévisions dans PostgreSQL en utilisant connect_postgresql().
    """
    try:
        # Vérification de la structure des données
        if not isinstance(forecast_json, dict) or "data" not in forecast_json:
            logging.error("Format invalide de forecast_json reçu dans save_predictions_to_postgres")
            raise HTTPException(status_code=500, detail="Format invalide de forecast_json")

        # Création du DataFrame à partir des prévisions
        data_cache = pd.DataFrame(forecast_json['data'])

        # Afficher les colonnes du DataFrame pour le débogage
        logging.debug(f"Colonnes présentes dans les données : {data_cache.columns.tolist()}")

        # Vérifier que toutes les colonnes nécessaires sont présentes
        expected_columns = ['Timestamp', 'Temperature', 'Humidity', 'EnergyProduced']
        missing_columns = [col for col in expected_columns if col not in data_cache.columns]

        if missing_columns:
            logging.error(f"Erreur : Colonnes manquantes {missing_columns}")
            raise HTTPException(status_code=400, detail=f"Colonnes manquantes : {missing_columns}")

        # Connexion à PostgreSQL
        conn = connect_postgresql()
        cursor = conn.cursor()

        # Préparation de la requête d'insertion
        insert_query = """
        INSERT INTO predictions (timestamp, temperature, humidity, energyproduced) 
        VALUES (%s, %s, %s, %s);
        """

        # Convertir DataFrame en liste de tuples pour exécution rapide
        records_to_insert = list(data_cache.itertuples(index=False, name=None))

        # Insérer les données
        cursor.executemany(insert_query, records_to_insert)
        conn.commit()

        logging.info(f"Prévisions sauvegardées avec succès. Nombre de lignes : {len(records_to_insert)}")

        # Fermer la connexion
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement des prévisions : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
