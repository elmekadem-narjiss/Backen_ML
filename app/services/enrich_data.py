import pandas as pd
import logging
from sqlalchemy import create_engine
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from app.config.config import (
    PG_DBNAME, PG_USER, PG_PASSWORD, PG_HOST, PG_PORT,
    INFLUX_URL, INFLUX_ORG, INFLUX_TOKEN, INFLUX_BUCKET
)

# Configuration des logs
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

def get_postgres_connection():
    try:
        logging.debug(f"Connexion PostgreSQL sur {PG_HOST}:{PG_PORT} - DB: {PG_DBNAME}")
        engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}')
        return engine
    except Exception as e:
        logging.error(f"Erreur de connexion PostgreSQL : {e}")
        raise

def load_data_from_postgres():
    try:
        query = "SELECT timestamp, temperature, humidity, energyproduced FROM predictions ORDER BY timestamp ASC"
        logging.debug(f"Requête SQL : {query}")
        conn = get_postgres_connection()
        df = pd.read_sql(query, conn)
        conn.dispose()
        logging.info(f"{len(df)} lignes chargées depuis PostgreSQL.")

        # Vérification des doublons et suppression
        if df.duplicated(subset=['timestamp']).any():
            logging.warning("Des doublons ont été détectés dans les données et seront supprimés.")
            df = df.drop_duplicates(subset=['timestamp'])
        
        return df
    except Exception as e:
        logging.error(f"Erreur lors du chargement des données : {e}")
        raise



def add_time_features(df):
    """Ajoute des variables temporelles au DataFrame uniquement pour les enregistrements à 15:00:00 UTC"""
    try:
        logging.debug("Ajout des variables temporelles au DataFrame.")
        
        # Conversion de timestamp en datetime, gestion des valeurs nulles
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')  # 'coerce' convertit les valeurs invalides en NaT
        if df['timestamp'].isnull().any():
            logging.warning("Des valeurs de 'timestamp' sont nulles après conversion, elles seront supprimées.")
            df = df.dropna(subset=['timestamp'])  # Supprimer les lignes avec des timestamp manquants
        
        # Filtrer les enregistrements pour n'inclure que ceux ayant 15:00:00 UTC
        df = df[df['timestamp'].dt.hour == 15]
        df = df[df['timestamp'].dt.minute == 0]
        df = df[df['timestamp'].dt.second == 0]
        
        # Ajout des variables temporelles
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['month'] = df['timestamp'].dt.month
        df['week_of_year'] = df['timestamp'].dt.isocalendar().week
        
        logging.debug(f"Variables temporelles ajoutées : {df.head()}")
        return df
    except Exception as e:
        logging.error(f"Erreur lors de l'ajout des variables temporelles : {e}")
        raise

def clear_old_data():
    try:
        logging.debug("Suppression des anciennes données d'InfluxDB...")
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        delete_api = client.delete_api()

        from datetime import datetime, timedelta
        start = datetime.utcnow() - timedelta(days=30)
        stop = datetime.utcnow()

        delete_api.delete(
            start=start,
            stop=stop,
            predicate='_measurement="energy_data"',
            bucket=INFLUX_BUCKET,
            org=INFLUX_ORG
        )
        logging.info("Anciennes données supprimées d'InfluxDB.")
        client.close()
    except Exception as e:
        logging.error(f"Erreur suppression InfluxDB : {e}")
        raise

def save_to_influx(df):
    try:
        logging.debug("Connexion à InfluxDB pour sauvegarde...")
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        # Suppression des doublons (optionnel : selon timestamp ou tout le DataFrame)
        df = df.drop_duplicates()

        points = []
        for _, row in df.iterrows():
            point = Point("energy_data") \
                .field("temperature", float(row["temperature"])) \
                .field("humidity", float(row["humidity"])) \
                .field("energyproduced", float(row["energyproduced"])) \
                .field("hour", int(row["hour"])) \
                .field("day_of_week", int(row["day_of_week"])) \
                .field("month", int(row["month"])) \
                .field("week_of_year", int(row["week_of_year"])) \
                .time(row["timestamp"], WritePrecision.NS)
            points.append(point)

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        logging.info("Données sauvegardées avec succès dans InfluxDB.")
        client.close()
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde dans InfluxDB : {e}")
        raise

def query_influx():
    try:
        logging.debug("Récupération des données enrichies depuis InfluxDB...")
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()

        # Nouvelle requête correcte
        query = '''
        import "date"
        from(bucket: "energy_data") 
        |> range(start: -0)
        |> filter(fn: (r) => r._measurement == "energy_data")
        |> filter(fn: (r) => date.hour(t: r._time) == 15 and date.minute(t: r._time) == 0 and date.second(t: r._time) == 0)  
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"], desc: true)
        '''


        # Exécuter la requête
        tables = query_api.query(query)
        results = []

        # Parcourir les résultats et les ajouter à la liste 'results'
        for table in tables:
            for record in table.records:
                try:
                    results.append({
                        "timestamp": record.get_time(),
                        "temperature": record.values.get("temperature"),
                        "humidity": record.values.get("humidity"),
                        "energyproduced": record.values.get("energyproduced"),
                        "hour": record.values.get("hour"),
                        "day_of_week": record.values.get("day_of_week"),
                        "month": record.values.get("month"),
                        "week_of_year": record.values.get("week_of_year")
                    })
                except KeyError as e:
                    logging.error(f"Clé manquante dans l'enregistrement : {e}")

        logging.debug(f"{len(results)} enregistrements récupérés depuis InfluxDB.")
        client.close()
        return results

    except Exception as e:
        logging.error(f"Erreur lors de la requête InfluxDB : {e}")
        return []


if __name__ == "__main__":
    try:
        logging.info("Début du processus ETL...")
        df = load_data_from_postgres()

        if not df.empty:
            df = add_time_features(df)
            clear_old_data()
            save_to_influx(df)
        else:
            logging.warning("Aucune donnée à traiter.")

        logging.info("Processus ETL terminé avec succès.")
    except Exception as e:
        logging.error(f"Erreur critique dans le script ETL : {e}")
