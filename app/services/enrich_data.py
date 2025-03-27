import psycopg2
import pandas as pd
from app.config.config import PG_DBNAME, PG_USER, PG_PASSWORD, PG_HOST, PG_PORT
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from app.config.config import INFLUX_URL, INFLUX_ORG, INFLUX_TOKEN, INFLUX_BUCKET

import logging
from sqlalchemy import create_engine

# Configuration des logs
logging.basicConfig(level=logging.DEBUG)

def get_postgres_connection():
    """Établit une connexion à PostgreSQL via SQLAlchemy"""
    try:
        logging.debug(f"Essai de connexion à PostgreSQL sur {PG_HOST}:{PG_PORT} avec la base de données {PG_DBNAME}")
        engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}')
        logging.debug("Connexion PostgreSQL réussie.")
        return engine
    except Exception as e:
        logging.error(f"Erreur lors de la connexion à PostgreSQL : {e}")
        raise

def load_data_from_postgres():
    """Charge les données depuis PostgreSQL dans un DataFrame Pandas"""
    try:
        query = "SELECT timestamp, forecast FROM predictions ORDER BY timestamp ASC"
        logging.debug(f"Exécution de la requête PostgreSQL : {query}")
        conn = get_postgres_connection()
        df = pd.read_sql(query, conn)
        logging.debug(f"Nombre de lignes récupérées depuis PostgreSQL : {len(df)}")
        conn.dispose()  # Fermer la connexion après utilisation
        if df.empty:
            logging.warning("Aucune donnée trouvée dans la base de données PostgreSQL.")
        return df
    except Exception as e:
        logging.error(f"Erreur lors du chargement des données depuis PostgreSQL : {e}")
        raise

def add_time_features(df):
    """Ajoute des variables temporelles à un DataFrame"""
    try:
        logging.debug("Ajout des variables temporelles au DataFrame.")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['month'] = df['timestamp'].dt.month
        df['week_of_year'] = df['timestamp'].dt.isocalendar().week
        logging.debug(f"Variables temporelles ajoutées : {df[['timestamp', 'hour', 'day_of_week', 'month', 'week_of_year']].head()}")
        return df
    except Exception as e:
        logging.error(f"Erreur lors de l'ajout des variables temporelles : {e}")
        raise


def save_to_influx(df):
    """Sauvegarde un DataFrame Pandas dans InfluxDB avec gestion d'erreur et options de batch."""
    try:
        logging.debug("Connexion à InfluxDB...")
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        if client is None:
            logging.error("Impossible de se connecter à InfluxDB.")
            return

        write_api = client.write_api(write_options=WriteOptions(batch_size=1000, flush_interval=10_000))
        logging.debug("Connexion à InfluxDB réussie.")
        
        # Nettoyage des doublons
        df = df.drop_duplicates()

        logging.debug("Envoi des données à InfluxDB...")
        
        for _, row in df.iterrows():
            # Créer un point pour chaque ligne du DataFrame
            point = Point("energy_data") \
                .field("forecast", row["forecast"]) \
                .field("hour", row["hour"]) \
                .field("day_of_week", row["day_of_week"]) \
                .field("month", row["month"]) \
                .field("week_of_year", row["week_of_year"]) \
                .time(row["timestamp"], WritePrecision.NS)  # Vérifier le format du timestamp

            try:
                # Essayer d'écrire dans InfluxDB
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            except Exception as e:
                logging.error(f"Erreur lors de l'écriture dans InfluxDB pour le timestamp {row['timestamp']}: {e}")
                continue  # Ignorer cette ligne et passer à la suivante

        logging.debug("Toutes les données ont été envoyées et enregistrées dans InfluxDB.")
        client.close()  # Fermer la connexion à InfluxDB
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde dans InfluxDB : {e}")
        raise  # Relancer l'erreur pour propager la gestion des erreurs
