from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import numpy as np
from app.database.db_connection import connect_influxdb, connect_mqtt
from influxdb_client import Point , WriteOptions
from app.config.config import INFLUX_BUCKET, INFLUX_ORG,MQTT_BROKER,MQTT_TOPIC
from influxdb_client.client.write_api import SYNCHRONOUS
import paho.mqtt.client as mqtt


app = FastAPI()

# Modèle Pydantic pour la validation des données envoyées à l'API
class EnergyDataInput(BaseModel):
    data: dict

# Fonction de traitement des messages MQTT
def on_message(client, userdata, message):
    """Traitement des messages reçus sur le topic MQTT."""
    try:
        # Décoder le message reçu
        data = message.payload.decode()

        # Exemple de traitement des données - Ajustez selon votre format de message
        # Supposons que le message soit un JSON avec des données de consommation d'énergie
        energy_data = data.split(",")  # Cela dépend du format de vos données MQTT
        timestamp = energy_data[0]
        energy_consumption = float(energy_data[1])

        # Connexion à InfluxDB et ajout des données
        influx_client = connect_influxdb()
        if influx_client:
            write_api = influx_client.write_api()
            point = Point("energy_consumption") \
                .tag("location", "building_1") \
                .field("energy_consumption", energy_consumption) \
                .time(timestamp)
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(f"✅ Données envoyées dans InfluxDB : {timestamp}, {energy_consumption}")
    except Exception as e:
        print(f"❌ Erreur lors du traitement du message MQTT : {e}")

# Fonction de connexion MQTT
def connect_mqtt():
    """Se connecter au broker MQTT et s'abonner à un topic."""
    client = mqtt.Client()

    # Définir la fonction de rappel pour les messages reçus
    client.on_message = on_message  # Assigner la fonction on_message à client.on_message

    try:
        # Connexion au broker MQTT
        client.connect(MQTT_BROKER, 1883, 60)
        print(f"✅ Connexion réussie au broker MQTT : {MQTT_BROKER}")

        # S'abonner au topic MQTT
        client.subscribe(MQTT_TOPIC)
        print(f"✅ Abonné au topic : {MQTT_TOPIC}")

        # Démarrer la boucle MQTT pour recevoir les messages
        client.loop_start()
    except Exception as e:
        print(f"❌ Erreur lors de la connexion à MQTT : {e}")



# Fonction pour sauvegarder les données dans InfluxDB
# Fonction pour sauvegarder les données dans InfluxDB
import time
from influxdb_client.client.write_api import WritePrecision

def save_data_to_influxdb(df):
    influx_client = connect_influxdb()
    if influx_client is None:
        print("❌ Impossible de se connecter à InfluxDB")
        return

    write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1000, flush_interval=10_000))

    print("📤 Envoi des données à InfluxDB...")

    # Nettoyage des doublons avant d'envoyer les données
    df = df.drop_duplicates()

    # Envoi des données
    for _, row in df.iterrows():
        # Envoi des données sans afficher dans le terminal
        point = Point("environment_data") \
            .tag("location", "office") \
            .field("temperature", row["Temperature"]) \
            .field("humidity", row["Humidity"]) \
            .field("squareFootage", row["SquareFootage"]) \
            .field("occupancy", row["Occupancy"]) \
            .field("renewableEnergy", row["RenewableEnergy"]) \
            .field("energyConsumption", row["EnergyConsumption"]) \
            .time(row["Timestamp"], WritePrecision.NS)  # Vérifie le timestamp

        try:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        except Exception as e:
            print(f"❌ Erreur lors de l'écriture dans InfluxDB: {e}")
            continue

    print("✅ Toutes les données ont été envoyées et enregistrées dans InfluxDB.")

    # ✅ Pause pour s'assurer que les données sont bien insérées avant la vérification
    time.sleep(5)
   # verify_influxdb_data(influx_client)


# Fonction pour vérifier les données enregistrées dans InfluxDB



# Fonction pour charger et nettoyer les données du fichier CSV
def load_energy_consumption_data(file_path: str):
    """Charge et nettoie les données à partir d'un fichier CSV."""
    if not file_path or not file_path.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Erreur : Chemin de fichier invalide ou format incorrect.")

    try:
        df = pd.read_csv(file_path, sep=",")
        
        # Vérification et conversion du timestamp
        if 'Timestamp' in df.columns:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            if df["Timestamp"].isnull().all():
                raise HTTPException(status_code=400, detail="Erreur : La colonne 'Timestamp' contient des valeurs invalides.")
        else:
            raise HTTPException(status_code=400, detail="Erreur : La colonne 'Timestamp' est absente du fichier.")
        
        # Extraction des informations temporelles
        df['Year'] = df['Timestamp'].dt.year
        df['Month'] = df['Timestamp'].dt.month
        df['Day'] = df['Timestamp'].dt.day
        df['Hour'] = df['Timestamp'].dt.hour
        
        # Suppression des lignes contenant des NaN
        df.dropna(axis=0, how='any', inplace=True)

        # Suppression des colonnes non numériques
        df = df.select_dtypes(include=[np.number])

        # Affichage du nombre de lignes après nettoyage
        nombre_de_lignes = len(df)
        print(f"Nombre de lignes après nettoyage : {nombre_de_lignes}")

        return df, nombre_de_lignes
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors du chargement des données : {e}")

# Exemple d'appel pour charger les données depuis le fichier CSV
# Exemple d'appel pour tester manuellement le chargement du fichier CSV
if __name__ == "__main__":
    file_path = "D:/PFE/DataSet/Energy_consumption.csv"
    df, n = load_energy_consumption_data(file_path)
    print(df.head())


# Appel de la fonction pour démarrer la connexion MQTT au démarrage de l'application
@app.on_event("startup")
async def startup_event():
    connect_mqtt()
