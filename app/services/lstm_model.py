import tensorflow as tf

from keras.api.models import Sequential,Model
from keras.api.layers import Dense,LSTM,Input,RepeatVector,TimeDistributed
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from influxdb_client import InfluxDBClient
import logging
from fastapi import FastAPI
import logging
import pickle  # Pour sauvegarder et charger le modèle

app = FastAPI()

# Paramètres InfluxDB
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "6gVj-CNfMCVW0otLynr2-E4WTfI-ww6Z2QV0NSe-LrYfVHpFCnfGf-XUNtQ31_9CJna40ifv67fKRnKfoDnKAg=="
INFLUX_ORG = "iot_lab"
INFLUX_BUCKET = "energy_data"

# Paramètres LSTM
SEQ_LENGTH = 10  # Nombre de jours en entrée
PREDICTION_DAYS = 5  # Nombre de jours à prédire

# Charger les données depuis InfluxDB
def load_data_from_influx():
    """Charge les données depuis InfluxDB dans un DataFrame Pandas."""
    try:
        query = '''
        from(bucket: "energy_data")
        |> range(start: 0)  // Charger 6 mois de données
        |> filter(fn: (r) => r["_measurement"] == "energy_data")
        |> filter(fn: (r) => r["_field"] == "forecast")
        |> keep(columns: ["_time", "_value"])
        |> yield(name: "debug")
        '''
        
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()
        result = query_api.query(org=INFLUX_ORG, query=query)
        
        data = []
        for table in result:
            for record in table.records:
                data.append([record.get_time(), record.get_value()])
        
        client.close()
        
        if not data:
            return pd.DataFrame(columns=["timestamp", "forecast"])

        return pd.DataFrame(data, columns=["timestamp", "forecast"])
    
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des données depuis InfluxDB : {e}")
        return pd.DataFrame(columns=["timestamp", "forecast"])

# Préparer les données pour LSTM
def prepare_data_for_lstm(df):
    """Prépare les données pour l'entraînement du modèle LSTM Encoder-Decoder."""
    df = df[['forecast']]
    scaler = MinMaxScaler()
    df_scaled = scaler.fit_transform(df)

    X, y = [], []
    for i in range(len(df_scaled) - SEQ_LENGTH - PREDICTION_DAYS):
        X.append(df_scaled[i:i + SEQ_LENGTH])
        y.append(df_scaled[i + SEQ_LENGTH:i + SEQ_LENGTH + PREDICTION_DAYS])

    X, y = np.array(X), np.array(y)
    return X, y, scaler

# Définition du modèle LSTM Encoder-Decoder
def build_lstm_encoder_decoder():
    """Construit un modèle LSTM Encoder-Decoder."""
    input_layer = Input(shape=(SEQ_LENGTH, 1))
    encoder = LSTM(50, activation="relu", return_sequences=True)(input_layer)
    encoder = LSTM(50, activation="relu")(encoder)
    repeat = RepeatVector(PREDICTION_DAYS)(encoder)
    decoder = LSTM(50, activation="relu", return_sequences=True)(repeat)
    decoder = LSTM(50, activation="relu", return_sequences=True)(decoder)
    output_layer = TimeDistributed(Dense(1))(decoder)

    model = Model(input_layer, output_layer)
    model.compile(optimizer="adam", loss="mse")
    return model

# Entraîner et sauvegarder le modèle
def train_and_save_model():
    """Charge les données, entraîne le modèle et le sauvegarde."""
    df = load_data_from_influx()
    if df.empty:
        logging.error("Aucune donnée récupérée, impossible d'entraîner le modèle.")
        return None, None

    X, y, scaler = prepare_data_for_lstm(df)
    if len(X) == 0:
        logging.error("Pas assez de données pour entraîner le modèle.")
        return None, None

    model = build_lstm_encoder_decoder()
    model.fit(X, y, epochs=20, batch_size=16, verbose=1)

    # Sauvegarde du modèle et du scaler
    model.save("lstm_encoder_decoder.h5")
    with open("scaler.pkl", "wb") as f:
        pickle.dump(scaler, f)

    return model, scaler

# Charger un modèle pré-entraîné
def load_trained_model():
    """Charge le modèle et le scaler sauvegardés."""
    try:
        model = tf.keras.models.load_model("lstm_encoder_decoder.h5")
        with open("scaler.pkl", "rb") as f:
            scaler = pickle.load(f)
        return model, scaler
    except Exception as e:
        logging.error(f"Erreur lors du chargement du modèle : {e}")
        return None, None

# Entraîner une seule fois au démarrage
MODEL, SCALER = train_and_save_model() if not load_trained_model()[0] else load_trained_model()

@app.get("/predict")
def predict():
    """Fait une prédiction avec le modèle LSTM Encoder-Decoder."""
    if MODEL is None or SCALER is None:
        return {"message": "Modèle non disponible."}

    df = load_data_from_influx()
    if df.empty:
        return {"message": "Aucune donnée récupérée depuis InfluxDB."}

    X, _, _ = prepare_data_for_lstm(df)
    if len(X) == 0:
        return {"message": "Pas assez de données pour faire une prédiction."}

    last_sequence = X[-1].reshape(1, X.shape[1], X.shape[2])
    prediction = MODEL.predict(last_sequence)
    prediction = SCALER.inverse_transform(prediction.reshape(-1, 1))

    return {"predictions": prediction.flatten().tolist()}
