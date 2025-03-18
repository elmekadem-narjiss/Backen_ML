from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


def predict_energy_consumption(data, days=30):
    """ Prédit la consommation d'énergie pour les prochains jours """
    if data is None or 'EnergyConsumption' not in data.columns:
        raise ValueError("Les données doivent contenir la colonne 'EnergyConsumption'.")

    # Utiliser les colonnes disponibles comme features
    X = data.drop(columns=['EnergyConsumption'])
    y = data['EnergyConsumption']

    model = LinearRegression()
    model.fit(X, y)

    # Simuler de nouvelles données pour les prochains jours
    future_data = np.mean(X, axis=0)  # Moyenne des features existantes
    future_data = np.tile(future_data, (days, 1))  # Étendre à plusieurs jours

    predictions = model.predict(future_data)  # Faire des prédictions

    if predictions is None:
        raise ValueError("La prédiction a échoué, les données sont peut-être incorrectes.")
    
    return predictions
