from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA

from sklearn.linear_model import LinearRegression
#from statsmodels.tsa.arima.model import ARIMA


def apply_arima_model(data, steps=30):
    """Applique ARIMA sur les données de consommation d'énergie et génère des prévisions pour un nombre de jours spécifié."""
    
    # Vérifier la présence de la colonne 'EnergyConsumption'
    if 'EnergyConsumption' not in data.columns:
        raise ValueError("Les données doivent contenir la colonne 'EnergyConsumption'.")
    
    # Vérifier que les données ne sont pas vides
    if data.empty:
        raise ValueError("Les données ne peuvent pas être vides.")
    
    # Convertir 'Timestamp' en datetime si ce n'est pas déjà fait
    if not pd.api.types.is_datetime64_any_dtype(data['Timestamp']):
        try:
            data['Timestamp'] = pd.to_datetime(data['Timestamp'])
        except Exception as e:
            raise ValueError(f"Erreur de conversion de la colonne 'Timestamp' : {e}")
    
    # Vérifier la présence de valeurs manquantes dans 'EnergyConsumption'
    if data['EnergyConsumption'].isnull().any():
        raise ValueError("La colonne 'EnergyConsumption' contient des valeurs manquantes.")

    # ARIMA nécessite une série temporelle unidimensionnelle, donc nous choisissons seulement la colonne 'EnergyConsumption'
    series = data.set_index('Timestamp')['EnergyConsumption']
    
    # Appliquer ARIMA pour prédire les prochaines valeurs
    model = ARIMA(series, order=(10 , 1, 0))  # (p, d, q) : vous pouvez ajuster ces paramètres
    model_fit = model.fit()

    # Générer les prévisions pour les prochaines 'steps' jours
    forecast = model_fit.forecast(steps=steps)  

    # Créer un DataFrame avec les prévisions pour un affichage plus clair
    forecast_dates = pd.date_range(start=data['Timestamp'].iloc[-1], periods=steps + 1, freq='D')[1:]
    forecast_df = pd.DataFrame({'Timestamp': forecast_dates, 'Forecast': forecast})

    return forecast_df
