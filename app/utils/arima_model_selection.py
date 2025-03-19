import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller

# Charger les données
your_time_series = pd.read_csv('D:/PFE/DataSet/Energy_consumption.csv', parse_dates=['Timestamp'], index_col='Timestamp')

# Stationnarisation des données si nécessaire
your_time_series = your_time_series.asfreq('H')

# Test de stationnarité
result = adfuller(your_time_series['EnergyConsumption'].dropna())
print(f"Statistique ADF : {result[0]}")
print(f"P-value : {result[1]}")

if result[1] > 0.05:
    print("Les données ne sont pas stationnaires. Application de la différence.")
    # Différenciation pour rendre la série stationnaire
    your_time_series['EnergyConsumption_diff'] = your_time_series['EnergyConsumption'].diff().dropna()
else:
    print("Les données sont déjà stationnaires.")

# Ajustement du modèle ARIMA
p_values = range(0, 3)  # Test de 0 à 2 pour p
d_values = range(0, 2)  # Test de 0 à 1 pour d
q_values = range(0, 3)  # Test de 0 à 2 pour q

# Liste pour enregistrer les résultats de chaque combinaison (p, d, q)
aic_values = []

# Test de différentes combinaisons (p, d, q)
for p in p_values:
    for d in d_values:
        for q in q_values:
            try:
                if 'EnergyConsumption_diff' in your_time_series.columns:
                    model = ARIMA(your_time_series['EnergyConsumption_diff'].dropna(), order=(p, d, q))
                else:
                    model = ARIMA(your_time_series['EnergyConsumption'].dropna(), order=(p, d, q))
                
                model_fit = model.fit()  # Pas besoin de maxiter ici
                aic_values.append((p, d, q, model_fit.aic))
            except Exception as e:
                print(f"Erreur avec (p, d, q) = ({p}, {d}, {q}): {e}")
                continue

# Vérification si des résultats sont disponibles
if aic_values:
    # Trier les résultats par AIC
    aic_df = pd.DataFrame(aic_values, columns=['p', 'd', 'q', 'AIC'])
    aic_df = aic_df.sort_values(by='AIC').reset_index(drop=True)

    # Afficher les meilleures combinaisons de (p, d, q) et leur AIC
    print("\nCombinations de (p, d, q) triées par AIC :")
    print(aic_df)

    # Récupérer la meilleure combinaison (p, d, q)
    best_order = tuple(aic_df.iloc[0][['p', 'd', 'q']])
    print(f"\nMeilleure combinaison (p, d, q) : {best_order}")
else:
    print("Aucune combinaison valide n'a été trouvée.")
