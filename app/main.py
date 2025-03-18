from fastapi import FastAPI, HTTPException
from pathlib import Path
from app.utils.time_series import load_energy_consumption_data
from app.services.prediction_service import predict_energy_consumption

app = FastAPI()

# Définir le chemin du fichier de données
DATASET_DIR = Path("D:/PFE/DataSet")
FILE_CSV = DATASET_DIR / "Energy_consumption.csv"
data_cache = None  # Stocke les données après /load-data

@app.get("/")
def root():
    return {"message": "Bienvenue dans le service de prédiction"}

@app.get("/load-data")
def load_data():
    """
    Route pour charger et afficher les données.
    """
    global data_cache  # Permet de modifier la variable globale

    try:
        # Vérifier si le fichier existe
        if not FILE_CSV.exists():
            raise HTTPException(status_code=404, detail="Fichier introuvable. Vérifiez le chemin.")

        # Charger les données
        df = load_energy_consumption_data(str(FILE_CSV))
        data_cache = df  # Stocker les données chargées

        return {"data": df.head().to_dict()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors du chargement des données : {e}")

@app.get("/predict")
def get_prediction(days: int = 10):
    """ Fait des prédictions sur la consommation d'énergie pour les prochains jours. """
    global data_cache  # Déclare `data_cache` comme global pour l'utiliser

    try:
        if data_cache is None:
            raise HTTPException(status_code=400, detail="Veuillez d'abord charger les données via /load-data.")

        if days < 1 or days > 365:
            raise HTTPException(status_code=400, detail="Le nombre de jours doit être compris entre 1 et 365.")

        prediction = predict_energy_consumption(data_cache, days)

        if prediction is None:
            raise HTTPException(status_code=500, detail="La prédiction a échoué, veuillez vérifier les données.")

        return {"predictions": prediction.tolist()}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la prédiction : {e}")
