from pydantic import BaseModel

class PredictionResult(BaseModel):
    date: str
    predicted_value: float
