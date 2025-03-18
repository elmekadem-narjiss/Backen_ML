from pydantic import BaseModel

class SensorData(BaseModel):
    timestamp: str
    temperature: float
    humidity: float
    power_consumption: float
