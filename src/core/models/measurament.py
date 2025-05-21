from abc import ABC, abstractmethod
from datetime import datetime

class Measurement(ABC):
    def __init__(self, timestamp: datetime, sensor_id: str):
        self.timestamp = timestamp
        self.sensor_id = sensor_id

    @abstractmethod
    def to_dict(self) -> dict:
        pass

class COMeasurement(Measurement):
    def __init__(self, timestamp, sensor_id, ppm):
        super().__init__(timestamp, sensor_id)
        self.ppm = ppm

    def to_dict(self):
        return {
            "timestamp": self.timestamp.isoformat(),
            "sensor_id": self.sensor_id,
            "ppm": self.ppm
        }

class WeatherMeasurement(Measurement):
    def __init__(self, timestamp, sensor_id, temperature, humidity, wind_speed):
        super().__init__(timestamp, sensor_id)
        self.temperature = temperature
        self.humidity = humidity
        self.wind_speed = wind_speed

    def to_dict(self):
        return {
            "timestamp": self.timestamp.isoformat(),
            "sensor_id": self.sensor_id,
            "temperature": self.temperature,
            "humidity": self.humidity,
            "wind_speed": self.wind_speed
        }
