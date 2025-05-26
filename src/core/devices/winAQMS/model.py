"""
Device Domain Models

This module defines the core device-related domain models and interfaces.
"""
from typing import TypedDict, Optional

class AirQualityDeviceDTO(TypedDict):
    """Type definition for air quality sensor data sent to the API."""
    timestamp: str
    CO: Optional[float]
    NO: Optional[float]
    NO2: Optional[float]
    NOx: Optional[float]
    O3: Optional[float]
    PM10: Optional[int] 
