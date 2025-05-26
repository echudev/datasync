"""
Device Domain Models

This module defines the core device-related domain models and interfaces.
"""
from typing import TypedDict, Optional

class WeatherDeviceDTO(TypedDict):
    """Type definition for weather station sensor data sent to the API."""
    timestamp: str
    TEMP: Optional[float]
    HR: Optional[float]
    PA: Optional[float]
    VV: Optional[float]
    DV: Optional[float]
    LLUVIA: Optional[float]
    UV: Optional[float]
    RS: Optional[float]