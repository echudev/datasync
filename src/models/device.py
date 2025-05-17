"""
Device Domain Models

This module defines the core device-related domain models and interfaces.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, TypedDict, Optional

class Device(ABC):
    """Abstract base class defining the interface for all measurement devices."""

    @abstractmethod
    async def read(self) -> Dict[str, float]:
        """Read current device measurements."""
        pass

class DeviceConfig(TypedDict):
    """Configuration for a measurement device in the system."""
    name: str
    keys: List[str]
    scan_interval: float


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


class AirQualityDeviceDTO(TypedDict):
    """Type definition for air quality sensor data sent to the API."""
    timestamp: str
    CO: Optional[float]
    NO: Optional[float]
    NO2: Optional[float]
    NOx: Optional[float]
    O3: Optional[float]
    PM10: Optional[int] 

