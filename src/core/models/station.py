"""
Station Domain Model

This module defines the core station-related domain models and interfaces.
"""


from typing import TypedDict

class StationConfig(TypedDict):
    name: str
    location: str
    latitude: float
    longitude: float
    elevation: float