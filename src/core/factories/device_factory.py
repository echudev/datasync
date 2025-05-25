"""
Device Factory Module

This module implements the Factory pattern for creating device instances.
"""

from typing import Dict, Type
from core.models import Device
from core.drivers.davis_vantage_pro2 import DavisVantagePro2

class DeviceFactory:
    """Factory class for creating device instances."""
    
    _instance = None
    _devices: Dict[str, Type[Device]] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DeviceFactory, cls).__new__(cls)
            cls._instance._initialize_devices()
        return cls._instance
    
    def _initialize_devices(self):
        """Initialize the device registry with available device types."""
        self._devices = {
            "davisvp2": DavisVantagePro2
            #  Agregar más dispositivos acá, por ejemplo:
            # "thermo48i": Thermo48i,
            # "Thermo42i": Thermo42i,
            # "Serinus30": Serinus30,
            # "Serinus10": Serinus10,
            # "BAM1020": BAM1020,
        }
    
    def create_device(self, device_type: str, **kwargs) -> Device:
        """Create a new device instance of the specified type."""
        if device_type not in self._devices:
            raise ValueError(f"Unknown device type: {device_type}")
        
        device_class = self._devices[device_type]
        return device_class(**kwargs)
    
    @classmethod
    def get_instance(cls) -> 'DeviceFactory':
        """Get the singleton instance of the factory."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance 