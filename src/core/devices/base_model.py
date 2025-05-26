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