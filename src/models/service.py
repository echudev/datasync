"""
Service State Model

This module defines the common state enumeration for all services in the system.
"""

from enum import Enum


class ServiceState(Enum):
    """Common state enumeration for all services (collectors, publishers, etc.)."""
    RUNNING = 1
    STOPPING = 2
    STOPPED = 3 