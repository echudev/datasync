"""
Log Manager Module

This module provides a class to manage application logs.

Loggin Levels:

DEBUG (10): Used for debugging messages.
INFO (20): Logs events within expected program behavior.
WARNING (30): Logs unexpected events that may not be severe errors.
ERROR (40): Logs unexpected failures in the program.
CRITICAL (50): Logs critical errors that may cause program termination

The logger will only display logs from the selected level and above.
"""

import logging
from pathlib import Path
from typing import Tuple
from enum import Enum


class LoggerLevel(Enum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

class LogManager:
    """Manages application logs operations."""

    def __init__(self, log_dir: str = None, log_file: str = "datasync.log", level: LoggerLevel = LoggerLevel.INFO):
        self.log_dir = Path(log_dir or Path(__file__).parent.parent.parent / "logs")
        self.log_file = self.log_dir / log_file
        self.level = level
        self._ensure_log_directory()
        
        # Configurar logger directamente en el init
        self.logger = logging.getLogger(log_file)  # Usar el nombre del archivo como nombre del logger
        self.logger.setLevel(self.level.value)
        
        # Crear formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Agregar FileHandler
        file_handler = logging.FileHandler(str(self.log_file))
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Agregar StreamHandler para consola
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # Evitar la propagación al logger raíz
        self.logger.propagate = False

    def _ensure_log_directory(self) -> None:
        """Ensure log directory exists."""
        self.log_dir.mkdir(exist_ok=True)