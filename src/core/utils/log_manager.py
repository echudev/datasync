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
from core.utils.path_dir import LOG_DIR


class LogManager:
    """
    Modern LogManager: acepta level como string ("INFO", "DEBUG", etc.) o int (logging.INFO, logging.DEBUG, ...).
    Ejemplo de uso:
        LogManager(level="DEBUG")
        LogManager(level=logging.INFO)
    """

    def __init__(self, log_dir: str = None, log_file: str = "datasync.log", level="INFO"):
        self.log_dir = Path(log_dir) if log_dir else LOG_DIR
        self.log_file = self.log_dir / log_file
        self.level = self._parse_level(level)
        self._ensure_log_directory()

        self.logger = logging.getLogger(log_file)
        self.logger.setLevel(self.level)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(str(self.log_file))
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        self.logger.propagate = False

    def _parse_level(self, level):
        if isinstance(level, int):
            return level
        if isinstance(level, str):
            return getattr(logging, level.upper(), logging.INFO)
        return logging.INFO

    def _ensure_log_directory(self) -> None:
        """Ensure log directory exists."""
        self.log_dir.mkdir(exist_ok=True)