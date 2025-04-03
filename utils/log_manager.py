"""
Log Manager Module

This module provides a class to manage application logs.
"""

import logging
from pathlib import Path
from typing import Tuple

class LogManager:
    """Manages application logs operations."""

    def __init__(self, log_dir: str = "logs", log_file: str = "data_collection.log"):
        self.log_dir = Path(log_dir)
        self.log_file = self.log_dir / log_file
        self.logger = logging.getLogger("data_collection")
        self._ensure_log_directory()

    def _ensure_log_directory(self) -> None:
        """Ensure log directory exists."""
        self.log_dir.mkdir(exist_ok=True)

    def initialize_logging(self) -> None:
        """Initialize logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(str(self.log_file)),
            ],
        )

    def read_logs(self) -> str:
        """Read the contents of the log file."""
        try:
            if self.log_file.exists():
                return self.log_file.read_text(encoding='utf-8')
            return "No logs found."
        except Exception as e:
            self.logger.error(f"Error reading logs: {e}")
            return f"Error reading logs: {e}"

    def delete_logs(self) -> Tuple[bool, str]:
        """
        Delete the log file.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            if self.log_file.exists():
                self.log_file.unlink()
                self.logger.info("Log file deleted successfully")
                return True, "Logs deleted successfully"
            return False, "Log file does not exist"
        except Exception as e:
            self.logger.error(f"Error deleting logs: {e}")
            return False, f"Error deleting logs: {e}"

    def get_log_path(self) -> Path:
        """Get the path to the log file."""
        return self.log_file

    def clear_logs(self) -> Tuple[bool, str]:
        """
        Clear the contents of the log file without deleting it.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            self.log_file.write_text('')
            self.logger.info("Log file cleared successfully")
            return True, "Logs cleared successfully"
        except Exception as e:
            self.logger.error(f"Error clearing logs: {e}")
            return False, f"Error clearing logs: {e}"
