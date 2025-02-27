"""
Data Collector Service

This module implements the data collection logic for environmental sensors
using asyncio for concurrency and pandas for data handling.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Any, TypedDict

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed


# Tipado más estricto para el buffer
class BufferEntry(TypedDict):
    data: Dict[str, float]
    count: int


# Estado del recolector
class CollectorState(Enum):
    RUNNING = 1
    STOPPING = 2
    STOPPED = 3


@dataclass
class SensorData:
    """Class for storing sensor data with timestamp."""

    timestamp: datetime
    values: Dict[str, float] = field(default_factory=dict)


class SensorConfig(TypedDict):
    """Type definition for sensor configuration."""

    name: str
    keys: List[str]
    scan_interval: float


class Sensor(ABC):
    """Abstract base class for sensor implementations."""

    @abstractmethod
    async def read(self) -> Dict[str, float]:
        pass


class DataCollector:
    """Handles collection and processing of sensor data."""

    def __init__(self, output_path: Path, logger: logging.Logger) -> None:
        """Initialize with dependencies."""
        self.output_path = output_path
        self.logger = logger
        self.data_buffer: Dict[str, BufferEntry] = defaultdict(
            lambda: {"data": {}, "count": 0}
        )
        self.data_lock = asyncio.Lock()
        self.state = CollectorState.RUNNING
        self.csv_columns: List[str] = ["timestamp"]
        self.data_to_save: List[Dict[str, Any]] = []

    async def __aenter__(self):
        """Async context manager entry."""
        self.logger.info("Initializing DataCollector")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Async context manager exit."""
        self.state = CollectorState.STOPPING
        await asyncio.sleep(1)  # Dar tiempo a las tareas para terminar
        self.state = CollectorState.STOPPED
        if self.data_to_save:  # Guardar datos pendientes
            await self._save_batch_data(self.data_to_save)
        self.logger.info("DataCollector shut down")

    async def collect_data(self, sensor: Sensor, sensor_config: SensorConfig) -> None:
        """Collect data from a sensor at regular intervals."""
        required = {"name", "keys", "scan_interval"}
        if not all(k in sensor_config for k in required):
            raise ValueError(f"Sensor config missing required fields: {required}")

        name = sensor_config["name"]
        scan_interval = sensor_config["scan_interval"]

        self.logger.info(f"Starting data collection for {name}")
        try:
            while self.state == CollectorState.RUNNING:
                start_time = datetime.now()
                timestamp_key = start_time.strftime("%Y-%m-%d %H:%M")

                sensor_data = await sensor.read()

                async with self.data_lock:
                    buffer_entry = self.data_buffer[timestamp_key]
                    for key, value in sensor_data.items():
                        buffer_entry["data"][key] = (
                            buffer_entry["data"].get(key, 0.0) + value
                        )
                    buffer_entry["count"] += 1

                elapsed = (datetime.now() - start_time).total_seconds()
                sleep_time = max(0.1, scan_interval - elapsed)
                await asyncio.sleep(sleep_time)
        except Exception as e:
            self.logger.error(f"Error in data collection for {name}: {e}")
            raise
        finally:
            self.logger.info(f"Stopped data collection for {name}")

    async def process_and_save_data(
        self, output_interval: float = 60.0, batch_size: int = 10
    ) -> None:
        """Process collected data and save in batches."""
        self.logger.info("Starting data processing task")
        try:
            while self.state == CollectorState.RUNNING:
                await asyncio.sleep(output_interval)

                now = datetime.now()
                process_time = now.replace(second=0, microsecond=0)
                process_time = process_time.replace(minute=process_time.minute - 1)
                timestamp_key = process_time.strftime("%Y-%m-%d %H:%M")

                async with self.data_lock:
                    if timestamp_key in self.data_buffer:
                        buffer_entry = self.data_buffer[timestamp_key]
                        averages = {
                            k: v / buffer_entry["count"]
                            for k, v in buffer_entry["data"].items()
                        }
                        self.data_to_save.append(
                            {"timestamp": timestamp_key, **averages}
                        )
                        del self.data_buffer[timestamp_key]

                if len(self.data_to_save) >= batch_size:
                    await self._save_batch_data(self.data_to_save)
                    self.data_to_save.clear()
        except Exception as e:
            self.logger.error(f"Error in data processing: {e}")
        finally:
            self.logger.info("Stopped data processing task")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def _save_batch_data(self, data: List[Dict[str, Any]]) -> None:
        """Save a batch of data to CSV with retries."""
        if not data:
            return

        df = pd.DataFrame(data)
        for col in self.csv_columns:
            if col not in df.columns:
                df[col] = None
        df = df[self.csv_columns]

        process_time = datetime.strptime(data[0]["timestamp"], "%Y-%m-%d %H:%M")
        year, month, day = (
            process_time.strftime("%Y"),
            process_time.strftime("%m"),
            process_time.strftime("%d"),
        )
        output_dir = self.output_path / year / month
        output_file = output_dir / f"{day}.csv"

        output_dir.mkdir(parents=True, exist_ok=True)
        file_exists = output_file.exists()

        df.to_csv(output_file, mode="a", index=False, header=not file_exists)
        self.logger.info(f"Batch data written to {output_file}")

    def set_columns(self, columns: List[str]) -> None:
        """Set the CSV column names."""
        self.csv_columns = columns

    def set_output_path(self, path: Path) -> None:
        """Set the output directory path."""
        self.output_path = path
