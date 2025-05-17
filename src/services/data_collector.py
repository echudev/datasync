"""
Data Collector Service

This module implements the data collection logic for environmental sensors
using asyncio for concurrency and pandas for data handling.
"""

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from models import Device, DeviceConfig
from utils.log_manager import LogManager
from utils.path_dir import DATA_DIR  

class DataCollector:
    """Handles collection and processing of device data."""

    def __init__(self, columns: List[str]):
        self.output_path = DATA_DIR
        self.data_buffer = defaultdict(lambda: {"data": defaultdict(float), "count": 0})
        self.data_to_save = []
        self.csv_columns = columns
        self.data_lock = asyncio.Lock()
        # Initialize logger
        self.log_manager = LogManager(log_file="collector.log")
        self.logger = self.log_manager.logger

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        # Give time for tasks to finish gracefully
        await asyncio.sleep(1)
        return None

    async def collect_data(self, device: Device, device_config: DeviceConfig) -> None:
        """Collect data from a sensor at regular intervals."""
        required = {"name", "keys", "scan_interval"}
        if not all(k in device_config for k in required):
            raise ValueError(f"Sensor config missing required fields: {required}")

        name = device_config["name"]
        scan_interval = device_config["scan_interval"]

        self.logger.info(f"Starting data collection for sensor {name}")
        try:
            while True:
                start_time = datetime.now()
                timestamp_key = start_time.strftime("%Y-%m-%d %H:%M")

                device_data = await device.read()

                async with self.data_lock:
                    buffer_entry = self.data_buffer[timestamp_key]
                    for key, value in device_data.items():
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
            self.logger.info(f"Stopped data collection for sensor {name}")

    async def process_and_save_data(
        self, output_interval: float = 60.0
    ) -> None:
        """Process collected data and save each minute."""
        self.logger.info("Starting data processing task")
        try:
            while True:
                await asyncio.sleep(output_interval)

                now = datetime.now()
                process_time = now.replace(second=0, microsecond=0)
                process_time = process_time - timedelta(minutes=1)
                timestamp_key = process_time.strftime("%Y-%m-%d %H:%M")

                async with self.data_lock:
                    if timestamp_key in self.data_buffer:
                        buffer_entry = self.data_buffer[timestamp_key]
                        averages = {
                            k: round(v / buffer_entry["count"], 1)
                            if k != "RainRate"
                            else round(v / buffer_entry["count"], 2)
                            for k, v in buffer_entry["data"].items()
                        }
                        self.data_to_save.append(
                            {"timestamp": timestamp_key, **averages}
                        )
                        del self.data_buffer[timestamp_key]
                        # Guardar inmediatamente los datos del minuto
                        await self._save_batch_data(self.data_to_save)
                        self.data_to_save.clear()

        except Exception as e:
            self.logger.error(f"Error in data processing: {e}")
        finally:
            if self.data_to_save:
                await self._save_batch_data(self.data_to_save)
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

        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            file_exists = output_file.exists()
            df.to_csv(output_file, mode="a", index=False, header=not file_exists)
        except Exception as e:
            self.logger.error(f"Error saving batch data to {output_file}: {e}")
            raise