"""
Data Collector Service

This module implements the data collection logic for environmental sensors
using asyncio for concurrency and pandas for data handling.
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from core.models import Device, DeviceConfig
from core.utils.path_dir import DATA_DIR

class DataCollector:
    """Manages data collection from multiple devices with a unified buffer."""
    
    def __init__(self, columns: List[str], logger: logging.Logger):
        self.output_path = DATA_DIR
        self.csv_columns = columns
        self.logger = logger
        self._devices: Dict[str, Device] = {}
        self._configs: Dict[str, DeviceConfig] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._running = False
        self._device_states: Dict[str, bool] = {}  # Estado individual de cada dispositivo
        self._data_buffer = defaultdict(lambda: {"data": defaultdict(float), "count": 0})
        self._data_lock = asyncio.Lock()
        self._processor_task: Optional[asyncio.Task] = None
    
    async def add_device(self, device: Device, device_config: DeviceConfig) -> None:
        """Add a new device to the collector."""
        device_name = device_config["name"]
        self._devices[device_name] = device
        self._configs[device_name] = device_config
        self._device_states[device_name] = False  # Inicialmente detenido
        self.logger.info(f"Added device {device_name}")
    
    async def remove_device(self, device_name: str) -> None:
        """Remove a device from the collector."""
        if device_name in self._tasks:
            await self.stop_device(device_name)
            del self._tasks[device_name]
        
        if device_name in self._devices:
            del self._devices[device_name]
            del self._configs[device_name]
            del self._device_states[device_name]
            self.logger.info(f"Removed device {device_name}")
    
    async def start_device(self, device_name: str) -> None:
        """Start data collection for a specific device."""
        if device_name not in self._devices:
            self.logger.error(f"Device {device_name} not found")
            return
            
        if self._device_states[device_name]:
            self.logger.warning(f"Device {device_name} is already running")
            return
            
        self._device_states[device_name] = True
        self._tasks[device_name] = asyncio.create_task(
            self._collect_device_data(device_name, self._devices[device_name], self._configs[device_name])
        )
        self.logger.info(f"Started data collection for device {device_name}")
    
    async def stop_device(self, device_name: str) -> None:
        """Stop data collection for a specific device."""
        if device_name not in self._devices:
            return
            
        if not self._device_states[device_name]:
            return
            
        self._device_states[device_name] = False
        if device_name in self._tasks:
            self._tasks[device_name].cancel()
            try:
                await self._tasks[device_name]
            except asyncio.CancelledError:
                pass
            del self._tasks[device_name]
        self.logger.info(f"Stopped data collection for device {device_name}")
    
    async def start_collection(self) -> None:
        """Start data collection for all devices."""
        if self._running:
            return
            
        self._running = True
        for device_name in self._devices:
            await self.start_device(device_name)
        
        self._processor_task = asyncio.create_task(self._process_data())
        self.logger.info("Started data collection for all devices")
    
    async def stop_collection(self) -> None:
        """Stop data collection for all devices."""
        if not self._running:
            return
            
        self._running = False
        
        # Detener todos los dispositivos
        for device_name in list(self._devices.keys()):
            await self.stop_device(device_name)
        
        # Cancelar la tarea de procesamiento
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
            self._processor_task = None
        
        self.logger.info("Stopped data collection for all devices")
    
    def get_device_state(self, device_name: str) -> bool:
        """Get the current state of a device."""
        return self._device_states.get(device_name, False)
    
    async def _collect_device_data(self, device_name: str, device: Device, config: DeviceConfig) -> None:
        """Collect data from a single device."""
        try:
            while self._device_states[device_name]:  # Usar el estado individual del dispositivo
                start_time = datetime.now()
                timestamp_key = start_time.strftime("%Y-%m-%d %H:%M")
                
                try:
                    device_data = await device.read()
                    
                    async with self._data_lock:
                        buffer_entry = self._data_buffer[timestamp_key]
                        for key, value in device_data.items():
                            buffer_entry["data"][key] = (
                                buffer_entry["data"].get(key, 0.0) + value
                            )
                        buffer_entry["count"] += 1
                        
                except Exception as e:
                    self.logger.error(f"Error reading from {device_name}: {e}")
                
                elapsed = (datetime.now() - start_time).total_seconds()
                sleep_time = max(0.1, config["scan_interval"] - elapsed)
                await asyncio.sleep(sleep_time)
                
        except asyncio.CancelledError:
            self.logger.info(f"Collection cancelled for {device_name}")
        except Exception as e:
            self.logger.error(f"Unexpected error in collection for {device_name}: {e}")
        finally:
            self._device_states[device_name] = False
    
    async def _process_data(self) -> None:
        """Process and save collected data periodically."""
        try:
            while self._running:
                await asyncio.sleep(60.0)  # Process every minute
                
                now = datetime.now()
                process_time = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
                timestamp_key = process_time.strftime("%Y-%m-%d %H:%M")
                
                async with self._data_lock:
                    if timestamp_key in self._data_buffer:
                        buffer_entry = self._data_buffer[timestamp_key]
                        averages = {
                            k: round(v / buffer_entry["count"], 1)
                            if k != "RainRate"
                            else round(v / buffer_entry["count"], 2)
                            for k, v in buffer_entry["data"].items()
                        }
                        
                        await self._save_batch_data([{
                            "timestamp": timestamp_key,
                            **averages
                        }])
                        
                        del self._data_buffer[timestamp_key]
                        
        except asyncio.CancelledError:
            self.logger.info("Data processor cancelled")
        except Exception as e:
            self.logger.error(f"Error in data processor: {e}")
    
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