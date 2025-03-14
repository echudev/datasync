"""
Module for publishing hourly averages from a CSV file to an external endpoint.

This module defines a CSVPublisher class that reads daily CSV data from the 'data' directory,
calculates hourly averages, and sends them to a specified API endpoint, controlled by a control file.
"""

import os
import asyncio
import aiohttp
import aiofiles
import logging
import traceback
from datetime import datetime, timedelta
from enum import Enum
from dotenv import load_dotenv
from typing import Dict, Any, Optional, TypedDict
import pandas as pd
import json
import aiocsv
import backoff
from aiohttp import ClientTimeout, TCPConnector
from aiohttp.client_exceptions import ClientError


class PublisherState(Enum):
    RUNNING = 1
    STOPPED = 3


class SensorData(TypedDict):
    timestamp: str
    TEMP: Optional[float]
    HR: Optional[float]
    PA: Optional[float]
    VV: Optional[float]
    DV: Optional[float]
    LLUVIA: Optional[float]
    UV: Optional[float]
    RS: Optional[float]


class ApiPayload(TypedDict):
    apiKey: str
    origen: str
    data: SensorData


class CSVPublisher:
    """Class to handle publishing hourly CSV data to an external endpoint."""

    def __init__(
        self,
        csv_dir: str = "data",
        endpoint_url: str = None,
        origen: str = None,
        apiKey: str = None,
        check_interval: int = 5,
        logger: Optional[logging.Logger] = None,
        control_file: str = "d:\\datasync\\control.json",
    ):
        """
        Initialize the CSVPublisher.

        Args:
        - csv_dir (str): Directory containing the CSV files (default: "data").
        - endpoint_url (str): URL of the API endpoint (loaded from env if None).
        - check_interval (int): Interval in seconds to check the control file (default: 5).
        - logger: Logger instance (optional).
        """
        load_dotenv()
        self.csv_dir = csv_dir
        self.endpoint_url = endpoint_url or os.getenv("GOOGLE_POST_URL")
        if not self.endpoint_url:
            raise ValueError(
                "Endpoint URL must be provided or set in .env as GOOGLE_POST_URL"
            )
        self.origen = origen or os.getenv("ORIGEN")
        if not self.origen:
            raise ValueError("Origen must be provided or set in .env as ORIGEN")
        self.apiKey = apiKey or os.getenv("API_KEY")
        if not self.apiKey:
            raise ValueError("API Key must be provided or set in .env as API_KEY")
        self.check_interval = check_interval
        self.last_execution = None
        self.logger = logger or logging.getLogger("publisher")
        self.state = PublisherState.RUNNING
        self.state_lock = asyncio.Lock()
        self.control_file = control_file
        # Sensor mapping for the CSV columns
        self.sensors = [
            "Temperature",
            "Humidity",
            "Pressure",
            "WindSpeed",
            "WindDirection",
            "RainRate",
            "UV",
            "SolarRadiation",
        ]
        self.header_mapping = {
            "Temperature": "TEMP",  # Corregido el orden (API name -> CSV column)
            "Humidity": "HR",
            "Pressure": "PA",
            "WindSpeed": "VV",
            "WindDirection": "DV",
            "RainRate": "LLUVIA",
            "UV": "UV",
            "SolarRadiation": "RS",
        }
        self.timeout = ClientTimeout(total=30)
        self.connector = TCPConnector(limit=10)  # Limit concurrent connections
        self.max_retries = 3

    async def update_state(self, new_state: str) -> None:
        """Update state when changed by user."""
        state_value = new_state.upper()
        async with self.state_lock:
            if state_value == "STOPPED":
                self.state = PublisherState.STOPPED
            elif state_value == "RUNNING":
                self.state = PublisherState.RUNNING

    async def get_state(self) -> PublisherState:
        """Get current state in a thread-safe way."""
        async with self.state_lock:
            return self.state

    def _build_csv_path(self, year: str, month: str, day: str) -> str:
        path = os.path.join(self.csv_dir, year, month, f"{day}.csv")
        return path.replace("\\", "/")

    async def _read_csv(self, year: str, month: str, day: str) -> pd.DataFrame:
        """
        Read the daily CSV file for the given date.
        """
        csv_path = self._build_csv_path(year, month, day)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        try:
            rows = []
            header = None
            async with aiofiles.open(csv_path, mode="r", encoding="utf-8") as f:
                reader = aiocsv.AsyncReader(f)
                header = await reader.__anext__()  # Get header first
                async for row in reader:
                    # Convert numeric strings to float where possible
                    processed_row = []
                    for value in row:
                        try:
                            processed_row.append(float(value))
                        except (ValueError, TypeError):
                            processed_row.append(value)
                    rows.append(processed_row)

            df = pd.DataFrame(rows, columns=header)
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            return df
        except Exception as e:
            self.logger.error(f"Error reading CSV file {csv_path}: {e}")
            raise

    async def _read_control(self) -> Optional[datetime]:
        """Read last successful hour from control file."""
        try:
            if not os.path.exists(self.control_file):
                return None
            async with aiofiles.open(self.control_file, "r") as f:
                data = json.loads(await f.read())
                if data.get("last_successful", {}).get("publisher"):
                    return datetime.fromisoformat(data["last_successful"]["publisher"])
                return None
        except Exception as e:
            self.logger.error(f"Error reading control file: {e}")
            return None

    async def _update_control(self, timestamp: datetime) -> None:
        """Update last successful hour in control file."""
        try:
            async with aiofiles.open(self.control_file, "r") as f:
                data = json.loads(await f.read())

            if "last_successful" not in data:
                data["last_successful"] = {}

            data["last_successful"]["publisher"] = timestamp.isoformat()

            async with aiofiles.open(self.control_file, "w") as f:
                await f.write(json.dumps(data, indent=4))
        except Exception as e:
            self.logger.error(f"Error updating control file: {e}")

    def _calculate_hourly_averages(
        self, df: pd.DataFrame, target_hour: datetime
    ) -> Optional[SensorData]:
        """Calculate hourly averages for a specific hour."""
        try:
            if "timestamp" not in df.columns:
                raise ValueError("Column 'timestamp' not found in data")

            hour_start = target_hour.replace(minute=0, second=0, microsecond=0)
            hour_end = hour_start + timedelta(hours=1)

            df = df[
                (df["timestamp"] >= hour_start) & (df["timestamp"] < hour_end)
            ].copy()

            if df.empty:
                return None

            result: SensorData = {
                "timestamp": hour_start.strftime("%Y-%m-%d %H:00"),
                "TEMP": None,
                "HR": None,
                "PA": None,
                "VV": None,
                "DV": None,
                "LLUVIA": None,
                "UV": None,
                "RS": None,
            }

            # Calculate averages for all parameters using the mapping
            for api_name, csv_column in self.header_mapping.items():
                if csv_column in df.columns:
                    values = pd.to_numeric(df[csv_column], errors="coerce")
                    if not values.empty and not values.isna().all():
                        result[api_name] = round(float(values.mean()), 2)

            return result

        except Exception as e:
            self.logger.error(f"Error calculating hourly data: {str(e)}")
            raise

    @backoff.on_exception(
        backoff.expo, (ClientError, asyncio.TimeoutError), max_tries=3, max_time=30
    )
    async def _send_to_endpoint(self, data: Dict[str, Any]) -> bool:
        """
        Send data to the external endpoint asynchronously.

        - Args:
            data (Dict[str, Any]): Data to send in JSON format.
            test_mode (bool): If True, always return True (for testing).
        - Returns: bool: True if successful, False otherwise.
        """
        try:
            async with aiohttp.ClientSession(
                timeout=self.timeout, connector=self.connector
            ) as session:
                async with session.post(
                    self.endpoint_url,
                    headers={"Content-Type": "application/json"},
                    json=data,
                    raise_for_status=True,
                ) as response:
                    await response.read()
                    return True
        except Exception as e:
            self.logger.error(f"Error sending data: {e}")
            return False

    async def _execute_publish_cycle(self) -> None:
        """Execute publish cycle with hour control."""
        self.logger.info("Executing publish cycle...")
        now = datetime.now()
        last_hour = await self._read_control()

        if not last_hour:
            last_hour = now.replace(hour=0, minute=0, second=0, microsecond=0)

        # Process all hours from last successful until current
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        process_hour = last_hour + timedelta(hours=1)

        while process_hour <= current_hour:
            try:
                df = await self._read_csv(
                    process_hour.year,
                    process_hour.strftime("%m"),
                    process_hour.strftime("%d"),
                )
                if df is not None:
                    hourly_data = self._calculate_hourly_averages(df, process_hour)
                    if hourly_data:
                        success = await self._send_to_endpoint(hourly_data)
                        if success:
                            await self._update_control(process_hour)
                        else:
                            self.logger.warning(
                                f"Failed to send data for hour {process_hour}"
                            )
                            break

                process_hour += timedelta(hours=1)
            except Exception as e:
                self.logger.error(f"Error processing hour {process_hour}: {e}")
                break

    async def run(self) -> None:
        """
        Run the publisher asynchronously, executing at :03 of each hour.
        First execution happens immediately, then waits for next :03 mark.
        """
        self.logger.info("Starting Publisher...")
        first_run = True

        while await self.get_state() == PublisherState.RUNNING:
            try:
                now = datetime.now()
                if first_run:
                    await self._execute_publish_cycle()
                    first_run = False
                    self.last_execution = now
                else:
                    current_hour = now.replace(minute=3, second=0, microsecond=0)
                    if now >= current_hour and (
                        not self.last_execution or self.last_execution.hour != now.hour
                    ):
                        await self._execute_publish_cycle()
                        self.last_execution = now
                await asyncio.sleep(self.check_interval)

            except Exception as e:
                self.logger.error(f"Error in publisher run loop: {e}")
                await asyncio.sleep(self.check_interval)


def main():
    """Main function to start the publisher (para pruebas manuales)."""
    try:
        logger = logging.getLogger("data_collection")  # Usar el logger compartido
        publisher = CSVPublisher(logger=logger)
        asyncio.run(publisher.run())
    except KeyboardInterrupt:
        logger.info("Publisher stopped by user")
    except Exception as e:
        logger.error(f"Publisher failed to start: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    main()
