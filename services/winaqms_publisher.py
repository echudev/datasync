"""
Module for publishing hourly averages from WinAQMS .wad files to an external endpoint.

This module defines a WinAQMSPublisher class that reads daily .wad data from the configured directory,
calculates hourly averages using the WinAQMS logic, and sends them to a specified API endpoint,
controlled by a control file.
"""

import os
import asyncio
import aiohttp
import aiofiles
import logging
import aiocsv  # Add this import
from datetime import datetime, timedelta
from dotenv import load_dotenv
from enum import Enum
from typing import Optional, TypedDict
import pandas as pd
import json
import backoff
from aiohttp import ClientTimeout
from aiohttp.client_exceptions import ClientError


class PublisherState(Enum):
    RUNNING = 1
    STOPPED = 3


class SensorData(TypedDict):
    timestamp: str
    CO: Optional[float]
    NO: Optional[float]
    NO2: Optional[float]
    NOx: Optional[float]
    O3: Optional[float]
    PM10: Optional[int]


class ApiPayload(TypedDict):
    apiKey: str
    origen: str
    data: SensorData


class WinAQMSPublisher:
    """Class to handle publishing hourly WinAQMS data to an external endpoint."""

    def __init__(
        self,
        wad_dir: str = "C:\\Data",
        endpoint_url: str = None,
        origen: str = None,
        apiKey: str = None,
        check_interval: int = 5,
        logger: Optional[logging.Logger] = None,
        control_file: str = "d:\\datasync\\control.json",
    ):
        """
        Initialize the WinAQMSPublisher.

        Args:
            wad_dir (str): Directory containing the WAD files (default: "C:\Data").
            endpoint_url (str): URL of the API endpoint (loaded from env if None).
            check_interval (int): Interval in seconds to check the control file (default: 5).
            logger: Logger instance (optional).
        """
        load_dotenv()
        self.wad_dir = wad_dir
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
        self.logger = logger or logging.getLogger("winaqms_publisher")
        self.state = PublisherState.RUNNING
        self.state_lock = asyncio.Lock()
        self.control_file = control_file

        # WinAQMS sensor configuration
        self.sensors = ["C1", "C2", "C3", "C4", "C5", "C6"]
        self.sensor_map = {
            "C1": "CO",
            "C2": "NO",
            "C3": "NO2",
            "C4": "NOx",
            "C5": "O3",
            "C6": "PM10",
        }
        self.timeout = ClientTimeout(total=30)  # 30 seconds timeout
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

    async def _read_wad_file(self, year: str, month: str, day: str) -> pd.DataFrame:
        """
        Read the WAD file for the given date asynchronously using aiocsv.
        """
        try:
            wad_folder = os.path.join(self.wad_dir, year, month)
            wad_file = f"eco{year}{month}{day}.wad"
            wad_path = os.path.join(wad_folder, wad_file)

            if not os.path.exists(wad_path):
                raise FileNotFoundError(f"WAD file not found: {wad_path}")

            rows = []
            header = None
            async with aiofiles.open(wad_path, mode="r", encoding="utf-8") as f:
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
            df["Date_Time"] = pd.to_datetime(
                df["Date_Time"], format="%Y/%m/%d %H:%M:%S", errors="coerce"
            )
            return df

        except Exception as e:
            self.logger.error(f"Error reading WAD file: {e}")
            raise

    async def _read_control(self) -> Optional[datetime]:
        """Read last successful hour from control file."""
        try:
            if not os.path.exists(self.control_file):
                return None
            async with aiofiles.open(self.control_file, "r") as f:
                data = json.loads(await f.read())
                if data.get("last_successful", {}).get("winaqms_publisher"):
                    return datetime.fromisoformat(
                        data["last_successful"]["winaqms_publisher"]
                    )
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

            data["last_successful"]["winaqms_publisher"] = timestamp.isoformat()

            async with aiofiles.open(self.control_file, "w") as f:
                await f.write(json.dumps(data, indent=4))
        except Exception as e:
            self.logger.error(f"Error updating control file: {e}")

    def _calculate_hourly_averages(
        self, df: pd.DataFrame, target_hour: datetime
    ) -> Optional[SensorData]:
        """Calculate hourly averages for a specific hour."""
        try:
            if "Date_Time" not in df.columns:
                raise ValueError("Column 'Date_Time' not found in WAD data")

            # Filter data for target hour
            hour_start = target_hour.replace(minute=0, second=0, microsecond=0)
            hour_end = hour_start + timedelta(hours=1)

            df = df[
                (df["Date_Time"] >= hour_start) & (df["Date_Time"] < hour_end)
            ].copy()

            if df.empty:
                return None

            result: SensorData = {
                "timestamp": hour_start.strftime("%Y-%m-%d %H:00"),
                "CO": None,
                "NO": None,
                "NO2": None,
                "NOx": None,
                "O3": None,
                "PM10": None,
            }

            # Calculate averages for each sensor
            for sensor in self.sensors:
                if sensor in df.columns:
                    values = pd.to_numeric(df[sensor], errors="coerce")
                    if not values.empty and not values.isna().all():
                        avg_value = values.mean()
                        if sensor in ("C1", "C2", "C3", "C4"):
                            avg_value = round(float(avg_value), 3)
                        elif sensor == "C6":
                            avg_value = round(avg_value)
                        else:
                            avg_value = round(avg_value, 2)
                        result[self.sensor_map[sensor]] = avg_value
                    else:
                        result[self.sensor_map[sensor]] = None
                else:
                    result[self.sensor_map[sensor]] = None

            return result

        except Exception as e:
            self.logger.error(f"Error calculating hourly data: {str(e)}")
            raise

    @backoff.on_exception(
        backoff.expo, (ClientError, asyncio.TimeoutError), max_tries=3, max_time=30
    )
    async def _send_to_endpoint(self, sensor_data: SensorData) -> bool:
        """
        Send data to the external endpoint asynchronously.

        Args:
            sensor_data (SensorData): Single sensor data reading to send.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            api_payload: ApiPayload = {
                "apiKey": self.apiKey,
                "origen": self.origen,
                "data": sensor_data,
            }

            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(
                    self.endpoint_url,
                    headers={"Content-Type": "application/json"},
                    json=api_payload,
                    raise_for_status=True,
                ) as response:
                    response_text = (
                        await response.text()
                    )  # Asegurar que leemos la respuesta
                    self.logger.info(
                        f"WinAqms data sent successfully: {response_text[:100]}..."
                    )
                    return True
        except Exception as e:
            self.logger.error(f"Error sending data to endpoint: {e}")
            return False

    async def _execute_publish_cycle(self) -> None:
        """Execute publish cycle with hour control."""
        self.logger.info("Executing winaqms publish cycle...")
        now = datetime.now()
        last_hour = await self._read_control()

        if not last_hour:
            last_hour = now.replace(hour=0, minute=0, second=0, microsecond=0)

        # Process all hours from last successful until current
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        process_hour = last_hour + timedelta(hours=1)

        while process_hour <= current_hour:
            try:
                year, month, day = (
                    process_hour.strftime("%Y"),
                    process_hour.strftime("%m"),
                    process_hour.strftime("%d"),
                )
                df = await self._read_wad_file(year, month, day)

                hourly_data = self._calculate_hourly_averages(df, process_hour)
                if hourly_data:
                    success = await self._send_to_endpoint(hourly_data)
                    if success:
                        await self._update_control(process_hour)
                    else:
                        self.logger.warning(
                            f"Failed to send data for hour {process_hour}"
                        )
                        break  # Stop processing on failure

                process_hour += timedelta(hours=1)
            except Exception as e:
                self.logger.error(f"Error processing hour {process_hour}: {e}")
                break

    async def run(self) -> None:
        """
        Run the publisher asynchronously, executing at :04 of each hour.
        First execution happens immediately, then waits for next :04 mark.
        """
        self.logger.info("Starting WinAQMS publisher...")
        first_run = True

        while await self.get_state() == PublisherState.RUNNING:
            try:
                now = datetime.now()
                if first_run:
                    await self._execute_publish_cycle()
                    first_run = False
                    self.last_execution = now
                else:
                    current_hour = now.replace(minute=4, second=0, microsecond=0)
                    if now >= current_hour and (
                        not self.last_execution or self.last_execution.hour != now.hour
                    ):
                        await self._execute_publish_cycle()
                        self.last_execution = now
                await asyncio.sleep(self.check_interval)

            except Exception as e:
                self.logger.error(f"Error in publisher run loop: {e}")
                await asyncio.sleep(self.check_interval)


async def main():
    """Main function to start the publisher."""
    try:
        logger = logging.getLogger("data_collection")  # Use shared logger
        publisher = WinAQMSPublisher(logger=logger)
        await publisher.run()
    except KeyboardInterrupt:
        logger.info("WinAQMS Publisher stopped by user")
    except Exception as e:
        logger.error(f"WinAQMS Publisher failed to start: {e}")


if __name__ == "__main__":
    asyncio.run(main())
