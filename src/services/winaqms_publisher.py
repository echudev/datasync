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
import aiocsv
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Optional
import pandas as pd
import json
import backoff
from aiohttp import ClientTimeout
from aiohttp.client_exceptions import ClientError
from utils.control import CONTROL_FILE, update_control_file
from pathlib import Path
from models import AirQualityDeviceDTO
from utils.log_manager import LogManager


class WinAQMSPublisher:
    """Class to handle publishing hourly WinAQMS data to an external endpoint."""

    def __init__(
        self,
        wad_dir: str = "C:\\Data",
        endpoint_url: str = None,
        origen: str = None,
        apiKey: str = None,
        check_interval: int = 5,
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
        self.wad_dir = Path(wad_dir)
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
        # Initialize logger
        self.log_manager = LogManager(log_file="winaqms_publisher.log")
        self.logger = self.log_manager.logger

        self.control_file = CONTROL_FILE  # Usar la constante del módulo control
        self._task = None  # Almacenar la tarea principal

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

    def _build_wad_path(self, year: str, month: str, day: str) -> Path:
        """Build path to WAD file for given date."""
        # Convert all inputs to strings and zero-pad month/day
        year_str = str(year)
        month_str = str(month).zfill(2)
        day_str = str(day).zfill(2)

        # Build WAD filename
        wad_file = f"eco{year_str}{month_str}{day_str}.wad"

        # Construct full path using Path object
        return self.wad_dir / year_str / month_str / wad_file

    async def _read_wad_file(self, year: str, month: str, day: str) -> pd.DataFrame:
        """
        Read the WAD file for the given date asynchronously using aiocsv.
        """
        try:
            wad_path = self._build_wad_path(year, month, day)
            if not wad_path.exists():
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
            if not self.control_file.exists():
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

    def _calculate_hourly_averages(
        self, df: pd.DataFrame, target_hour: datetime
    ) -> Optional[AirQualityDeviceDTO]:
        """Calculate hourly averages for a specific hour."""
        try:
            if "Date_Time" not in df.columns:
                raise ValueError("Column 'Date_Time' not found in WAD data")

            # Filter data for target hour
            hour_start = target_hour.replace(minute=0, second=0, microsecond=0)
            hour_end = hour_start + timedelta(hours=1)
            
            # Log timeframe being processed
            self.logger.debug(f"Processing data from {hour_start} to {hour_end}")

            # Log unique timestamps before filtering
            self.logger.debug(f"Unique timestamps before filtering: {df['Date_Time'].unique()}")

            df = df[
                (df["Date_Time"] >= hour_start) & (df["Date_Time"] < hour_end)
            ].copy()

            # Log number of records after filtering
            self.logger.debug(f"Number of records for hour {hour_start.hour}: {len(df)}")

            if df.empty:
                return None

            result: AirQualityDeviceDTO = {
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
                        # Log raw values for C6 (PM10)
                        if sensor == "C6":
                            self.logger.debug(f"C6 raw values: {values.tolist()}")
                        
                        avg_value = values.mean()
                        if sensor in ("C1", "C2", "C3", "C4"):
                            avg_value = round(float(avg_value), 3)
                        elif sensor == "C6":
                            avg_value = round(avg_value)
                            self.logger.debug(f"C6 calculated average: {avg_value} for hour {hour_start}")
                        else:
                            avg_value = round(avg_value, 2)
                        result[self.sensor_map[sensor]] = avg_value
                    else:
                        result[self.sensor_map[sensor]] = None
                else:
                    result[self.sensor_map[sensor]] = None

            # Log final result
            self.logger.debug(f"Calculated averages for {hour_start}: {result}")
            return result

        except Exception as e:
            self.logger.error(f"Error calculating hourly data: {str(e)}")
            raise

    @backoff.on_exception(
        backoff.expo, (ClientError, asyncio.TimeoutError), max_tries=3, max_time=30
    )
    async def _send_to_endpoint(self, device_data: AirQualityDeviceDTO) -> bool:
        """
        Send data to the external endpoint asynchronously.

        Args:
            device_data (AirQualitySensorData): Single sensor data reading to send.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            api_payload = {
                "apiKey": self.apiKey,
                "origen": self.origen,
                "data": [device_data],
            }

            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(
                    self.endpoint_url,
                    headers={"Content-Type": "application/json"},
                    json=api_payload,
                    raise_for_status=True,
                ) as response:
                    response_text = await response.text()
                    self.logger.info(
                        f"WinAqms data: {device_data['timestamp']}, sent successfully to: {response_text[:100]}"
                    )
                    return True
        except Exception as e:
            self.logger.error(f"Error sending data to endpoint: {e}")
            return False

    async def _execute_publish_cycle(self) -> None:
        """Execute publish cycle with hour control."""
        self.logger.debug("Executing publish cycle (publish_cycle method)")
        now = datetime.now()
        last_hour = await self._read_control()

        if not last_hour:
            last_hour = now.replace(hour=0, minute=0, second=0, microsecond=0)

        # Process all hours from last successful until current
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        process_hour = last_hour + timedelta(hours=1)
        self.logger.debug(f"Last successful hour: {last_hour}, Current hour: {current_hour}")

        while process_hour < current_hour:
            self.logger.debug(f"Processing hour: {process_hour}, Current hour: {current_hour}")
            try:
                year, month, day = (
                    process_hour.strftime("%Y"),
                    process_hour.strftime("%m"),
                    process_hour.strftime("%d"),
                )
                try:
                    df = await self._read_wad_file(year, month, day)
                except FileNotFoundError:
                    self.logger.warning(f"No data file found for {year}/{month}/{day}, skipping hour {process_hour.hour}")
                    process_hour += timedelta(hours=1)
                    continue

                hourly_data = self._calculate_hourly_averages(df, process_hour)
                self.logger.debug(f"Hourly data for {process_hour}: {hourly_data}")
                if hourly_data:
                    success = await self._send_to_endpoint(hourly_data)
                    if success:
                        # Reemplazar llamada al método local por la función del módulo
                        data = {
                            "last_successful": {
                                "winaqms_publisher": process_hour.isoformat()
                            }
                        }
                        await update_control_file("last_successful", data)
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
        Run the publisher asynchronously, executing at :02 of each hour.
        First execution happens immediately, then waits for next :02 mark.
        """
        self.logger.info("Starting WinAQMS publisher...")
        first_run = True

        while True:
            try:
                now = datetime.now()
                self.logger.debug(f"Current time: {now}, Last execution: {self.last_execution}")
                if first_run:
                    self.logger.debug("First run, executing winaqms publish")
                    await self._execute_publish_cycle()
                    first_run = False
                    self.last_execution = now
                    self.logger.debug(f"First run completed, last execution set to: {self.last_execution}")
                else:
                    current_hour = now.replace(minute=2, second=0, microsecond=0)
                    self.logger.debug(f"Current hour for execution: {current_hour}")
                    if now >= current_hour and (
                        not self.last_execution or self.last_execution.hour != now.hour
                    ):
                        self.logger.debug("Executing publish cycle (run loop)")
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
