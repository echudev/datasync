"""
Module for publishing hourly averages from WinAQMS .wad files to an external endpoint.

This module defines a WinAQMSPublisher class that reads daily .wad data from the configured directory,
calculates hourly averages using the WinAQMS logic, and sends them to a specified API endpoint,
controlled by a control file.
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
import aiohttp
from dotenv import load_dotenv
from enum import Enum
import pandas as pd
from typing import Dict, Optional, Any


class PublisherState(Enum):
    """Enumeration for publisher states."""

    RUNNING = 1
    STOPPING = 2
    STOPPED = 3


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

    def update_state(self, new_state: str) -> None:
        """Update state when changed by user."""
        state_value = new_state.upper()
        if state_value == "STOPPED":
            self.state = PublisherState.STOPPED
        elif state_value == "RUNNING":
            self.state = PublisherState.RUNNING

    async def _read_wad_file(self, year: str, month: str, day: str) -> pd.DataFrame:
        """
        Read the WAD file for the given date.

        Args:
            year (str): Year to process.
            month (str): Month to process.
            day (str): Day to process.

        Returns:
            pd.DataFrame: DataFrame with the WAD data.

        Raises:
            FileNotFoundError: If the WAD file doesn't exist.
            Exception: For other read errors.
        """
        try:
            wad_folder = os.path.join(self.wad_dir, year, month)
            wad_file = f"eco{year}{month}{day}.wad"
            wad_path = os.path.join(wad_folder, wad_file)
            if not os.path.exists(wad_path):
                raise FileNotFoundError(f"WAD file not found: {wad_path}")

            # Use asyncio.to_thread for file reading to avoid blocking
            df = await asyncio.to_thread(pd.read_csv, wad_path)

            # Convert Date_Time column to datetime
            df["Date_Time"] = pd.to_datetime(
                df["Date_Time"], format="%Y/%m/%d %H:%M:%S", errors="coerce"
            )

            # Check if conversion was successful
            if df["Date_Time"].isnull().all():
                raise ValueError("All Date_Time values are invalid or null")

            return df
        except Exception as e:
            self.logger.error(f"Error reading WAD file: {e}")
            raise

    def _filter_hour_data(self, df: pd.DataFrame, hour: int) -> pd.DataFrame:
        """
        Filter the WAD data for a specific hour.

        Args:
            df (pd.DataFrame): DataFrame with the WAD data.
            hour (int): Hour to filter (0-23).

        Returns:
            pd.DataFrame: DataFrame with data for the specified hour.
        """
        # Create time range for filtering
        date = (
            df["Date_Time"].dt.date.iloc[0] if not df.empty else datetime.now().date()
        )
        start_time = datetime.combine(date, datetime.min.time()) + timedelta(hours=hour)
        end_time = start_time + timedelta(hours=1)

        # Filter rows for the specified hour
        return df[(df["Date_Time"] >= start_time) & (df["Date_Time"] < end_time)]

    def _calculate_hourly_averages(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate hourly averages from the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with WAD data.

        Returns:
            Dict[str, Any]: Dictionary with hourly averages data formatted for API.

        Raises:
            Exception: If calculation fails.
        """
        try:
            if df.empty:
                self.logger.warning("Empty DataFrame provided")
                now = datetime.now()
                timestamp = now.strftime("%Y-%m-%d %H:00")
                data = [
                    {
                        "timestamp": timestamp,
                        **{self.sensor_map[sensor]: None for sensor in self.sensors},
                    }
                ]
                return {"apiKey": self.apiKey, "origen": self.origen, "data": data}

            # Ensure Date_Time column exists
            if "Date_Time" not in df.columns:
                raise ValueError("Column 'Date_Time' not found in WAD data")

            # Ensure Date_Time is datetime type
            if not pd.api.types.is_datetime64_dtype(df["Date_Time"]):
                df["Date_Time"] = pd.to_datetime(
                    df["Date_Time"], format="%Y/%m/%d %H:%M:%S", errors="coerce"
                )

            # Group by hour and calculate averages
            df_hourly = (
                df.groupby(df["Date_Time"].dt.floor("h"))
                .mean(numeric_only=True)
                .reset_index()
            )

            if df_hourly.empty:
                self.logger.warning("No data available after grouping")
                now = datetime.now()
                timestamp = now.strftime("%Y-%m-%d %H:00")
                data = [
                    {
                        "timestamp": timestamp,
                        **{self.sensor_map[sensor]: None for sensor in self.sensors},
                    }
                ]
                return {"apiKey": self.apiKey, "origen": self.origen, "data": data}

            # Ensure all sensor columns are numeric
            for sensor in self.sensors:
                if sensor in df_hourly.columns:
                    df_hourly[sensor] = pd.to_numeric(
                        df_hourly[sensor], errors="coerce"
                    )

            # Format timestamps to string
            df_hourly["timestamp"] = df_hourly["Date_Time"].dt.strftime(
                "%Y-%m-%d %H:00"
            )

            # Process the data for each hour
            result_data = []
            for _, row in df_hourly.iterrows():
                hour_data = {"timestamp": row["timestamp"]}

                # Add sensor data with appropriate rounding
                for sensor in self.sensors:
                    if sensor in row and not pd.isna(row[sensor]):
                        value = row[sensor]
                        # Ensure consistent rounding precision with test expectations
                        if sensor in ("C1", "C2", "C3", "C4"):
                            # Use consistent rounding method to match test expectations
                            value = round(float(value), 3)
                        elif sensor == "C6":
                            value = round(value)
                        else:
                            value = round(value, 2)
                        hour_data[self.sensor_map[sensor]] = value
                    else:
                        hour_data[self.sensor_map[sensor]] = None

                result_data.append(hour_data)

            result = {"apiKey": self.apiKey, "origen": self.origen, "data": result_data}
            return result

        except Exception as e:
            self.logger.error(f"Error calculating hourly averages: {str(e)}")
            raise

    async def _send_to_endpoint(self, data: Dict[str, Any]) -> bool:
        """
        Send data to the external endpoint asynchronously.

        Args:
            data (Dict[str, Any]): Data to send in JSON format.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.endpoint_url,
                    headers={"Content-Type": "application/json"},
                    json=data,
                ) as response:
                    await response.text()
                    response.raise_for_status()
                    self.logger.info("WinAqms data sent successfully")
                    return True
        except Exception as e:
            self.logger.error(f"Error sending data to endpoint: {e}")
            return False

    async def run(self) -> None:
        """
        Run the publisher asynchronously, executing at :05 of each hour.
        First execution happens immediately, then waits for next :05 mark.
        """
        self.logger.info("Starting WinAQMS publisher...")
        first_run = True

        while self.state != PublisherState.STOPPED:
            try:
                now = datetime.now()

                if self.state == PublisherState.RUNNING:
                    if first_run:
                        await self._execute_publish_cycle()
                        first_run = False
                        self.last_execution = now
                    else:
                        current_hour = now.replace(minute=4, second=0, microsecond=0)
                        if now >= current_hour and (
                            not self.last_execution
                            or self.last_execution.hour != now.hour
                        ):
                            await self._execute_publish_cycle()
                            self.last_execution = now

                await asyncio.sleep(self.check_interval)

            except Exception as e:
                self.logger.error(f"Error in publisher run loop: {e}")
                await asyncio.sleep(self.check_interval)

    async def _execute_publish_cycle(self) -> None:
        """Helper method to execute one publish cycle asynchronously."""
        self.logger.info("Executing winaqms publish cycle...")
        now = datetime.now()
        year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")

        try:
            df = await self._read_wad_file(year, month, day)
            hourly_data = self._calculate_hourly_averages(df)
            success = await self._send_to_endpoint(hourly_data)
            if not success:
                self.logger.info("Failed to send WinAQMS data, continuing...")
        except Exception as e:
            self.logger.error(f"Error during publish cycle: {e}")


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
