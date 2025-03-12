"""
Module for publishing hourly averages from a CSV file to an external endpoint.

This module defines a CSVPublisher class that reads daily CSV data from the 'data' directory,
calculates hourly averages, and sends them to a specified API endpoint, controlled by a control file.
"""

import os
import asyncio
import logging
from datetime import datetime
from enum import Enum
import pandas as pd
import aiohttp
from dotenv import load_dotenv
from typing import Dict, Any, Optional


class PublisherState(Enum):
    RUNNING = 1
    STOPPING = 2
    STOPPED = 3


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
    ):
        """
        Initialize the CSVPublisher.

        Args:
            csv_dir (str): Directory containing the CSV files (default: "data").
            endpoint_url (str): URL of the API endpoint (loaded from env if None).
            check_interval (int): Interval in seconds to check the control file (default: 5).
            logger: Logger instance (optional).
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

    def update_state(self, new_state: str) -> None:
        """Update state when changed by user."""
        state_value = new_state.upper()
        if state_value == "STOPPED":
            self.state = PublisherState.STOPPED
        elif state_value == "RUNNING":
            self.state = PublisherState.RUNNING

    async def _read_csv(self, year: str, month: str, day: str) -> pd.DataFrame:
        """
        Read the daily CSV file for the given date.

        Args:
            year (str): Year in YYYY format.
            month (str): Month in MM format.
            day (str): Day in DD format.

        Returns:
            pd.DataFrame: DataFrame with the CSV data.

        Raises:
            FileNotFoundError: If the CSV file doesn't exist.
            Exception: For other read errors.
        """
        try:
            csv_path = os.path.join(self.csv_dir, year, month, f"{day}.csv")
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"CSV file not found: {csv_path}")

            # Use asyncio.to_thread for file reading to avoid blocking
            df = await asyncio.to_thread(pd.read_csv, csv_path)
            return df
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {e}")
            raise

    def _calculate_hourly_averages(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate hourly averages from the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with minutal data.

        Returns:
            Dict[str, Any]: Dictionary with hourly averages, rounded appropriately.

        Raises:
            Exception: If calculation fails.
        """
        try:
            if "timestamp" not in df.columns:
                raise ValueError("Column 'timestamp' not found in CSV data")
            df = (
                df.copy()
            )  # Trabajar con una copia para evitar problemas de modificación
            df["timestamp"] = pd.to_datetime(
                df["timestamp"], errors="coerce", format="%Y-%m-%d %H:%M"
            )
            if df["timestamp"].isnull().all():
                raise ValueError("All timestamps are invalid or null")

            # Agrupar por hora y calcular promedios
            df_hourly = (
                df.groupby(df["timestamp"].dt.floor("h"))
                .mean(numeric_only=True)
                .reset_index()
            )

            # Convertir Timestamp a string antes de devolver los datos
            df_hourly["timestamp"] = df_hourly["timestamp"].astype(str)

            # Redondear promedios y manejar NaN
            data = df_hourly.to_dict(orient="records")

            # Mapear claves de los datos a los encabezados esperados por Google Apps Script
            header_mapping = {
                "Temperature": "TEMP",
                "Humidity": "HR",
                "Pressure": "PA",
                "WindSpeed": "VV",
                "WindDirection": "DV",
                "RainRate": "LLUVIA",
                "UV": "UV",
                "SolarRadiation": "RS",
            }

            new_data = []
            for record in data:
                new_record = {"timestamp": record["timestamp"]}
                for old_key, new_key in header_mapping.items():
                    value = record.get(old_key)
                    if pd.isna(value):
                        new_record[new_key] = (
                            None  # Reemplazar NaN con None (JSON null)
                        )
                    else:
                        if new_key == "LLUVIA":
                            new_record[new_key] = round(float(value), 2)
                        else:
                            new_record[new_key] = round(float(value), 1)
                new_data.append(new_record)

            result = {"apiKey": self.apiKey, "origen": self.origen, "data": new_data}
            return result
        except Exception as e:
            self.logger.error(f"Error calculating hourly averages: {e}")
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
                    self.logger.info("Data sent successfully")
                    return True
        except Exception as e:
            self.logger.error(f"Error sending data to endpoint: {e}")
            return False

    async def run(self) -> None:
        """
        Run the publisher asynchronously, executing at :05 of each hour.
        First execution happens immediately, then waits for next :05 mark.
        """
        self.logger.info("Starting Publisher...")
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
                        current_hour = now.replace(minute=3, second=0, microsecond=0)
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
        self.logger.info("Executing publish cycle...")
        now = datetime.now()
        year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")

        try:
            df = await self._read_csv(year, month, day)
            hourly_data = self._calculate_hourly_averages(df)
            success = await self._send_to_endpoint(hourly_data)
            if not success:
                self.logger.info("Failed to send data, continuing...")
        except Exception as e:
            self.logger.error(f"Error during publish cycle: {e}")


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


if __name__ == "__main__":
    main()
