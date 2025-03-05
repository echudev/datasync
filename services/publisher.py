"""
Module for publishing hourly averages from a CSV file to an external endpoint.

This module defines a CSVPublisher class that reads daily CSV data from the 'data' directory,
calculates hourly averages, and sends them to a specified API endpoint once per execution.
"""

import logging
import os
import json
from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("publisher.log")],
)
logger = logging.getLogger("publisher")


class CSVPublisher:
    """Class to handle publishing hourly CSV data to an external endpoint."""

    def __init__(
        self,
        csv_dir: str = "data",
        endpoint_url: str = None,
        origen: str = "CENTENARIO",
    ):
        """
        Initialize the CSVPublisher.

        Args:
            csv_dir (str): Directory containing the CSV files (default: "data").
            endpoint_url (str): URL of the API endpoint (loaded from env if None).
            origen (str): Origin identifier for the data (default: "CENTENARIO").
        """
        load_dotenv()
        self.csv_dir = csv_dir
        self.endpoint_url = endpoint_url or os.getenv("GOOGLE_POST_URL")
        if not self.endpoint_url:
            raise ValueError(
                "Endpoint URL must be provided or set in .env as GOOGLE_POST_URL"
            )
        self.origen = origen
        self.headers = {
            "Content-Type": "application/json",
        }

    def _read_csv(self, year: str, month: str, day: str) -> pd.DataFrame:
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
            df = pd.read_csv(csv_path)
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise

    def _calculate_hourly_averages(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate hourly averages from the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with minutal data.

        Returns:
            Dict[str, Any]: Dictionary with hourly averages, rounded appropriately.
        """
        try:
            # Convert timestamp to datetime and group by hour
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df_hourly = df.groupby(df["timestamp"].dt.floor("H")).mean().reset_index()

            # Redondear promedios: 1 decimal para todo menos RainRate (2 decimales)
            data = df_hourly.to_dict(orient="records")
            for record in data:
                for key in record:
                    if key != "timestamp" and key != "RainRate":
                        record[key] = round(float(record[key]), 1)
                    elif key == "RainRate":
                        record[key] = round(float(record[key]), 2)

            return {"origen": self.origen, "data": data}
        except Exception as e:
            logger.error(f"Error calculating hourly averages: {e}")
            raise

    def _send_to_endpoint(self, data: Dict[str, Any]) -> bool:
        """
        Send data to the external endpoint synchronously.

        Args:
            data (Dict[str, Any]): Data to send in JSON format.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            response = requests.post(
                self.endpoint_url,
                headers=self.headers,
                data=json.dumps(data),
            )
            response.raise_for_status()
            logger.info(
                f"[{datetime.now()}] Data sent successfully to {self.endpoint_url}: {response.text}"
            )
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"[{datetime.now()}] Error sending data to endpoint: {e}")
            return False

    def run_once(self) -> None:
        """
        Run the publisher to send hourly averages once per execution.
        """
        try:
            # Obtener la fecha actual
            now = datetime.now()
            year, month, day = (
                now.strftime("%Y"),
                now.strftime("%m"),
                now.strftime("%d"),
            )

            # Leer el CSV
            df = self._read_csv(year, month, day)

            # Calcular promedios horarios
            hourly_data = self._calculate_hourly_averages(df)

            # Enviar a la API
            success = self._send_to_endpoint(hourly_data)
            if not success:
                logger.warning(f"[{datetime.now()}] Failed to send data")
        except Exception as e:
            logger.error(f"[{datetime.now()}] Error in publisher run_once: {e}")


def main():
    """Main function to start the publisher for a single execution."""
    try:
        publisher = CSVPublisher()
        publisher.run_once()
    except Exception as e:
        logger.error(f"Publisher failed to start: {e}")


if __name__ == "__main__":
    main()
