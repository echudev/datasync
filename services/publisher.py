"""
Module for publishing hourly averages from a CSV file to an external endpoint.

This module defines a CSVPublisher class that reads daily CSV data from the 'data' directory,
calculates hourly averages, and sends them to a specified API endpoint, controlled by a control file.
"""

import os
import json
import time
import logging
from datetime import datetime
from enum import Enum
import pandas as pd
import requests
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
        control_file: str = "control.json",
        check_interval: int = 5,
        logger: Optional[
            logging.Logger
        ] = None,  # Logger opcional para usar el compartido
    ):
        """
        Initialize the CSVPublisher.

        Args:
            csv_dir (str): Directory containing the CSV files (default: "data").
            endpoint_url (str): URL of the API endpoint (loaded from env if None).
            control_file (str): Path to the control file (default: "control.json").
            check_interval (int): Interval in seconds to check the control file (default: 5).
            logger: Logger instance (si se pasa, se usa; de lo contrario, se crea uno local).
        """
        load_dotenv()
        self.csv_dir = csv_dir
        self.endpoint_url = endpoint_url or os.getenv("GOOGLE_POST_URL")
        if not self.endpoint_url:
            raise ValueError(
                "Endpoint URL must be provided or set in .env as GOOGLE_POST_URL"
            )
        self.control_file = control_file
        self.check_interval = check_interval
        self.last_execution = None
        self.state = PublisherState.RUNNING
        self.logger = logger or logging.getLogger(
            "publisher"
        )  # Usar logger compartido o crear uno local

    def _read_control_file(self) -> Dict[str, Any]:
        """
        Read the control file to determine the state of the publisher.

        Returns:
            Dict[str, Any]: Dictionary with control state (e.g., {"publisher": "RUNNING"}).

        Raises:
            FileNotFoundError: If the control file doesn't exist.
            json.JSONDecodeError: If the control file is not valid JSON.
        """
        try:
            with open(self.control_file, "r") as f:
                control = json.load(f)
            return control
        except FileNotFoundError:
            self.logger.error(
                f"Control file {self.control_file} not found. Defaulting to STOPPED."
            )
            return {"publisher": "STOPPED"}
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding control file {self.control_file}: {e}")
            return {"publisher": "STOPPED"}

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
                df.groupby(df["timestamp"].dt.floor("H"))
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

            result = {"origen": "CENTENARIO", "data": new_data}
            return result
        except Exception as e:
            self.logger.error(f"Error calculating hourly averages: {e}")
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
                headers={"Content-Type": "application/json"},
                data=json.dumps(data),
            )
            response.raise_for_status()
            self.logger.info(f"Data sent successfully: {response.text}")
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error sending data to endpoint: {e}")
            return False

    def run(self) -> None:
        """
        Run the publisher, checking both internal state and control file.
        Executes once per hour if in RUNNING state.
        """
        self.logger.info("Starting Publisher...")
        while self.state != PublisherState.STOPPED:
            try:
                # Check the control file to sync with external commands
                control = self._read_control_file()
                file_state = control.get("publisher", "STOPPED").upper()

                # Sync internal state with control file if needed
                if (
                    file_state == "STOPPED"
                    and self.state != PublisherState.STOPPING
                    and self.state != PublisherState.STOPPED
                ):
                    self.logger.info("Publisher stopping due to control file.")
                    self.state = PublisherState.STOPPING
                elif file_state == "RUNNING" and self.state != PublisherState.RUNNING:
                    self.state = PublisherState.RUNNING
                    self.logger.info("Publisher resumed via control file.")

                # Verify the state and take action
                if self.state == PublisherState.STOPPING:
                    self.logger.info("Publisher stopping gracefully...")
                    self.state = PublisherState.STOPPED
                    break
                elif self.state == PublisherState.RUNNING:
                    # Verificar si ha pasado una hora desde la última ejecución
                    now = datetime.now()
                    if (
                        not self.last_execution
                        or (now - self.last_execution).total_seconds() >= 3600
                    ):
                        self.logger.info("Executing publish cycle...")
                        # Obtener la fecha actual
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
                            self.logger.info(
                                "Failed to send data, continuing..."
                            )  # Cambiado a INFO

                        # Actualizar la última ejecución
                        self.last_execution = now
                    else:
                        time.sleep(self.check_interval)
                else:
                    self.logger.info(
                        f"Unknown state for publisher: {self.state}. Defaulting to STOPPED."
                    )  # Cambiado a INFO
                    time.sleep(self.check_interval)
                    continue

                # Esperar antes de verificar el archivo de control nuevamente
                time.sleep(self.check_interval)

            except Exception as e:
                self.logger.error(f"Error in publisher run loop: {e}")
                time.sleep(self.check_interval)


def main():
    """Main function to start the publisher (para pruebas manuales)."""
    try:
        logger = logging.getLogger("data_collection")  # Usar el logger compartido
        publisher = CSVPublisher(logger=logger)
        publisher.run()
    except KeyboardInterrupt:
        logger.info("Publisher stopped by user")
    except Exception as e:
        logger.error(f"Publisher failed to start: {e}")


if __name__ == "__main__":
    main()
