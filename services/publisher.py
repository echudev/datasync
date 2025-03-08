"""
Module for publishing hourly averages from a CSV file to an external endpoint.

This module defines a CSVPublisher class that reads daily CSV data from the 'data' directory,
calculates hourly averages, and sends them to a specified API endpoint, controlled by a control file.
"""

import logging
import os
import json
import time
from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
from typing import Dict, Any

# Crear la carpeta 'logs' si no existe
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging al inicio del script
logging.basicConfig(
    level=logging.DEBUG,  # Mantener DEBUG para depuración
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(log_dir, "publisher.log")),
    ],
)
logger = logging.getLogger("publisher")
logger.info(
    "Initializing publisher script..."
)  # Log inicial para confirmar que el script inicia


class CSVPublisher:
    """Class to handle publishing hourly CSV data to an external endpoint."""

    def __init__(
        self,
        csv_dir: str = "data",
        endpoint_url: str = None,
        control_file: str = "control.json",
        check_interval: int = 5,  # Intervalo para verificar el archivo de control (segundos)
    ):
        """
        Initialize the CSVPublisher.

        Args:
            csv_dir (str): Directory containing the CSV files (default: "data").
            endpoint_url (str): URL of the API endpoint (loaded from env if None).
            control_file (str): Path to the control file (default: "control.json").
            check_interval (int): Interval in seconds to check the control file (default: 5).
        """
        logger.info("Creating CSVPublisher instance...")
        load_dotenv()
        self.csv_dir = csv_dir
        self.endpoint_url = endpoint_url or os.getenv("GOOGLE_POST_URL")
        if not self.endpoint_url:
            logger.error("GOOGLE_POST_URL not found in .env or as argument.")
            raise ValueError(
                "Endpoint URL must be provided or set in .env as GOOGLE_POST_URL"
            )
        logger.info(f"Using endpoint URL: {self.endpoint_url}")
        self.control_file = control_file
        self.check_interval = check_interval
        self.last_execution = None  # Para rastrear la última ejecución

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
            logger.debug(f"Read control file: {control}")
            return control
        except FileNotFoundError:
            logger.error(
                f"Control file {self.control_file} not found. Defaulting to STOPPED."
            )
            return {"publisher": "STOPPED"}
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding control file {self.control_file}: {e}")
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
            logger.info(f"Reading CSV file: {csv_path}")
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"CSV file not found: {csv_path}")
            df = pd.read_csv(csv_path)
            logger.info(f"Successfully read CSV file with {len(df)} rows")
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

        Raises:
            Exception: If calculation fails.
        """
        try:
            logger.info("Calculating hourly averages...")
            # Verificar y convertir timestamp sin sobrescribir directamente
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
            logger.info(
                f"Hourly averages calculated successfully. Number of averages: {len(new_data)}"
            )
            logger.debug(f"Calculated averages: {json.dumps(result, indent=2)}")
            return result
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
            logger.info(f"Sending data to endpoint: {self.endpoint_url}")
            response = requests.post(
                self.endpoint_url,
                headers={"Content-Type": "application/json"},
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

    def run(self) -> None:
        """
        Run the publisher, checking the control file to determine the state.
        Executes once per hour if in RUNNING state.
        """
        logger.info("Starting publisher...")
        while True:
            try:
                # Leer el archivo de control
                control = self._read_control_file()
                state = control.get("publisher", "STOPPED").upper()

                # Verificar el estado
                if state == "STOPPED":
                    logger.info("Publisher stopped by control file.")
                    break  # Salir del bucle y terminar el script
                elif state == "PAUSED":
                    logger.info("Publisher paused. Waiting for state change...")
                    time.sleep(self.check_interval)
                    continue
                elif state == "RUNNING":
                    # Verificar si ha pasado una hora desde la última ejecución
                    now = datetime.now()
                    if (
                        not self.last_execution
                        or (now - self.last_execution).total_seconds() >= 3600
                    ):
                        logger.info("Executing publish cycle...")
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
                            logger.warning("Failed to send data, continuing...")

                        # Actualizar la última ejecución
                        self.last_execution = now
                    else:
                        logger.debug("Not yet time for next execution. Waiting...")
                else:
                    logger.warning(
                        f"Unknown state for publisher in control file: {state}. Defaulting to PAUSED."
                    )
                    time.sleep(self.check_interval)
                    continue

                # Esperar antes de verificar el archivo de control nuevamente
                time.sleep(self.check_interval)

            except Exception as e:
                logger.error(f"Error in publisher run loop: {e}")
                time.sleep(self.check_interval)


def main():
    """Main function to start the publisher."""
    try:
        logger.info("Starting main function...")
        publisher = CSVPublisher()
        publisher.run()
    except KeyboardInterrupt:
        logger.info("Publisher stopped by user")
    except Exception as e:
        logger.error(f"Publisher failed to start: {e}")


if __name__ == "__main__":
    main()
