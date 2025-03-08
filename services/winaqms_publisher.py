"""
Module for publishing hourly averages from WinAQMS .wad files to an external endpoint.

This module defines a WinAQMSPublisher class that reads daily .wad data from the configured directory,
calculates hourly averages using the WinAQMS logic, and sends them to a specified API endpoint,
controlled by a control file.
"""

import os
import csv
import json
import time
import logging
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any


class WinAQMSPublisher:
    """Class to handle publishing hourly WinAQMS data to an external endpoint."""

    def __init__(
        self,
        wad_dir: str = "C:\\Data",
        endpoint_url: str = None,
        control_file: str = "control.json",
        check_interval: int = 5,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the WinAQMSPublisher.

        Args:
            wad_dir (str): Directory containing the WAD files (default: "C:\Data").
            endpoint_url (str): URL of the API endpoint (loaded from env if None).
            control_file (str): Path to the control file (default: "control.json").
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
        self.control_file = control_file
        self.check_interval = check_interval
        self.last_execution = None
        self.logger = logger or logging.getLogger("winaqms_publisher")
        self._setup_logger()

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

    def _setup_logger(self) -> None:
        """Setup the logger if it's not already configured."""
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

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
            return {"winaqms_publisher": "STOPPED"}
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding control file {self.control_file}: {e}")
            return {"winaqms_publisher": "STOPPED"}

    def _get_wad_path(self, process_date: datetime) -> str:
        """
        Get the WAD file path for the given date.

        Args:
            process_date (datetime): The date to process.

        Returns:
            str: Path to the WAD file.
        """
        year_str = process_date.strftime("%Y")
        month_str = process_date.strftime("%m")
        day_str = process_date.strftime("%d")
        folder_path = os.path.join(self.wad_dir, year_str, month_str)
        wad_file = f"eco{year_str}{month_str}{day_str}.wad"
        return os.path.join(folder_path, wad_file)

    def _process_row_data(
        self, row_data: Dict[str, List[float]]
    ) -> Dict[str, Optional[float]]:
        """
        Process row data to calculate averages for each sensor.

        Args:
            row_data (Dict[str, List[float]]): Dictionary with sensor data.

        Returns:
            Dict[str, Optional[float]]: Dictionary with sensor averages.
        """
        averages = {}
        for sensor, values in row_data.items():
            if not values:
                averages[sensor] = None
                continue

            avg = sum(values) / len(values)
            if sensor in ("C1", "C2", "C3", "C4"):
                avg = round(avg, 3)
            elif sensor == "C6":
                avg = round(avg)
            averages[sensor] = avg
        return averages

    def _calculate_hourly_averages(self, date: datetime, hour: int) -> Dict[str, Any]:
        """
        Calculate hourly averages from the WAD file for the given date and hour.

        Args:
            date (datetime): Date to process.
            hour (int): Hour to process (0-23).

        Returns:
            Dict[str, Any]: Dictionary with hourly averages data formatted for API.

        Raises:
            Exception: If calculation fails.
        """
        wad_path = self._get_wad_path(date)

        if not os.path.exists(wad_path):
            self.logger.error(f"File not found: {wad_path}")
            raise FileNotFoundError(f"WAD file not found: {wad_path}")

        start_time = datetime.strptime(
            f"{date.strftime('%Y/%m/%d')} {hour:02d}:00:00",
            "%Y/%m/%d %H:%M:%S",
        )
        end_time = start_time + timedelta(hours=1)

        sensor_data = {sensor: [] for sensor in self.sensors}

        try:
            with open(wad_path, "r", newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    try:
                        dt = datetime.strptime(row["Date_Time"], "%Y/%m/%d %H:%M:%S")
                    except ValueError:
                        self.logger.warning(f"Invalid date format: {row['Date_Time']}")
                        continue

                    if start_time <= dt < end_time:
                        for sensor in self.sensors:
                            val_str = row.get(sensor, "").strip()
                            if val_str:
                                try:
                                    sensor_data[sensor].append(float(val_str))
                                except ValueError:
                                    self.logger.warning(
                                        f"Invalid value for {sensor}: {val_str}"
                                    )

            averages = self._process_row_data(sensor_data)

            # Format the output to match the expected API format
            data = [
                {
                    "timestamp": start_time.strftime("%Y-%m-%d %H:00"),
                    **{
                        self.sensor_map[sensor]: avg for sensor, avg in averages.items()
                    },
                }
            ]

            result = {"origen": "CENTENARIO", "data": data}
            return result

        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            raise

    def _send_to_endpoint(self, data: Dict[str, Any]) -> bool:
        """
        Send data to the external endpoint.

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
        Run the publisher, checking the control file to determine the state.
        Executes once per hour if in RUNNING state.
        """
        self.logger.info("Starting WinAQMS Publisher...")
        while True:
            try:
                # Read control file
                control = self._read_control_file()
                state = control.get("winaqms_publisher", "STOPPED").upper()

                # Check state
                if state == "STOPPED":
                    self.logger.info("Publisher stopped by control file.")
                    break  # Exit loop and terminate script
                elif state == "PAUSED":
                    self.logger.info("Publisher paused. Waiting for state change...")
                    time.sleep(self.check_interval)
                    continue
                elif state == "RUNNING":
                    # Check if an hour has passed since last execution
                    now = datetime.now()
                    if (
                        not self.last_execution
                        or (now - self.last_execution).total_seconds() >= 3600
                    ):
                        self.logger.info("Executing publish cycle...")

                        # Handle hour 0 for previous day
                        if now.hour == 0:
                            process_date = now - timedelta(days=1)
                            hour_to_process = 23
                        else:
                            process_date = now
                            hour_to_process = now.hour - 1

                        try:
                            # Calculate hourly averages
                            hourly_data = self._calculate_hourly_averages(
                                process_date, hour_to_process
                            )

                            # Send to API
                            success = self._send_to_endpoint(hourly_data)
                            if not success:
                                self.logger.info("Failed to send data, continuing...")
                        except FileNotFoundError as e:
                            self.logger.info(f"{e}, continuing...")
                        except Exception as e:
                            self.logger.error(f"Error calculating hourly averages: {e}")

                        # Update last execution time
                        self.last_execution = now
                    else:
                        time.sleep(self.check_interval)
                else:
                    self.logger.info(
                        f"Unknown state for publisher: {state}. Defaulting to PAUSED."
                    )
                    time.sleep(self.check_interval)
                    continue

                # Wait before checking control file again
                time.sleep(self.check_interval)

            except Exception as e:
                self.logger.error(f"Error in publisher run loop: {e}")
                time.sleep(self.check_interval)


def main():
    """Main function to start the publisher."""
    try:
        logger = logging.getLogger("data_collection")  # Use shared logger
        publisher = WinAQMSPublisher(logger=logger)
        publisher.run()
    except KeyboardInterrupt:
        logger.info("WinAQMS Publisher stopped by user")
    except Exception as e:
        logger.error(f"WinAQMS Publisher failed to start: {e}")


if __name__ == "__main__":
    main()
