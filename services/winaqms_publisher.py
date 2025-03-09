"""
Module for publishing hourly averages from WinAQMS .wad files to an external endpoint.

This module defines a WinAQMSPublisher class that reads daily .wad data from the configured directory,
calculates hourly averages using the WinAQMS logic, and sends them to a specified API endpoint,
controlled by a control file.
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
import requests
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
        # Use None for clarity instead of relying on falsy value
        self.endpoint_url = endpoint_url if endpoint_url is not None else os.getenv("GOOGLE_POST_URL")
        if self.endpoint_url is None:
            raise ValueError(
                "Endpoint URL must be provided or set in .env as GOOGLE_POST_URL"
            )
        self.control_file = control_file
        self.check_interval = check_interval
        self.last_execution = None
        self.state = PublisherState.RUNNING
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
            return {"winaqms_publisher": "STOPPED", "publisher": "STOPPED"}

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

    def _read_wad_file(self, date: datetime) -> pd.DataFrame:
        """
        Read the WAD file for the given date.

        Args:
            date (datetime): The date to process.

        Returns:
            pd.DataFrame: DataFrame with the WAD data.

        Raises:
            FileNotFoundError: If the WAD file doesn't exist.
            Exception: For other read errors.
        """
        try:
            wad_path = self._get_wad_path(date)
            if not os.path.exists(wad_path):
                raise FileNotFoundError(f"WAD file not found: {wad_path}")

            # Read WAD file as CSV with pandas
            df = pd.read_csv(wad_path)

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
                return {"origen": "CENTENARIO", "data": data}

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
                df.groupby(df["Date_Time"].dt.floor("H"))
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
                return {"origen": "CENTENARIO", "data": data}

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

            result = {"origen": "CENTENARIO", "data": result_data}
            return result

        except Exception as e:
            self.logger.error(f"Error calculating hourly averages: {str(e)}")
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
        Run the publisher, checking both internal state and control file.
        Executes once per hour if in RUNNING state.
        """
        self.logger.info("Starting WinAQMS Publisher...")
        while self.state != PublisherState.STOPPED:
            try:
                # Check the control file to sync with external commands
                control = self._read_control_file()
                file_state = control.get("winaqms_publisher", "STOPPED").upper()

                # Sync internal state with control file if needed
                if (
                    file_state == "STOPPED"
                    and self.state != PublisherState.STOPPING
                    and self.state != PublisherState.STOPPED
                ):
                    self.logger.info("WinAQMS Publisher stopping due to control file.")
                    self.state = PublisherState.STOPPING
                elif file_state == "RUNNING" and self.state != PublisherState.RUNNING:
                    self.state = PublisherState.RUNNING
                    self.logger.info("WinAQMS Publisher resumed via control file.")

                # Verify the state and take action
                if self.state == PublisherState.STOPPING:
                    self.logger.info("WinAQMS Publisher stopping gracefully...")
                    self.state = PublisherState.STOPPED
                    break
                elif self.state == PublisherState.RUNNING:
                    # Check if an hour has passed since last execution
                    now = datetime.now()
                    if (
                        not self.last_execution
                        or (now - self.last_execution).total_seconds() >= 3600
                    ):
                        self.logger.info("Executing WinAQMS publish cycle...")

                        # Handle previous hour data
                        # Process the current day's data
                        process_date = now
                        date = process_date.date()
                        
                        self.logger.info(f"Processing data for {date} (all hours)")
                        
                        try:
                            # Read the WAD file for current day
                            df = self._read_wad_file(process_date)
                            
                            # Calculate hourly averages for all hours in the file
                            hourly_data = self._calculate_hourly_averages(df)
                            
                        except FileNotFoundError:
                            self.logger.warning(
                                f"WAD file not found for {process_date.strftime('%Y-%m-%d')}"
                            )
                            # Create empty data for the current day with hourly intervals
                            data = []
                            for hour in range(24):
                                timestamp = datetime.combine(date, datetime.min.time())
                                timestamp = timestamp + timedelta(hours=hour)
                                data.append({
                                    "timestamp": timestamp.strftime("%Y-%m-%d %H:00"),
                                    **{
                                        self.sensor_map[sensor]: None
                                        for sensor in self.sensors
                                    },
                                })
                            hourly_data = {"origen": "CENTENARIO", "data": data}
                        except Exception as e:
                            self.logger.error(f"Error in WinAQMS publish cycle: {e}")
                            # Skip to next interval if there's an error
                            time.sleep(self.check_interval)
                            continue
                            
                        # At this point, hourly_data should be defined either from file or empty template
                        # Send data to endpoint
                        try:
                            success = self._send_to_endpoint(hourly_data)
                            if success:
                                self.logger.info(
                                    f"Successfully published data for {date} (all hours)"
                                )
                            else:
                                self.logger.error(
                                    f"Failed to publish data for {date} (all hours)"
                                )
                        except Exception as e:
                            self.logger.error(f"Error sending data to endpoint: {e}")
                            success = False
                            
                        # Update last execution time regardless of success
                        self.last_execution = now

                    # Sleep until next check
                    time.sleep(self.check_interval)

            except Exception as e:
                self.logger.error(f"Error in WinAQMS Publisher run loop: {e}")
                time.sleep(self.check_interval)

        self.logger.info("WinAQMS Publisher stopped")


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
