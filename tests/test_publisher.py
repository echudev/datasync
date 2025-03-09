"""
Test module for publisher.py

This module contains unit tests for the CSVPublisher class
to ensure correct functionality for reading CSV files, calculating
hourly averages, and sending data to endpoints.
"""

import unittest
from unittest.mock import patch, mock_open, MagicMock
import json
import os
import pandas as pd

# Import the module to be tested
from services import CSVPublisher, PublisherState


class TestCSVPublisher(unittest.TestCase):
    """Test cases for the CSVPublisher class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Create a mock logger for testing
        self.mock_logger = MagicMock()

        # Mock environment variable for endpoint URL
        with patch.dict(os.environ, {"GOOGLE_POST_URL": "https://example.com/api"}):
            self.publisher = CSVPublisher(
                csv_dir="test_data",
                control_file="test_control.json",
                check_interval=1,
                logger=self.mock_logger,
            )

    def test_init(self):
        """Test proper initialization of the CSVPublisher class."""
        self.assertEqual(self.publisher.csv_dir, "test_data")
        self.assertEqual(self.publisher.endpoint_url, "https://example.com/api")
        self.assertEqual(self.publisher.control_file, "test_control.json")
        self.assertEqual(self.publisher.check_interval, 1)
        self.assertEqual(self.publisher.state, PublisherState.RUNNING)
        self.assertIsNone(self.publisher.last_execution)

    @patch(
        "builtins.open", new_callable=mock_open, read_data='{"publisher": "RUNNING"}'
    )
    def test_read_control_file_success(self, mock_file):
        """Test reading control file when file exists and is valid."""
        control = self.publisher._read_control_file()
        mock_file.assert_called_once_with("test_control.json", "r")
        self.assertEqual(control, {"publisher": "RUNNING"})

    @patch("builtins.open", side_effect=FileNotFoundError())
    def test_read_control_file_not_found(self, mock_file):
        """Test reading control file when file doesn't exist."""
        control = self.publisher._read_control_file()
        self.assertEqual(control, {"publisher": "STOPPED"})
        self.mock_logger.error.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data="invalid json")
    def test_read_control_file_invalid_json(self, mock_file):
        """Test reading control file when file contains invalid JSON."""
        control = self.publisher._read_control_file()
        self.assertEqual(control, {"publisher": "STOPPED"})
        self.mock_logger.error.assert_called_once()

    @patch("os.path.exists")
    @patch("os.path.join")
    @patch("pandas.read_csv")
    def test_read_csv_success(self, mock_read_csv, mock_join, mock_exists):
        """Test reading CSV file successfully."""
        # Setup mocks
        mock_exists.return_value = True
        mock_join.return_value = "test_data/2023/01/01.csv"
        mock_df = pd.DataFrame(
            {
                "timestamp": ["2023-01-01 00:00", "2023-01-01 00:01"],
                "Temperature": [20.5, 21.2],
                "Humidity": [45.0, 46.5],
            }
        )
        mock_read_csv.return_value = mock_df

        # Call the method
        result = self.publisher._read_csv("2023", "01", "01")

        # Assert results
        mock_join.assert_called_once_with("test_data", "2023", "01", "01.csv")
        mock_exists.assert_called_once_with("test_data/2023/01/01.csv")
        mock_read_csv.assert_called_once_with("test_data/2023/01/01.csv")
        pd.testing.assert_frame_equal(result, mock_df)

    @patch("os.path.exists")
    @patch("os.path.join")
    def test_read_csv_file_not_found(self, mock_join, mock_exists):
        """Test reading CSV file when file doesn't exist."""
        # Setup mocks
        mock_exists.return_value = False
        mock_join.return_value = "test_data/2023/01/01.csv"

        # Call the method and assert it raises exception
        with self.assertRaises(FileNotFoundError):
            self.publisher._read_csv("2023", "01", "01")

    def test_calculate_hourly_averages(self):
        """Test calculating hourly averages from DataFrame."""
        # Create test data
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2023-01-01 10:00",
                    "2023-01-01 10:15",
                    "2023-01-01 10:30",
                    "2023-01-01 10:45",
                    "2023-01-01 11:00",
                    "2023-01-01 11:15",
                    "2023-01-01 11:30",
                    "2023-01-01 11:45",
                ],
                "Temperature": [20.1, 20.3, 20.5, 20.7, 21.0, 21.2, 21.4, 21.6],
                "Humidity": [45.0, 45.5, 46.0, 46.5, 47.0, 47.5, 48.0, 48.5],
                "RainRate": [0.25, 0.30, 0.40, 0.50, 0.55, 0.45, 0.35, 0.25],
                "WindSpeed": [5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8],
                "WindDirection": [
                    180.0,
                    181.0,
                    182.0,
                    183.0,
                    184.0,
                    185.0,
                    186.0,
                    187.0,
                ],
                "Pressure": [
                    1011.1,
                    1011.2,
                    1011.3,
                    1011.4,
                    1011.5,
                    1011.6,
                    1011.7,
                    1011.8,
                ],
                "UV": [3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8],
                "SolarRadiation": [
                    500.1,
                    502.2,
                    504.3,
                    506.4,
                    508.5,
                    510.6,
                    512.7,
                    514.8,
                ],
            }
        )

        # Calculate hourly averages
        result = self.publisher._calculate_hourly_averages(df)

        # Assert on structure and values
        self.assertEqual(result["origen"], "CENTENARIO")
        self.assertEqual(len(result["data"]), 2)  # Two hours in the data

        # Check first hour averages
        hour_10 = next(
            item
            for item in result["data"]
            if item["timestamp"].startswith("2023-01-01 10:00")
        )
        self.assertEqual(hour_10["TEMP"], 20.4)  # Average of 20.1, 20.3, 20.5, 20.7
        self.assertEqual(hour_10["HR"], 45.8)  # Average of 45.0, 45.5, 46.0, 46.5
        self.assertEqual(hour_10["LLUVIA"], 0.36)  # Average with 2 decimal places

        # Check second hour averages
        hour_11 = next(
            item
            for item in result["data"]
            if item["timestamp"].startswith("2023-01-01 11:00")
        )
        self.assertEqual(hour_11["TEMP"], 21.3)  # Average of 21.0, 21.2, 21.4, 21.6
        self.assertEqual(hour_11["HR"], 47.8)  # Average of 47.0, 47.5, 48.0, 48.5
        self.assertEqual(hour_11["LLUVIA"], 0.4)  # Average with 2 decimal places

    def test_calculate_hourly_averages_missing_timestamp(self):
        """Test calculating hourly averages with missing timestamp column."""
        # Create invalid data (missing timestamp)
        df = pd.DataFrame({"Temperature": [20.1, 20.3], "Humidity": [45.0, 45.5]})

        # Assert it raises ValueError
        with self.assertRaises(ValueError):
            self.publisher._calculate_hourly_averages(df)

    def test_calculate_hourly_averages_invalid_timestamps(self):
        """Test calculating hourly averages with invalid timestamps."""
        # Create invalid data (bad timestamp format)
        df = pd.DataFrame(
            {
                "timestamp": ["invalid", "format"],
                "Temperature": [20.1, 20.3],
                "Humidity": [45.0, 45.5],
            }
        )

        # Assert it raises ValueError
        with self.assertRaises(ValueError):
            self.publisher._calculate_hourly_averages(df)

    @patch("requests.post")
    def test_send_to_endpoint_success(self, mock_post):
        """Test sending data to endpoint successfully."""
        # Setup mock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "Success"
        mock_post.return_value = mock_response

        # Test data
        data = {
            "origen": "CENTENARIO",
            "data": [{"timestamp": "2023-01-01 10:00:00", "TEMP": 20.4, "HR": 45.8}],
        }

        # Call the method
        result = self.publisher._send_to_endpoint(data)

        # Assert
        self.assertTrue(result)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "https://example.com/api")
        self.assertEqual(kwargs["headers"], {"Content-Type": "application/json"})
        self.assertEqual(json.loads(kwargs["data"]), data)

    @patch("requests.post")
    def test_send_to_endpoint_failure(self, mock_post):
        """Test sending data to endpoint with failure."""
        # Setup mock to raise an exception
        mock_post.side_effect = Exception("Connection error")

        # Test data
        data = {
            "origen": "CENTENARIO",
            "data": [{"timestamp": "2023-01-01 10:00:00", "TEMP": 20.4, "HR": 45.8}],
        }

        # Call the method
        result = self.publisher._send_to_endpoint(data)

        # Assert
        self.assertFalse(result)
        mock_post.assert_called_once()
        self.mock_logger.error.assert_called_once()

    @patch.object(CSVPublisher, "_read_control_file")
    @patch.object(CSVPublisher, "_read_csv")
    @patch.object(CSVPublisher, "_calculate_hourly_averages")
    @patch.object(CSVPublisher, "_send_to_endpoint")
    @patch("time.sleep")
    def test_run_execute_cycle(
        self, mock_sleep, mock_send, mock_calc, mock_read_csv, mock_read_control
    ):
        """Test the run method with a full execution cycle."""
        # Setup mocks
        mock_read_control.return_value = {"publisher": "RUNNING"}
        mock_df = pd.DataFrame(
            {
                "timestamp": ["2023-01-01 10:00", "2023-01-01 10:15"],
                "Temperature": [20.1, 20.3],
                "Humidity": [45.0, 45.5],
            }
        )
        mock_read_csv.return_value = mock_df
        mock_calc.return_value = {
            "origen": "CENTENARIO",
            "data": [{"timestamp": "2023-01-01 10:00:00"}],
        }
        mock_send.return_value = True

        # Make run exit after one iteration
        def side_effect(*args, **kwargs):
            self.publisher.state = PublisherState.STOPPED

        mock_sleep.side_effect = side_effect

        # Run the method
        self.publisher.run()

        # Assert
        mock_read_control.assert_called()
        mock_read_csv.assert_called_once()
        mock_calc.assert_called_once_with(mock_df)
        mock_send.assert_called_once()
        self.assertIsNotNone(self.publisher.last_execution)

    @patch.object(CSVPublisher, "_read_control_file")
    @patch("time.sleep")
    def test_run_stopping_from_control_file(self, mock_sleep, mock_read_control):
        """Test the run method stops when control file says STOPPED."""
        # Setup mocks
        mock_read_control.return_value = {"publisher": "STOPPED"}

        # Run the method
        self.publisher.run()

        # Assert
        mock_read_control.assert_called()
        self.assertEqual(self.publisher.state, PublisherState.STOPPED)


if __name__ == "__main__":
    unittest.main()
