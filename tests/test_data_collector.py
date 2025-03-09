import unittest
import asyncio
import logging
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
from services.data_collector import DataCollector, Sensor, CollectorState
from typing import Dict


class MockSensor(Sensor):
    """Mock implementation of the Sensor abstract class for testing."""

    def __init__(self, mock_data=None):
        self.mock_data = mock_data or {"Temperature": 22.5, "Humidity": 45.0}

    async def read(self) -> Dict[str, float]:
        """Implement the abstract read method to return mock sensor data."""
        return self.mock_data


class TestDataCollector(unittest.TestCase):
    """Tests for the DataCollector class."""

    def setUp(self):
        """Set up test environment before each test."""
        self.logger = logging.getLogger("test_data_collector")
        self.logger.setLevel(logging.DEBUG)
        self.output_path = Path("/tmp/test_data")
        self.collector = DataCollector(self.output_path, self.logger)

        # Add timestamp to all columns
        self.collector.set_columns(["timestamp", "Temperature", "Humidity", "RainRate"])

        # Mock sensor configuration
        self.sensor_config = {
            "name": "test_sensor",
            "keys": ["Temperature", "Humidity"],
            "scan_interval": 0.1,
        }

        # Create a mock sensor
        self.sensor = MockSensor()

    @pytest.fixture
    async def collector_context(self):
        """Async fixture that provides a collector within context."""
        async with self.collector as collector:
            yield collector
        # Context will automatically exit after the test

    def test_initialization(self):
        """Test that DataCollector initializes with correct values."""
        self.assertEqual(self.collector.output_path, self.output_path)
        self.assertEqual(self.collector.state, CollectorState.RUNNING)
        self.assertEqual(
            self.collector.csv_columns,
            ["timestamp", "Temperature", "Humidity", "RainRate"],
        )
        self.assertEqual(len(self.collector.data_buffer), 0)
        self.assertEqual(len(self.collector.data_to_save), 0)

    @pytest.mark.asyncio
    @patch("services.data_collector.datetime")
    async def test_collect_data(self, mock_datetime):
        """Test that collect_data correctly collects and stores data."""
        # Mock datetime.now() to return a consistent timestamp
        mock_now = datetime(2023, 1, 1, 12, 30)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strftime = datetime.strftime
        mock_datetime.strptime = datetime.strptime

        # Configure the test to run collector for a bit then stop
        self.collector.state = CollectorState.RUNNING

        # Create a custom task for collection that we'll stop after a short time
        collector_task = asyncio.create_task(
            self.collector.collect_data(self.sensor, self.sensor_config)
        )

        # Let it run briefly
        await asyncio.sleep(0.3)

        # Change state to stop collection
        self.collector.state = CollectorState.STOPPING

        # Wait for the task to complete
        try:
            await asyncio.wait_for(collector_task, timeout=1.0)
        except asyncio.TimeoutError:
            collector_task.cancel()
            try:
                await collector_task
            except asyncio.CancelledError:
                pass

        # Check the buffer has entries
        async with self.collector.data_lock:
            timestamp_key = mock_now.strftime("%Y-%m-%d %H:%M")
            self.assertIn(timestamp_key, self.collector.data_buffer)
            buffer_entry = self.collector.data_buffer[timestamp_key]
            self.assertGreaterEqual(buffer_entry["count"], 1)
            self.assertIn("Temperature", buffer_entry["data"])
            self.assertIn("Humidity", buffer_entry["data"])

    @pytest.mark.asyncio
    @patch("services.data_collector.datetime")
    async def test_process_and_save_data(self, mock_datetime):
        """Test that process_and_save_data correctly processes buffer data."""
        # Set up a consistent timestamp
        mock_now = datetime(2023, 1, 1, 12, 30)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strftime = datetime.strftime
        mock_datetime.strptime = datetime.strptime

        # Add some test data to the buffer
        timestamp_key = "2023-01-01 12:29"  # One minute before now
        async with self.collector.data_lock:
            self.collector.data_buffer[timestamp_key] = {
                "data": {"Temperature": 22.5, "Humidity": 45.0, "RainRate": 0.25},
                "count": 1,
            }

        # Mock _save_batch_data to track calls
        self.collector._save_batch_data = AsyncMock()

        # Run process_and_save_data in a task
        process_task = asyncio.create_task(
            self.collector.process_and_save_data(output_interval=0.1, batch_size=1)
        )

        # Let it run briefly
        await asyncio.sleep(0.3)

        # Stop the task
        self.collector.state = CollectorState.STOPPING

        # Wait for the task to complete
        try:
            await asyncio.wait_for(process_task, timeout=1.0)
        except asyncio.TimeoutError:
            process_task.cancel()
            try:
                await process_task
            except asyncio.CancelledError:
                pass

        # Verify _save_batch_data was called with processed data
        self.collector._save_batch_data.assert_called_once()

        # Check the call arguments - verify that the rounding is done correctly
        # as per the implementation (1 decimal for Temperature and Humidity, 2 for RainRate)
        call_args = self.collector._save_batch_data.call_args[0][0]
        self.assertEqual(len(call_args), 1)
        self.assertEqual(call_args[0]["timestamp"], timestamp_key)
        self.assertEqual(call_args[0]["Temperature"], 22.5)  # 1 decimal place
        self.assertEqual(call_args[0]["Humidity"], 45.0)  # 1 decimal place
        self.assertEqual(call_args[0]["RainRate"], 0.25)  # 2 decimal places

    @pytest.mark.asyncio
    @patch("services.data_collector.pd.DataFrame")
    @patch("pathlib.Path.mkdir")
    @patch("pandas.DataFrame.to_csv")
    async def test_save_batch_data(self, mock_to_csv, mock_mkdir, mock_dataframe):
        """Test that _save_batch_data correctly saves data to CSV file."""
        # Prepare test data
        test_data = [
            {
                "timestamp": "2023-01-01 12:30",
                "Temperature": 22.5,
                "Humidity": 45.0,
                "RainRate": 0.25,
            }
        ]

        # Configure mocks
        mock_df = MagicMock()
        mock_dataframe.return_value = mock_df
        mock_df.__getitem__.return_value = mock_df
        mock_df.columns = ["timestamp", "Temperature", "Humidity", "RainRate"]

        # Call the method under test
        await self.collector._save_batch_data(test_data)

        # Verify pd.DataFrame was called with our test data
        mock_dataframe.assert_called_once_with(test_data)

        # Verify directory creation
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

        # Verify CSV creation with correct parameters
        mock_to_csv.assert_called_once()

        # Check that the output file path is constructed correctly based on the date
        self.assertEqual(mock_df.__getitem__.call_count, 1)
        self.assertEqual(
            mock_df.__getitem__.call_args[0][0], self.collector.csv_columns
        )

    @pytest.mark.asyncio
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.mkdir")
    @patch("pandas.DataFrame.to_csv")
    async def test_save_batch_data_file_exists(
        self, mock_to_csv, mock_mkdir, mock_exists
    ):
        """Test that _save_batch_data handles existing files correctly."""
        # Prepare test data
        test_data = [
            {
                "timestamp": "2023-01-01 12:30",
                "Temperature": 22.5,
                "Humidity": 45.0,
                "RainRate": 0.25,
            }
        ]

        # Configure mocks
        mock_exists.return_value = True  # File exists

        # Call the method under test
        with patch("pandas.DataFrame"):
            await self.collector._save_batch_data(test_data)

        # Verify directory creation
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

        # Verify CSV update with header=False (append mode)
        mock_to_csv.assert_called_once()
        self.assertEqual(mock_to_csv.call_args[1]["header"], False)
        self.assertEqual(mock_to_csv.call_args[1]["mode"], "a")

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test that the async context manager protocol works correctly."""
        async with self.collector as collector:
            self.assertEqual(collector.state, CollectorState.RUNNING)

        # After context exit
        self.assertEqual(self.collector.state, CollectorState.STOPPED)


if __name__ == "__main__":
    unittest.main()
