import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import os
import pandas as pd
from aiohttp import ClientResponse
from aiohttp.client_exceptions import ClientConnectorError

# Import the module to be tested
from services import CSVPublisher, PublisherState


class TestCSVPublisher(unittest.IsolatedAsyncioTestCase):
    """Test cases for the CSVPublisher class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Create a mock logger for testing
        self.mock_logger = MagicMock()

        # Mock environment variables
        with patch.dict(
            os.environ,
            {
                "GOOGLE_POST_URL": "https://example.com/api",
                "ORIGEN": "CENTENARIO",
                "API_KEY": "test-key",
            },
        ):
            self.publisher = CSVPublisher(
                csv_dir="test_data",
                endpoint_url="https://example.com/api",
                check_interval=1,
                logger=self.mock_logger,
            )

    def test_init(self):
        """Test proper initialization of the CSVPublisher class."""
        self.assertEqual(self.publisher.csv_dir, "test_data")
        self.assertEqual(self.publisher.endpoint_url, "https://example.com/api")
        self.assertEqual(self.publisher.check_interval, 1)
        self.assertEqual(self.publisher.state, PublisherState.RUNNING)
        self.assertIsNone(self.publisher.last_execution)

    async def test_update_state_stopped(self):
        """Test updating state to STOPPED."""
        await self.publisher.update_state("STOPPED")
        self.assertEqual(self.publisher.state, PublisherState.STOPPED)

    async def test_update_state_running(self):
        """Test updating state to RUNNING."""
        # First set to STOPPED to verify the change
        self.publisher.state = PublisherState.STOPPED
        await self.publisher.update_state("RUNNING")
        self.assertEqual(self.publisher.state, PublisherState.RUNNING)

    async def test_update_state_case_insensitive(self):
        """Test updating state with lowercase input."""
        # First set to STOPPED to verify the change
        self.publisher.state = PublisherState.STOPPED
        await self.publisher.update_state("running")
        self.assertEqual(self.publisher.state, PublisherState.RUNNING)

    @patch("os.path.exists")
    @patch("os.path.join")
    @patch("pandas.read_csv")
    async def test_read_csv_success(self, mock_read_csv, mock_join, mock_exists):
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
        result = await self.publisher._read_csv("2023", "01", "01")

        # Assert results
        calls = [
            call
            for call in mock_join.mock_calls
            if call.args == ("test_data", "2023", "01", "01.csv")
        ]
        self.assertEqual(
            len(calls),
            1,
            f"Expected 'join' to be called once with correct args. Calls: {mock_join.mock_calls}",
        )
        mock_exists.assert_called_once_with("test_data/2023/01/01.csv")
        mock_read_csv.assert_called_once_with("test_data/2023/01/01.csv")
        pd.testing.assert_frame_equal(result, mock_df)

    @patch("os.path.exists")
    @patch("os.path.join")
    async def test_read_csv_file_not_found(self, mock_join, mock_exists):
        """Test reading CSV file when file doesn't exist."""
        # Setup mocks
        mock_exists.return_value = False
        mock_join.return_value = "test_data/2023/01/01.csv"

        # Call the method and assert it raises exception
        with self.assertRaises(FileNotFoundError):
            await self.publisher._read_csv("2023", "01", "01")

    def test_build_csv_path(self):
        """Test building the CSV file path."""
        result = self.publisher._build_csv_path("2023", "01", "01")
        self.assertEqual(result, "test_data/2023/01/01.csv")

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
            if item["timestamp"].startswith("2023-01-01 10:00:00")
        )

        # Verify hour 10 values
        self.assertEqual(hour_10["timestamp"], "2023-01-01 10:00:00")
        self.assertEqual(hour_10["TEMP"], 20.4)
        self.assertEqual(hour_10["HR"], 45.8)
        self.assertEqual(hour_10["LLUVIA"], 0.36)
        self.assertEqual(hour_10["VV"], 5.2)
        self.assertEqual(hour_10["DV"], 181.5)
        self.assertEqual(hour_10["PA"], 1011.2)
        self.assertEqual(hour_10["UV"], 3.2)
        self.assertEqual(hour_10["RS"], 503.2)

        # Check second hour averages
        hour_11 = next(
            item
            for item in result["data"]
            if item["timestamp"].startswith("2023-01-01 11:00:00")
        )

        # Verify hour 11 values
        self.assertEqual(hour_11["timestamp"], "2023-01-01 11:00:00")
        self.assertEqual(hour_11["TEMP"], 21.3)
        self.assertEqual(hour_11["HR"], 47.8)
        self.assertEqual(hour_11["LLUVIA"], 0.4)
        self.assertEqual(hour_11["VV"], 5.7)
        self.assertEqual(hour_11["DV"], 185.5)
        self.assertEqual(hour_11["PA"], 1011.6)
        self.assertEqual(hour_11["UV"], 3.6)
        self.assertEqual(hour_11["RS"], 511.6)

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

    @patch("aiohttp.ClientSession.post")
    async def test_send_to_endpoint_success(self, mock_post):
        """Test sending data to endpoint successfully."""
        # Mock the response
        mock_response = AsyncMock(spec=ClientResponse)
        mock_response.status = 200
        mock_response.text.return_value = AsyncMock(return_value="Success")
        mock_response.raise_for_status = AsyncMock(
            return_value=None
        )  # Simula éxito sin excepciones

        # Configurar el mock para el contexto asíncrono
        mock_post.return_value = mock_response

        # Test data
        data = {
            "apiKey": "test-key",
            "origen": "CENTENARIO",
            "data": [{"timestamp": "2023-01-01 10:00:00", "TEMP": 20.4, "HR": 45.8}],
        }

        # Call the method
        result = await self.publisher._send_to_endpoint(data)

        # Assert
        self.assertTrue(result)
        self.mock_logger.info.assert_called()

    @patch("aiohttp.ClientSession.post")
    async def test_send_to_endpoint_failure(self, mock_post):
        """Test sending data to endpoint with failure."""
        # Mock the ClientSession.post to raise an exception
        mock_post.side_effect = ClientConnectorError(MagicMock(), OSError("Timeout"))

        # Test data
        data = {
            "apiKey": "test-key",
            "origen": "CENTENARIO",
            "data": [{"timestamp": "2023-01-01 10:00:00", "TEMP": 20.4, "HR": 45.8}],
        }

        # Call the method
        result = await self.publisher._send_to_endpoint(data)

        # Assert
        self.assertFalse(result)
        self.mock_logger.error.assert_called()

    @patch.object(CSVPublisher, "_read_csv", new_callable=AsyncMock)
    @patch.object(CSVPublisher, "_calculate_hourly_averages")
    @patch.object(CSVPublisher, "_send_to_endpoint", new_callable=AsyncMock)
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_run_execute_cycle(
        self, mock_sleep, mock_send, mock_calc, mock_read_csv
    ):
        """Test the run method with a full execution cycle."""
        # Setup mocks
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
        async def side_effect(*args, **kwargs):
            self.publisher.state = PublisherState.STOPPED

        mock_sleep.side_effect = side_effect

        # Run the method
        await self.publisher.run()

        # Assert
        mock_read_csv.assert_called_once()
        mock_calc.assert_called_once_with(mock_df)
        mock_send.assert_called_once()
        self.assertIsNotNone(self.publisher.last_execution)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_run_stopping(self, mock_sleep):
        """Test the run method stops when state is set to STOPPED."""

        # Setup mock to exit immediately
        async def side_effect(*args, **kwargs):
            # Set state to STOPPED to exit the loop
            self.publisher.state = PublisherState.STOPPED

        mock_sleep.side_effect = side_effect

        # Initial state is RUNNING
        self.assertEqual(self.publisher.state, PublisherState.RUNNING)

        # Run the method
        await self.publisher.run()

        # Assert
        self.assertEqual(self.publisher.state, PublisherState.STOPPED)


if __name__ == "__main__":
    unittest.main()
