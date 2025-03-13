import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import pandas as pd
import os
from datetime import datetime
from aiohttp.client_exceptions import ClientConnectorError

# Import the module to be tested
from services import WinAQMSPublisher, PublisherState


class TestWinAQMSPublisher(unittest.TestCase):
    def setUp(self):
        # Create a mock logger for testing
        self.mock_logger = MagicMock()
        # Ensure the endpoint URL is set (or provided via .env)
        with patch.dict(os.environ, {"GOOGLE_POST_URL": "https://example.com/api"}):
            self.publisher = WinAQMSPublisher(
                wad_dir="test_data",
                endpoint_url="https://example.com/api",
                check_interval=1,
                logger=self.mock_logger,
            )

    def test_init_without_endpoint(self):
        """Test initialization fails when no endpoint URL is provided."""
        with patch.dict("os.environ", {}, clear=True):
            with patch("services.winaqms_publisher.load_dotenv", return_value=None):
                with self.assertRaises(ValueError):
                    WinAQMSPublisher(
                        wad_dir="test_data",
                        endpoint_url=None,
                        check_interval=1,
                        logger=self.mock_logger,
                    )

    async def test_update_state(self):
        # Test that state updates correctly with different case inputs
        self.publisher.state = PublisherState.RUNNING
        await self.publisher.update_state("stopped")
        self.assertEqual(self.publisher.state, PublisherState.STOPPED)
        await self.publisher.update_state("RUNNING")
        self.assertEqual(self.publisher.state, PublisherState.RUNNING)

    @patch("os.path.exists")
    @patch("os.path.join")
    @patch("pandas.read_csv")
    async def test_read_wad_file_success(self, mock_read_csv, mock_join, mock_exists):
        """Test successful WAD file reading."""
        # Setup
        date = datetime(2023, 1, 1)
        mock_exists.return_value = True
        mock_join.return_value = "test_data/2023/01/eco20230101.wad"
        df_sample = pd.DataFrame(
            {
                "Date_Time": ["2023/01/01 00:00:00", "2023/01/01 00:01:00"],
                "C1": [10, 20],
            }
        )
        mock_read_csv.return_value = df_sample

        # Execute
        result = await self.publisher._read_wad_file(
            year=date.strftime("%Y"), month=date.strftime("%m"), day=date.strftime("%d")
        )

        # Assert results
        calls = [
            call
            for call in mock_join.mock_calls
            if call.args == ("test_data", "2023", "01", "eco20230101.wad")
        ]
        self.assertEqual(
            len(calls),
            1,
            f"Expected 'join' to be called once with correct args. Calls: {mock_join.mock_calls}",
        )
        mock_exists.assert_called_once_with("test_data/2023/01/01.csv")
        mock_read_csv.assert_called_once_with("test_data/2023/01/01.csv")
        pd.testing.assert_frame_equal(result, df_sample)

    @patch("os.path.exists")
    @patch("os.path.join")
    async def test_read_wad_file_not_found(self, mock_join, mock_exists):
        # Simulate file not found scenario
        mock_exists.return_value = False
        mock_join.return_value = "test_data/2023/01/01.csv"
        with self.assertRaises(FileNotFoundError):
            await self.publisher._read_wad_file("2023", "01", "01")

    def test_calculate_hourly_averages_empty_df(self):
        # For an empty dataframe, the method should return a default structure.
        empty_df = pd.DataFrame()
        result = self.publisher._calculate_hourly_averages(empty_df)
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(
            result["data"][0]["timestamp"], datetime.now().strftime("%Y-%m-%d %H:00")
        )
        for sensor in self.publisher.sensor_map.values():
            self.assertEqual(result["data"][0][sensor], None)

    def test_calculate_hourly_averages_success(self):
        data = {
            "Date_Time": [
                "2023/01/01 10:00:00",
                "2023/01/01 10:30:00",
                "2023/01/01 11:00:00",
                "2023/01/01 11:30:00",
            ],
            "C1": [10, 20, 30, 40],
            "C2": [5, 15, 25, 35],
        }
        df = pd.DataFrame(data)
        df["Date_Time"] = pd.to_datetime(df["Date_Time"], errors="coerce")

        result = self.publisher._calculate_hourly_averages(df)

        self.assertEqual(len(result["data"]), 2)
        first_entry = result["data"][0]
        self.assertTrue(first_entry["timestamp"].startswith("2023-01-01 10:00"))
        self.assertAlmostEqual(first_entry["CO"], 15.0)  # Average of 10 and 20
        self.assertAlmostEqual(first_entry["NO"], 10.0)  # Average of 5 and 15

    def test_calculate_hourly_averages_missing_date_time(self):
        # Missing 'Date_Time' column should trigger a ValueError.
        df = pd.DataFrame({"C1": [10, 20]})
        with self.assertRaises(ValueError):
            self.publisher._calculate_hourly_averages(df)

    def test_calculate_hourly_averages_invalid_date_time(self):
        df = pd.DataFrame({"Date_Time": ["invalid", "bad"], "C1": [10, 20]})
        df["Date_Time"] = pd.to_datetime(df["Date_Time"], errors="coerce")
        result = self.publisher._calculate_hourly_averages(df)
        self.assertTrue(pd.isna(result["data"][0]["CO"]))  # Check specific value

    @patch("aiohttp.ClientSession.post", new_callable=AsyncMock)
    async def test_send_to_endpoint_success(self, mock_post):
        """Test sending data to endpoint successfully."""
        # Setup mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(return_value="Success")
        mock_response.raise_for_status = AsyncMock(
            return_value=None
        )  # Simula éxito sin excepciones

        # Configurar el mock para el contexto asíncrono
        mock_post.return_value = mock_response

        # Test data
        data = {
            "origen": "CENTENARIO",
            "data": [{"timestamp": "2023-01-01 10:00:00", "TEMP": 20.4, "HR": 45.8}],
        }

        # Call the method
        result = await self.publisher._send_to_endpoint(data)

        # Assert
        self.assertTrue(result)
        mock_post.assert_called_once_with(
            "https://example.com/api",
            headers={"Content-Type": "application/json"},
            json=data,
        )
        self.mock_logger.info.assert_called_once_with("Data sent successfully")

    @patch("aiohttp.ClientSession.post")
    async def test_send_to_endpoint_failure(self, mock_post):
        """Test handling of endpoint connection failure."""
        # Setup
        mock_post.side_effect = ClientConnectorError(MagicMock(), OSError("Timeout"))
        data = {
            "apiKey": "test-key",
            "origen": "CENTENARIO",
            "data": [{"timestamp": "2023-01-01 10:00", "CO": 10}],
        }

        # Execute
        result = await self.publisher._send_to_endpoint(data)

        # Assert
        self.assertFalse(result)
        self.mock_logger.error.assert_called_once()

    @patch.object(WinAQMSPublisher, "_read_wad_file")
    @patch.object(WinAQMSPublisher, "_calculate_hourly_averages")
    @patch.object(WinAQMSPublisher, "_send_to_endpoint")
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_run_loop_execute_cycle(
        self, mock_sleep, mock_send, mock_calc, mock_read
    ):
        # Setup the mocks for one execution cycle.
        df_sample = pd.DataFrame(
            {
                "Date_Time": ["2023/01/01 10:00:00", "2023/01/01 10:15:00"],
                "C1": [10, 20],
            }
        )
        mock_read.return_value = df_sample
        mock_calc.return_value = {
            "origin": "CENTENARIO",
            "data": [{"timestamp": "2023-01-01 10:00", "CO": 15.0}],
        }
        mock_send.return_value = True

        # To exit the run loop after one cycle, set the publisher state to STOPPED in sleep.
        def exit_loop(*args, **kwargs):
            self.publisher.state = PublisherState.STOPPED

        mock_sleep.side_effect = exit_loop
        await self.publisher.run()
        mock_read.assert_called_once()
        mock_calc.assert_called_once_with(df_sample)
        mock_send.assert_called_once()
        self.assertIsNotNone(self.publisher.last_execution)


if __name__ == "__main__":
    unittest.main()
