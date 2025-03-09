# DataSync Testing Documentation

This directory contains test files for the DataSync project's services. The tests are designed to verify the functionality of the data collection and publishing components.

## Test Structure

The test directory is organized as follows:

- `test_data_collector.py`: Tests for the DataCollector service, which handles collecting data from environmental sensors and storing it in CSV files.
- `test_publisher.py`: Tests for the CSVPublisher service, which processes CSV files to calculate hourly averages and publishes them to external endpoints.
- `test_winaqms_publisher.py`: Tests for the WinAQMSPublisher service, which processes WinAQMS .wad files and publishes calculated data to external endpoints.
- `run_tests.py`: A script that discovers and runs all tests, generating a basic test report.

## Running Tests

### Prerequisites

Ensure you have installed all the required dependencies:

```bash
pip install pytest pytest-mock pytest-asyncio pandas requests aiohttp
```

### Running All Tests

To run all tests at once, use the provided test runner script:

```bash
python tests/run_tests.py
```

This will discover and execute all test cases, and generate a basic test report.

### Running Individual Tests

To run specific test files:

```bash
# Run data collector tests
python -m unittest tests.test_data_collector

# Run publisher tests
python -m unittest tests.test_publisher

# Run WinAQMS publisher tests
python -m unittest tests.test_winaqms_publisher
```

You can also run specific test cases by specifying the test class and method:

```bash
python -m unittest tests.test_data_collector.TestDataCollector.test_collect_data
```

## Test Dependencies

The test suite has the following dependencies:

- `unittest`: Python's built-in testing framework
- `pytest`: Enhanced testing framework
- `pytest-mock`: Mocking library for pytest
- `pytest-asyncio`: For testing asynchronous code
- `pandas`: For data manipulation tests
- `requests-mock`: For mocking HTTP requests
- `aiohttp`: For testing asynchronous HTTP operations

## Adding New Tests

When adding new tests:

1. Follow the naming convention `test_*.py` for test files
2. Place the test file in the `tests` directory
3. Use appropriate mocking for external dependencies
4. Consider adding the new test to the relevant section in this README

## Mocking External Dependencies

The tests use mocking to simulate external dependencies:

- File operations are mocked to avoid actual file I/O
- HTTP requests are mocked to avoid actual network calls
- Sensor connections are simulated to avoid hardware dependencies

## Continuous Integration

These tests are designed to be run in a CI/CD pipeline. The test runner's output format is compatible with most CI systems for reporting test results.

