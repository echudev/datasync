#!/usr/bin/env python3
"""
Test Runner Script for DataSync Project

This script discovers and runs all tests in the project, providing a comprehensive
test report. It includes proper setup and teardown procedures.
"""

import os
import sys
import time
import datetime
from unittest import TextTestRunner, TestLoader
from os.path import dirname, abspath, join


class CustomTextTestRunner(TextTestRunner):
    """Custom test runner that provides additional reporting capabilities."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = None
        self.end_time = None
        self.success_count = 0
        self.failure_count = 0
        self.error_count = 0
        self.skipped_count = 0

    def run(self, test):
        """Run the test suite with timing and result tracking."""
        self.start_time = time.time()
        result = super().run(test)
        self.end_time = time.time()

        self.success_count = (
            len(result.successes)
            if hasattr(result, "successes")
            else result.testsRun
            - len(result.failures)
            - len(result.errors)
            - len(result.skipped)
        )
        self.failure_count = len(result.failures)
        self.error_count = len(result.errors)
        self.skipped_count = len(result.skipped)

        return result


def setup_environment():
    """Setup any necessary environment before running tests."""
    print("Setting up test environment...")

    # Add the parent directory to sys.path to make imports work
    parent_dir = dirname(dirname(abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    # Create any necessary test directories or resources
    test_data_dir = join(dirname(abspath(__file__)), "test_data")
    if not os.path.exists(test_data_dir):
        os.makedirs(test_data_dir)

    print("Test environment setup complete.")


def teardown_environment():
    """Clean up after all tests have run."""
    print("Tearing down test environment...")

    # Clean up any test artifacts or resources here
    # For example, delete temporary files created during tests

    print("Test environment teardown complete.")


def generate_report(runner, test_result):
    """Generate a formatted test report."""
    duration = runner.end_time - runner.start_time

    print("\n" + "=" * 80)
    print("TEST EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Run Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration:.2f} seconds")
    print(
        f"Total Tests: {runner.success_count + runner.failure_count + runner.error_count + runner.skipped_count}"
    )
    print(f"Successes: {runner.success_count}")
    print(f"Failures: {runner.failure_count}")
    print(f"Errors: {runner.error_count}")
    print(f"Skipped: {runner.skipped_count}")

    if runner.failure_count > 0 or runner.error_count > 0:
        print("\nFAILURE DETAILS:")
        print("-" * 80)

        if runner.failure_count > 0:
            print("\nFAILURES:")
            for i, (test, traceback) in enumerate(test_result.failures, 1):
                print(f"\n{i}. {test}")
                print(f"{traceback}")

        if runner.error_count > 0:
            print("\nERRORS:")
            for i, (test, traceback) in enumerate(test_result.errors, 1):
                print(f"\n{i}. {test}")
                print(f"{traceback}")

    print("=" * 80)

    # Optionally save the report to a file
    with open(join(dirname(abspath(__file__)), "test_report.txt"), "w") as f:
        f.write("TEST EXECUTION SUMMARY\n")
        f.write(f"Run Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Duration: {duration:.2f} seconds\n")
        f.write(
            f"Total Tests: {runner.success_count + runner.failure_count + runner.error_count + runner.skipped_count}\n"
        )
        f.write(f"Successes: {runner.success_count}\n")
        f.write(f"Failures: {runner.failure_count}\n")
        f.write(f"Errors: {runner.error_count}\n")
        f.write(f"Skipped: {runner.skipped_count}\n")


def run_tests():
    """Discover and run all tests in the project."""
    # Setup the test environment
    setup_environment()

    try:
        # Discover all tests in the tests directory
        start_dir = dirname(abspath(__file__))
        test_loader = TestLoader()
        test_suite = test_loader.discover(start_dir, pattern="test_*.py")

        # Run the tests with a custom runner
        runner = CustomTextTestRunner(verbosity=2)
        test_result = runner.run(test_suite)

        # Generate and print the test report
        generate_report(runner, test_result)

        # Return non-zero exit code if there were test failures or errors
        if test_result.failures or test_result.errors:
            return 1
        return 0
    finally:
        # Always tear down the test environment, even if tests fail
        teardown_environment()


if __name__ == "__main__":
    sys.exit(run_tests())
