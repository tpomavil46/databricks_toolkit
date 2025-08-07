#!/usr/bin/env python3
"""
Integration Test Runner

This script runs the comprehensive integration test suite for the Databricks Toolkit.
"""

import os
import sys
import unittest
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_integration_tests():
    """Run all integration tests."""
    # Discover and run tests
    loader = unittest.TestLoader()
    start_dir = Path(__file__).parent / "integration"
    suite = loader.discover(start_dir, pattern="test_*.py")

    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


def run_specific_test(test_name):
    """Run a specific test by name."""
    loader = unittest.TestLoader()
    start_dir = Path(__file__).parent / "integration"
    suite = loader.discover(start_dir, pattern="test_*.py")

    # Filter to specific test
    filtered_suite = unittest.TestSuite()
    for test_suite in suite:
        for test_case in test_suite:
            if test_name in str(test_case):
                filtered_suite.addTest(test_case)

    if not filtered_suite.countTestCases():
        print(f"❌ No tests found matching '{test_name}'")
        return False

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(filtered_suite)

    return result.wasSuccessful()


def main():
    """Main entry point for the test runner."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run integration tests for Databricks Toolkit"
    )
    parser.add_argument("--test", "-t", help="Run a specific test by name")
    parser.add_argument(
        "--list", "-l", action="store_true", help="List all available tests"
    )

    args = parser.parse_args()

    if args.list:
        # List all available tests
        loader = unittest.TestLoader()
        start_dir = Path(__file__).parent / "integration"
        suite = loader.discover(start_dir, pattern="test_*.py")

        print("📋 Available Integration Tests:")
        print("=" * 50)

        for test_suite in suite:
            for test_case in test_suite:
                print(f"  • {test_case}")

        return True

    if args.test:
        # Run specific test
        success = run_specific_test(args.test)
    else:
        # Run all tests
        success = run_integration_tests()

    if success:
        print("\n✅ All integration tests passed!")
        return 0
    else:
        print("\n❌ Some integration tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
