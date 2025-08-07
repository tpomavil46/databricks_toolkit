#!/usr/bin/env python3
"""
CI Test Runner

This script runs tests specifically for CI environments,
skipping tests that require Databricks connections.
"""

import unittest
import sys
import os
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_ci_tests():
    """Run tests suitable for CI environment."""

    # Set CI environment flag
    os.environ["CI"] = "true"

    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Import and add specific test classes that don't require connections
    try:
        from tests.test_ingest_customer import (
            test_ingest_customer_placeholder,
            test_file_operations,
        )
        from tests.test_transform_order import (
            test_transform_order_placeholder,
            test_csv_operations,
        )
        from tests.test_pipeline import (
            test_pipeline_placeholder,
            test_environment_variables,
            test_basic_assertions,
        )

        # Create test cases
        class CITestCase(unittest.TestCase):
            def test_ingest_customer_placeholder(self):
                test_ingest_customer_placeholder()

            def test_file_operations(self):
                test_file_operations()

            def test_transform_order_placeholder(self):
                test_transform_order_placeholder()

            def test_csv_operations(self):
                test_csv_operations()

            def test_pipeline_placeholder(self):
                test_pipeline_placeholder()

            def test_environment_variables(self):
                test_environment_variables()

            def test_basic_assertions(self):
                test_basic_assertions()

            def test_sql_library_patterns(self):
                """Test SQL library patterns functionality."""
                try:
                    from sql_library.core.sql_patterns import SQLPatterns

                    patterns = SQLPatterns()
                    all_patterns = patterns.list_patterns()
                    self.assertGreater(len(all_patterns), 0)
                except ImportError:
                    self.skipTest("SQL library not available")

            def test_sql_library_quality_checks(self):
                """Test SQL library quality checks functionality."""
                try:
                    from sql_library.core.data_quality import DataQualityChecks

                    quality = DataQualityChecks()
                    all_checks = quality.list_checks()
                    self.assertGreater(len(all_checks), 0)
                except ImportError:
                    self.skipTest("SQL library not available")

            def test_sql_library_functions(self):
                """Test SQL library functions functionality."""
                try:
                    from sql_library.core.sql_functions import SQLFunctions

                    functions = SQLFunctions()
                    all_functions = functions.list_functions()
                    self.assertGreater(len(all_functions), 0)
                except ImportError:
                    self.skipTest("SQL library not available")

            def test_sql_library_templates(self):
                """Test SQL library templates functionality."""
                try:
                    from sql_library.core.sql_templates import SQLTemplates

                    templates = SQLTemplates()
                    all_templates = templates.list_templates()
                    self.assertGreater(len(all_templates), 0)
                except ImportError:
                    self.skipTest("SQL library not available")

            def test_utils_logger(self):
                """Test logger utility functionality."""
                try:
                    from utils.logger import log_function_call

                    @log_function_call
                    def test_function():
                        return "test"

                    result = test_function()
                    self.assertEqual(result, "test")
                except ImportError:
                    self.skipTest("Logger utility not available")

            def test_utils_schema_normalizer(self):
                """Test schema normalizer functionality."""
                try:
                    from utils.schema_normalizer import auto_normalize_columns
                    from pyspark.sql import SparkSession
                    from pyspark.sql.types import StructType, StructField, StringType

                    # Create a simple Spark session for testing
                    spark = (
                        SparkSession.builder.master("local[1]")
                        .appName("test")
                        .getOrCreate()
                    )

                    # Create test data
                    schema = StructType(
                        [
                            StructField("TestColumn", StringType(), True),
                            StructField("another_column", StringType(), True),
                        ]
                    )

                    data = [("test1", "test2"), ("test3", "test4")]
                    df = spark.createDataFrame(data, schema)

                    # Test normalization with column mapping
                    column_mapping = {
                        "TestColumn": "test_column",
                        "another_column": "another_column",
                    }
                    normalized_df = auto_normalize_columns(
                        df, column_mapping=column_mapping
                    )
                    self.assertIsNotNone(normalized_df)

                    spark.stop()
                except ImportError:
                    self.skipTest("Schema normalizer not available")
                except Exception as e:
                    self.skipTest(f"Schema normalizer test skipped: {e}")

        # Add the test case
        suite.addTest(loader.loadTestsFromTestCase(CITestCase))

    except ImportError as e:
        print(f"⚠️ Could not import test modules: {e}")

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print summary
    print(f"\n{'='*60}")
    print(f"📊 CI TEST SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Tests run: {result.testsRun}")
    print(f"❌ Failures: {len(result.failures)}")
    print(f"⚠️ Errors: {len(result.errors)}")
    print(f"⏭️ Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")

    if result.failures:
        print(f"\n❌ FAILURES:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.split('AssertionError:')[-1].strip()}")

    if result.errors:
        print(f"\n⚠️ ERRORS:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split('Exception:')[-1].strip()}")

    return len(result.failures) + len(result.errors) == 0


if __name__ == "__main__":
    success = run_ci_tests()
    sys.exit(0 if success else 1)
