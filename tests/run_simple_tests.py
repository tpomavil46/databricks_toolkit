#!/usr/bin/env python3
"""
Simple Test Runner

This script runs only the tests that don't require Databricks connections,
focusing on unit tests and basic functionality tests.
"""

import unittest
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_simple_tests():
    """Run tests that don't require external connections."""
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Import and add specific test classes
    try:
        from tests.test_ingest_customer import test_ingest_customer_placeholder, test_file_operations
        from tests.test_transform_order import test_transform_order_placeholder, test_csv_operations
        from tests.test_pipeline import test_pipeline_placeholder, test_environment_variables, test_basic_assertions
        
        # Create test cases
        class SimpleTestCase(unittest.TestCase):
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
        
        # Add the test case
        suite.addTest(loader.loadTestsFromTestCase(SimpleTestCase))
        
    except ImportError as e:
        print(f"⚠️ Could not import test modules: {e}")
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"📊 TEST SUMMARY")
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
    success = run_simple_tests()
    sys.exit(0 if success else 1) 