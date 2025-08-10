#!/usr/bin/env python3
"""
Test Databricks Session Functionality

This test file verifies that our Databricks session creation and basic operations work correctly.
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class TestDatabricksSession(unittest.TestCase):
    """Test Databricks session creation and basic functionality."""

    def setUp(self):
        """Set up test environment."""
        pass

    def tearDown(self):
        """Clean up after tests."""
        pass

    def test_databricks_connect_import(self):
        """Test that databricks-connect can be imported."""
        try:
            from databricks.connect import DatabricksSession
            self.assertTrue(True, "DatabricksSession can be imported")
        except ImportError as e:
            self.fail(f"Failed to import DatabricksSession: {e}")

    def test_session_builder_creation(self):
        """Test that we can create a session builder."""
        try:
            from databricks.connect import DatabricksSession
            
            # Test session builder creation
            builder = DatabricksSession.builder
            self.assertIsNotNone(builder, "Session builder should be created")
            
        except Exception as e:
            self.fail(f"Failed to create session builder: {e}")

    def test_session_profile_configuration(self):
        """Test that we can configure session with profile."""
        try:
            from databricks.connect import DatabricksSession
            
            # Test profile configuration
            builder = DatabricksSession.builder.profile("databricks")
            self.assertIsNotNone(builder, "Profile configuration should work")
            
        except Exception as e:
            self.fail(f"Failed to configure session profile: {e}")

    @patch('databricks.connect.DatabricksSession')
    def test_session_creation_with_mock(self, mock_session):
        """Test session creation with mocked DatabricksSession."""
        # Setup mock
        mock_builder = MagicMock()
        mock_session.builder = mock_builder
        mock_builder.profile.return_value.getOrCreate.return_value = MagicMock()
        
        # Test the session creation pattern we use in our code
        session = mock_session.builder.profile("databricks").getOrCreate()
        
        # Verify the call pattern
        mock_builder.profile.assert_called_once_with("databricks")
        mock_builder.profile.return_value.getOrCreate.assert_called_once()
        self.assertIsNotNone(session)

    def test_shared_utils_session_import(self):
        """Test that our shared utils session module can be imported."""
        try:
            from shared.utils.session import MyDatabricksSession
            self.assertTrue(True, "shared.utils.session can be imported")
        except ImportError as e:
            self.fail(f"Failed to import shared.utils.session: {e}")

    def test_session_utility_class(self):
        """Test our session utility class."""
        try:
            from shared.utils.session import MyDatabricksSession
            
            # Test that the class exists and has the expected method
            self.assertTrue(hasattr(MyDatabricksSession, 'get_spark'), 
                           "MyDatabricksSession should have get_spark method")
            self.assertTrue(callable(MyDatabricksSession.get_spark), 
                           "get_spark should be callable")
            
        except Exception as e:
            self.fail(f"Failed to test session utility class: {e}")

    def test_actual_session_creation(self):
        """Test actual session creation with real Databricks connection."""
        try:
            from shared.utils.session import MyDatabricksSession
            
            # Test actual session creation (this will connect to your cluster)
            spark = MyDatabricksSession.get_spark()
            
            # Verify we got a valid Spark session
            self.assertIsNotNone(spark, "Should get a valid Spark session")
            
            # Test basic Spark functionality
            test_df = spark.range(5)
            count = test_df.count()
            self.assertEqual(count, 5, "Basic Spark functionality should work")
            
            print(f"✅ Successfully created Spark session and tested basic functionality")
            
        except Exception as e:
            self.fail(f"Failed to create actual session: {e}")


if __name__ == "__main__":
    unittest.main()
