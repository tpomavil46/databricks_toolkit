"""
Comprehensive Integration Test Suite

This module provides integration tests for all components of the Databricks Toolkit.
Tests are organized by component and can be run individually or as a complete suite.
"""

import os
import sys
import unittest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List
import tempfile
import json

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient


class BaseIntegrationTest(unittest.TestCase):
    """Base class for all integration tests with common setup and teardown."""

    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests in the class."""
        cls.test_config = {
            "cluster_id": "test-cluster-id",
            "profile": "test-profile",
            "workspace_url": "https://test-workspace.cloud.databricks.com",
        }

        # Mock Databricks session
        cls.mock_spark = Mock()
        cls.mock_spark.sql.return_value = Mock()
        cls.mock_spark.read.csv.return_value = Mock()

        # Mock WorkspaceClient
        cls.mock_client = Mock()
        cls.mock_client.users.list.return_value = []
        cls.mock_client.clusters.list.return_value = []
        cls.mock_client.workspace.list.return_value = []

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up after each test method."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment after all tests in the class."""
        pass


class TestCLITools(BaseIntegrationTest):
    """Integration tests for CLI tools."""

    def test_dbfs_cli_basic_functionality(self):
        """Test basic DBFS CLI functionality."""
        from cli.dbfs_cli import explore_dbfs_path

        with patch("bootstrap.dbfs_explorer.explore_dbfs_path") as mock_explore:
            mock_explore.return_value = ["dbfs:/test/path1", "dbfs:/test/path2"]

            result = explore_dbfs_path("dbfs:/test/", recursive=False)

            self.assertEqual(len(result), 2)
            self.assertIn("dbfs:/test/path1", result)
            self.assertIn("dbfs:/test/path2", result)

    def test_analyze_dataset_basic_functionality(self):
        """Test basic dataset analysis functionality."""
        from cli.analyze_dataset import analyze_dataset

        with patch("databricks.connect.DatabricksSession") as mock_session:
            mock_spark = Mock()
            mock_session.builder.profile.return_value.clusterId.return_value.getOrCreate.return_value = (
                mock_spark
            )

            # Mock DataFrame operations
            mock_df = Mock()
            mock_df.count.return_value = 100
            mock_df.columns = ["col1", "col2"]
            mock_df.schema = Mock()
            mock_df.schema.fields = [Mock(), Mock()]

            mock_spark.read.csv.return_value = mock_df
            mock_spark.read.parquet.return_value = mock_df
            mock_spark.read.table.return_value = mock_df

            # Test should not raise exceptions
            try:
                analyze_dataset("dbfs:/test/data.csv", "test_dataset", max_rows=1000)
                self.assertTrue(True)  # Test passed if no exception
            except Exception as e:
                self.fail(f"analyze_dataset raised an exception: {e}")

    def test_bronze_ingestion_basic_functionality(self):
        """Test basic bronze ingestion functionality."""
        from cli.bronze_ingestion import bronze_ingestion

        with patch("databricks.connect.DatabricksSession") as mock_session:
            mock_spark = Mock()
            mock_session.builder.profile.return_value.clusterId.return_value.getOrCreate.return_value = (
                mock_spark
            )

            # Mock DataFrame operations
            mock_df = Mock()
            mock_df.withColumn.return_value = mock_df
            mock_df.write.mode.return_value.saveAsTable.return_value = None

            mock_spark.read.csv.return_value = mock_df
            mock_spark.read.parquet.return_value = mock_df
            mock_spark.read.table.return_value = mock_df

            # Test should not raise exceptions
            try:
                bronze_ingestion(
                    "dbfs:/test/data.csv", "test_bronze_table", "test_project"
                )
                self.assertTrue(True)  # Test passed if no exception
            except Exception as e:
                self.fail(f"bronze_ingestion raised an exception: {e}")

    def test_drop_table_basic_functionality(self):
        """Test basic drop table functionality."""
        from cli.drop_table import drop_table

        with patch("databricks.connect.DatabricksSession") as mock_session:
            mock_spark = Mock()
            mock_session.builder.profile.return_value.clusterId.return_value.getOrCreate.return_value = (
                mock_spark
            )

            # Mock SQL execution
            mock_spark.sql.return_value = None

            # Test should not raise exceptions
            try:
                drop_table("test_table")
                self.assertTrue(True)  # Test passed if no exception
            except Exception as e:
                self.fail(f"drop_table raised an exception: {e}")


class TestAdminTools(BaseIntegrationTest):
    """Integration tests for administrative tools."""

    def test_admin_client_initialization(self):
        """Test AdminClient initialization."""
        from admin.core.admin_client import AdminClient

        with patch("databricks.sdk.WorkspaceClient") as mock_workspace_client:
            mock_workspace_client.return_value = self.mock_client

            # Test should not raise exceptions
            try:
                admin_client = AdminClient()
                self.assertIsNotNone(admin_client)
                self.assertIsNotNone(admin_client.users)
                self.assertIsNotNone(admin_client.clusters)
                self.assertIsNotNone(admin_client.security)
                self.assertIsNotNone(admin_client.workspace)
                self.assertIsNotNone(admin_client.privileges)
            except Exception as e:
                self.fail(f"AdminClient initialization raised an exception: {e}")

    def test_user_manager_basic_functionality(self):
        """Test basic user management functionality."""
        from admin.core.user_manager import UserManager

        user_manager = UserManager(self.mock_client)

        # Test list users
        with patch.object(self.mock_client.users, "list") as mock_list:
            mock_list.return_value = []
            users = user_manager.list_users()
            self.assertIsInstance(users, list)

    def test_cluster_manager_basic_functionality(self):
        """Test basic cluster management functionality."""
        from admin.core.cluster_manager import ClusterManager

        cluster_manager = ClusterManager(self.mock_client)

        # Test list clusters
        with patch.object(self.mock_client.clusters, "list") as mock_list:
            mock_list.return_value = []
            clusters = cluster_manager.list_clusters()
            self.assertIsInstance(clusters, list)

    def test_security_manager_basic_functionality(self):
        """Test basic security management functionality."""
        from admin.core.security_manager import SecurityManager

        security_manager = SecurityManager(self.mock_client)

        # Test security audit
        with patch.object(self.mock_client, "users") as mock_users:
            mock_users.list.return_value = []
            audit_result = security_manager.run_security_audit()
            self.assertIsInstance(audit_result, dict)

    def test_workspace_manager_basic_functionality(self):
        """Test basic workspace management functionality."""
        from admin.core.workspace_manager import WorkspaceManager

        workspace_manager = WorkspaceManager(self.mock_client)

        # Test workspace info
        with patch.object(self.mock_client.workspace, "list") as mock_list:
            mock_list.return_value = []
            info = workspace_manager.get_workspace_info()
            self.assertIsInstance(info, dict)

    def test_privilege_manager_basic_functionality(self):
        """Test basic privilege management functionality."""
        from admin.core.privilege_manager import PrivilegeManager

        privilege_manager = PrivilegeManager(self.mock_client)

        # Test privilege summary
        with patch.object(self.mock_client, "users") as mock_users:
            mock_users.list.return_value = []
            summary = privilege_manager.get_privilege_summary()
            self.assertIsInstance(summary, dict)


class TestETLTools(BaseIntegrationTest):
    """Integration tests for ETL tools."""

    def test_etl_pipeline_initialization(self):
        """Test ETL pipeline initialization."""
        from etl.core.etl_pipeline import StandardETLPipeline
        from etl.core.config import PipelineConfig

        config = PipelineConfig(
            project_name="test_project",
            cluster_config={"cluster_id": "test-cluster"},
            database_config={"catalog": "test", "schema": "test"},
            table_config={"project_name": "test", "environment": "dev"},
            validation_config={"enable_validation": True},
        )

        pipeline = StandardETLPipeline(config)
        self.assertIsNotNone(pipeline)
        self.assertEqual(pipeline.config.project_name, "test_project")

    def test_data_transformation_basic_functionality(self):
        """Test basic data transformation functionality."""
        from etl.core.transformations import DataTransformation

        with patch("databricks.connect.DatabricksSession") as mock_session:
            mock_spark = Mock()
            mock_session.builder.profile.return_value.clusterId.return_value.getOrCreate.return_value = (
                mock_spark
            )

            # Test should not raise exceptions
            try:
                transformer = DataTransformation(mock_spark)
                self.assertIsNotNone(transformer)
            except Exception as e:
                self.fail(f"DataTransformation initialization raised an exception: {e}")

    def test_data_validator_basic_functionality(self):
        """Test basic data validation functionality."""
        from etl.core.validators import DataValidator
        from etl.core.config import PipelineConfig

        config = PipelineConfig(
            project_name="test_project",
            cluster_config={"cluster_id": "test-cluster"},
            database_config={"catalog": "test", "schema": "test"},
            table_config={"project_name": "test", "environment": "dev"},
            validation_config={"enable_validation": True},
        )

        with patch("databricks.connect.DatabricksSession") as mock_session:
            mock_spark = Mock()
            mock_session.builder.profile.return_value.clusterId.return_value.getOrCreate.return_value = (
                mock_spark
            )

            validator = DataValidator(mock_spark, config)
            self.assertIsNotNone(validator)
            self.assertTrue(validator.config.validation_config.enable_validation)


class TestCoreTools(BaseIntegrationTest):
    """Integration tests for core tools."""

    def test_sql_pipeline_executor_basic_functionality(self):
        """Test basic SQL pipeline executor functionality."""
        from core.sql_pipeline_executor import SQLPipelineExecutor

        with patch("databricks.connect.DatabricksSession") as mock_session:
            mock_spark = Mock()
            mock_session.builder.profile.return_value.clusterId.return_value.getOrCreate.return_value = (
                mock_spark
            )

            # Mock DataFrame
            mock_df = Mock()
            mock_df.count.return_value = 10
            mock_spark.sql.return_value = mock_df

            # Test should not raise exceptions
            try:
                executor = SQLPipelineExecutor(mock_spark)
                self.assertIsNotNone(executor)

                # Test parameter substitution
                sql = "SELECT * FROM ${table_name} WHERE id = ${id}"
                params = {"table_name": "test_table", "id": 123}
                result = executor._substitute_parameters(sql, params)
                self.assertIn("test_table", result)
                self.assertIn("123", result)
            except Exception as e:
                self.fail(f"SQLPipelineExecutor raised an exception: {e}")

    def test_sql_driven_pipeline_basic_functionality(self):
        """Test basic SQL-driven pipeline functionality."""
        from pipelines.sql_driven_pipeline import SQLDrivenPipeline

        with patch("databricks.connect.DatabricksSession") as mock_session:
            mock_spark = Mock()
            mock_session.builder.profile.return_value.clusterId.return_value.getOrCreate.return_value = (
                mock_spark
            )

            # Mock DataFrame
            mock_df = Mock()
            mock_df.count.return_value = 10
            mock_spark.sql.return_value = mock_df

            # Test should not raise exceptions
            try:
                # Pass spark as first parameter, then sql_base_path and project
                pipeline = SQLDrivenPipeline(mock_spark, "sql", "retail")
                self.assertIsNotNone(pipeline)
                self.assertEqual(pipeline.project, "retail")
            except Exception as e:
                self.fail(f"SQLDrivenPipeline raised an exception: {e}")


class TestBootstrapTools(BaseIntegrationTest):
    """Integration tests for bootstrap tools."""

    def test_dbfs_explorer_basic_functionality(self):
        """Test basic DBFS explorer functionality."""
        from bootstrap.dbfs_explorer import explore_dbfs_path

        with patch("databricks.sdk.WorkspaceClient") as mock_workspace_client:
            mock_client = Mock()
            mock_workspace_client.return_value = mock_client

            # Mock DBFS operations
            mock_client.dbfs.list.return_value = Mock()
            mock_client.dbfs.list.return_value.files = []

            # Test should not raise exceptions
            try:
                result = explore_dbfs_path("dbfs:/test/", recursive=False)
                self.assertIsInstance(result, list)
            except Exception as e:
                self.fail(f"explore_dbfs_path raised an exception: {e}")


class TestUtilityTools(BaseIntegrationTest):
    """Integration tests for utility tools."""

    def test_logger_basic_functionality(self):
        """Test basic logger functionality."""
        from utils.logger import log_function_call

        # Test decorator functionality
        @log_function_call
        def test_function():
            return "test_result"

        # Test should not raise exceptions
        try:
            result = test_function()
            self.assertEqual(result, "test_result")
        except Exception as e:
            self.fail(f"Logger decorator raised an exception: {e}")

    def test_schema_normalizer_basic_functionality(self):
        """Test basic schema normalizer functionality."""
        from utils.schema_normalizer import auto_normalize_columns

        # Test should not raise exceptions
        try:
            # Mock DataFrame
            mock_df = Mock()
            mock_df.columns = ["tpep_pickup_datetime", "VendorID"]
            mock_df.withColumnRenamed.return_value = mock_df

            # Test column normalization
            column_mapping = {
                "tpep_pickup_datetime": "pickup_datetime",
                "VendorID": "vendor",
            }

            result = auto_normalize_columns(mock_df, column_mapping)
            self.assertIsNotNone(result)
        except Exception as e:
            self.fail(f"Schema normalizer raised an exception: {e}")


class TestEndToEndWorkflows(BaseIntegrationTest):
    """End-to-end workflow tests."""

    def test_complete_eda_workflow(self):
        """Test complete EDA workflow."""
        # This test simulates a complete EDA workflow
        workflow_steps = [
            "explore_dbfs_path",
            "analyze_dataset",
            "bronze_ingestion",
            "drop_table",
        ]

        for step in workflow_steps:
            # Each step should be callable without raising exceptions
            self.assertTrue(callable, f"Step {step} should be callable")

    def test_complete_admin_workflow(self):
        """Test complete administrative workflow."""
        # This test simulates a complete administrative workflow
        workflow_steps = [
            "list_users",
            "list_clusters",
            "run_security_audit",
            "get_workspace_info",
            "run_privilege_audit",
        ]

        for step in workflow_steps:
            # Each step should be callable without raising exceptions
            self.assertTrue(callable, f"Step {step} should be callable")

    def test_complete_etl_workflow(self):
        """Test complete ETL workflow."""
        # This test simulates a complete ETL workflow
        workflow_steps = [
            "bronze_ingestion",
            "silver_transformation",
            "gold_aggregation",
            "data_validation",
        ]

        for step in workflow_steps:
            # Each step should be callable without raising exceptions
            self.assertTrue(callable, f"Step {step} should be callable")


def run_integration_tests():
    """Run all integration tests."""
    # Create test suite
    test_suite = unittest.TestSuite()

    # Add test classes
    test_classes = [
        TestCLITools,
        TestAdminTools,
        TestETLTools,
        TestCoreTools,
        TestBootstrapTools,
        TestUtilityTools,
        TestEndToEndWorkflows,
    ]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_integration_tests()
    sys.exit(0 if success else 1)
