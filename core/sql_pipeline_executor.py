"""
SQL Pipeline Executor

A flexible, reusable pipeline executor that accepts SQL files as arguments
and executes them while maintaining loose coupling between SQL and Python code.

This module provides a framework for:
- Loading SQL from files or strings
- Parameterizing SQL queries
- Executing SQL with error handling
- Maintaining separation of concerns
"""

import os
import re
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import logging

from utils.logger import log_function_call


@dataclass
class SQLPipelineConfig:
    """Configuration for SQL pipeline execution."""

    # SQL source configuration
    sql_file_path: Optional[str] = None
    sql_string: Optional[str] = None
    sql_parameters: Optional[Dict[str, Any]] = None

    # Execution configuration
    create_temp_view: bool = False
    temp_view_name: Optional[str] = None
    write_to_table: bool = False
    table_name: Optional[str] = None
    write_format: str = "delta"
    write_mode: str = "overwrite"

    # Validation configuration
    expected_schema: Optional[StructType] = None
    validate_schema: bool = False

    # Error handling
    continue_on_error: bool = False
    log_sql: bool = True


class SQLPipelineExecutor:
    """
    A reusable SQL pipeline executor that maintains loose coupling between SQL and Python.

    This class provides a framework for executing SQL queries with:
    - Parameter substitution
    - Error handling and logging
    - Schema validation
    - Flexible output options (temp views, tables, DataFrames)
    - Separation of SQL logic from Python orchestration
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the SQL pipeline executor.

        Args:
            spark: The Spark session to use for SQL execution
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    @log_function_call
    def load_sql(self, config: SQLPipelineConfig) -> str:
        """
        Load SQL from file or string with parameter substitution.

        Args:
            config: Configuration containing SQL source and parameters

        Returns:
            The processed SQL string with parameters substituted

        Raises:
            ValueError: If neither sql_file_path nor sql_string is provided
            FileNotFoundError: If SQL file doesn't exist
        """
        if config.sql_file_path:
            sql = self._load_sql_from_file(config.sql_file_path)
        elif config.sql_string:
            sql = config.sql_string
        else:
            raise ValueError("Either sql_file_path or sql_string must be provided")

        # Apply parameter substitution
        if config.sql_parameters:
            sql = self._substitute_parameters(sql, config.sql_parameters)

        if config.log_sql:
            self.logger.info(f"Loaded SQL:\n{sql}")

        return sql

    def _load_sql_from_file(self, file_path: str) -> str:
        """Load SQL content from a file."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")

        with open(path, "r") as f:
            return f.read()

    def _substitute_parameters(self, sql: str, parameters: Dict[str, Any]) -> str:
        """
        Substitute parameters in SQL using a safe templating approach.

        Args:
            sql: The SQL string with parameter placeholders
            parameters: Dictionary of parameter names and values

        Returns:
            SQL with parameters substituted
        """
        # Use a simple but safe parameter substitution
        # Format: ${parameter_name}
        for param_name, param_value in parameters.items():
            placeholder = f"${{{param_name}}}"
            if placeholder in sql:
                # Handle different parameter types safely
                if isinstance(param_value, str):
                    # For table names and identifiers, don't quote them
                    if (
                        param_name.endswith("_table_name")
                        or param_name.endswith("_path")
                        or param_name == "file_format"
                    ):
                        sql = sql.replace(placeholder, param_value)
                    elif param_value == "":  # Handle empty strings
                        sql = sql.replace(placeholder, "")
                    else:
                        sql = sql.replace(placeholder, f"'{param_value}'")
                elif param_value is None:
                    sql = sql.replace(placeholder, "NULL")
                else:
                    # For numbers, booleans, etc.
                    sql = sql.replace(placeholder, str(param_value))

        return sql

    @log_function_call
    def execute_sql(self, config: SQLPipelineConfig) -> Optional[DataFrame]:
        """
        Execute SQL with the given configuration.

        Args:
            config: Configuration for SQL execution

        Returns:
            DataFrame if create_temp_view is False, None otherwise

        Raises:
            Exception: If SQL execution fails and continue_on_error is False
        """
        try:
            sql = self.load_sql(config)

            # Execute the SQL
            result = self.spark.sql(sql)

            # Check if this is a DDL statement (CREATE, DROP, etc.)
            sql_upper = sql.strip().upper()
            is_ddl = any(
                keyword in sql_upper
                for keyword in ["CREATE", "DROP", "ALTER", "INSERT", "UPDATE", "DELETE"]
            )

            if is_ddl:
                # For DDL statements, just execute and return None
                self.logger.info(f"Executed DDL statement: {sql_upper.split()[0]}")
                return None

            # For SELECT statements, handle as DataFrame
            if config.create_temp_view:
                view_name = config.temp_view_name or self._generate_temp_view_name()
                result.createOrReplaceTempView(view_name)
                self.logger.info(f"Created temp view: {view_name}")
                return None

            elif config.write_to_table:
                if not config.table_name:
                    raise ValueError(
                        "table_name is required when write_to_table is True"
                    )

                result.write.format(config.write_format).mode(
                    config.write_mode
                ).saveAsTable(config.table_name)
                self.logger.info(f"Wrote to table: {config.table_name}")
                return None

            else:
                # Return DataFrame for further processing
                return result

        except Exception as e:
            self.logger.error(f"SQL execution failed: {str(e)}")
            if not config.continue_on_error:
                raise
            return None

    def _generate_temp_view_name(self) -> str:
        """Generate a unique temp view name."""
        import uuid

        return f"temp_view_{uuid.uuid4().hex[:8]}"

    @log_function_call
    def execute_pipeline(
        self, pipeline_configs: List[SQLPipelineConfig]
    ) -> Dict[str, Any]:
        """
        Execute a sequence of SQL operations as a pipeline.

        Args:
            pipeline_configs: List of SQL configurations to execute in order

        Returns:
            Dictionary containing execution results and any returned DataFrames
        """
        results = {
            "success": True,
            "executed_steps": [],
            "dataframes": {},
            "errors": [],
        }

        for i, config in enumerate(pipeline_configs):
            try:
                step_name = f"step_{i+1}"
                self.logger.info(f"Executing {step_name}")

                df = self.execute_sql(config)

                if df is not None:
                    results["dataframes"][step_name] = df

                results["executed_steps"].append(step_name)

            except Exception as e:
                error_msg = f"Step {i+1} failed: {str(e)}"
                self.logger.error(error_msg)
                results["errors"].append(error_msg)

                if not config.continue_on_error:
                    results["success"] = False
                    break

        return results

    def validate_schema(self, df: DataFrame, expected_schema: StructType) -> bool:
        """
        Validate DataFrame schema against expected schema.

        Args:
            df: DataFrame to validate
            expected_schema: Expected schema structure

        Returns:
            True if schemas match, False otherwise
        """
        actual_schema = df.schema
        return actual_schema == expected_schema


class SQLPipelineBuilder:
    """
    Builder pattern for creating SQL pipeline configurations.

    This provides a fluent interface for building complex SQL pipeline configurations.
    """

    def __init__(self):
        self.config = SQLPipelineConfig()

    def from_file(self, file_path: str) -> "SQLPipelineBuilder":
        """Set SQL source from file."""
        self.config.sql_file_path = file_path
        return self

    def from_string(self, sql_string: str) -> "SQLPipelineBuilder":
        """Set SQL source from string."""
        self.config.sql_string = sql_string
        return self

    def with_parameters(self, parameters: Dict[str, Any]) -> "SQLPipelineBuilder":
        """Set SQL parameters for substitution."""
        self.config.sql_parameters = parameters
        return self

    def create_temp_view(self, view_name: Optional[str] = None) -> "SQLPipelineBuilder":
        """Configure to create a temp view."""
        self.config.create_temp_view = True
        self.config.temp_view_name = view_name
        return self

    def write_to_table(
        self, table_name: str, format: str = "delta", mode: str = "overwrite"
    ) -> "SQLPipelineBuilder":
        """Configure to write to a table."""
        self.config.write_to_table = True
        self.config.table_name = table_name
        self.config.write_format = format
        self.config.write_mode = mode
        return self

    def with_schema_validation(
        self, expected_schema: StructType
    ) -> "SQLPipelineBuilder":
        """Configure schema validation."""
        self.config.validate_schema = True
        self.config.expected_schema = expected_schema
        return self

    def continue_on_error(self) -> "SQLPipelineBuilder":
        """Configure to continue on errors."""
        self.config.continue_on_error = True
        return self

    def build(self) -> SQLPipelineConfig:
        """Build the final configuration."""
        return self.config
