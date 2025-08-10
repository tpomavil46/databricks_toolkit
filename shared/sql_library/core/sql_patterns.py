"""
SQL Patterns Module

This module provides standardized SQL patterns for common operations
in Databricks, leveraging built-in functionality where possible.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from pathlib import Path
import json


@dataclass
class SQLPattern:
    """Represents a SQL pattern with metadata and examples.
    
    A SQL pattern is a reusable template that can be parameterized and rendered
    into executable SQL code. Each pattern includes metadata for categorization,
    search, and documentation purposes.
    
    Attributes:
        name: Human-readable name for the pattern.
        description: Detailed description of what the pattern does.
        sql_template: SQL template string with parameter placeholders.
        parameters: List of required parameter names for the template.
        category: Category classification (e.g., 'ingestion', 'transformation').
        tags: List of tags for searching and categorization.
        examples: List of example usage dictionaries.
        
    Example:
        >>> pattern = SQLPattern(
        ...     name="Bronze Ingestion",
        ...     description="Standard pattern for ingesting raw data",
        ...     sql_template="CREATE TABLE {table_name} AS SELECT * FROM {source}",
        ...     parameters=["table_name", "source"],
        ...     category="ingestion",
        ...     tags=["bronze", "raw", "ingestion"],
        ...     examples=[{"description": "Example usage", "parameters": {...}}]
        ... )
    """

    name: str
    description: str
    sql_template: str
    parameters: List[str]
    category: str
    tags: List[str]
    examples: List[Dict[str, Any]]


class SQLPatterns:
    """
    SQL Patterns Library for Databricks.
    
    This class provides reusable SQL patterns that leverage Databricks' built-in 
    functionality and follow best practices for data engineering workflows.
    
    The library includes patterns for:
    - Data ingestion (bronze layer)
    - Data transformation (silver layer) 
    - Data aggregation (gold layer)
    - Data quality validation
    - Business rule validation
    
    Attributes:
        patterns: Dictionary mapping pattern names to SQLPattern objects.
        
    Example:
        >>> patterns = SQLPatterns()
        >>> sql = patterns.render_pattern('bronze_ingestion', {
        ...     'catalog': 'hive_metastore',
        ...     'schema': 'retail',
        ...     'table_name': 'customers',
        ...     'source_table': 'csv.`/mnt/databricks-datasets/retail-org/customers/`'
        ... })
        >>> print(sql)
    """

    def __init__(self):
        """Initialize SQL patterns library.
        
        Loads all available SQL patterns into memory for quick access.
        Patterns are organized by category: ingestion, transformation, 
        quality, aggregation, and validation.
        """
        self.patterns = self._load_patterns()

    def _load_patterns(self) -> Dict[str, SQLPattern]:
        """Load all SQL patterns from all categories.
        
        This method initializes the complete pattern library by loading patterns
        from all categories: ingestion, transformation, quality, aggregation,
        and validation.
        
        Returns:
            Dictionary mapping pattern names to SQLPattern objects.
            
        Note:
            This is an internal method called during initialization.
            Patterns are loaded once and cached for performance.
        """
        patterns = {}

        # Data Ingestion Patterns
        patterns.update(self._get_ingestion_patterns())

        # Data Transformation Patterns
        patterns.update(self._get_transformation_patterns())

        # Data Quality Patterns
        patterns.update(self._get_quality_patterns())

        # Data Aggregation Patterns
        patterns.update(self._get_aggregation_patterns())

        # Data Validation Patterns
        patterns.update(self._get_validation_patterns())

        return patterns

    def _get_ingestion_patterns(self) -> Dict[str, SQLPattern]:
        """Get data ingestion patterns for bronze layer processing.
        
        Returns:
            Dictionary of ingestion patterns including:
            - bronze_ingestion: Standard bronze layer ingestion
            - incremental_ingestion: Incremental ingestion with deduplication
            
        Note:
            This is an internal method used during pattern library initialization.
        """
        return {
            "bronze_ingestion": SQLPattern(
                name="Bronze Ingestion",
                description="Standard pattern for ingesting raw data into bronze layer",
                sql_template="""
CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name}_bronze
USING DELTA
AS SELECT 
    *,
    current_timestamp() as ingestion_timestamp,
    input_file_name() as source_file,
    'bronze' as data_layer
FROM {source_table}
""",
                parameters=["catalog", "schema", "table_name", "source_table"],
                category="ingestion",
                tags=["bronze", "raw", "ingestion"],
                examples=[
                    {
                        "description": "Ingest CSV data to bronze",
                        "parameters": {
                            "catalog": "hive_metastore",
                            "schema": "retail",
                            "table_name": "customers",
                            "source_table": "csv.`/mnt/databricks-datasets/retail-org/customers/`",
                        },
                    }
                ],
            ),
            "incremental_ingestion": SQLPattern(
                name="Incremental Ingestion",
                description="Pattern for incremental data ingestion with deduplication",
                sql_template="""
MERGE INTO {catalog}.{schema}.{table_name}_bronze AS target
USING (
    SELECT 
        *,
        current_timestamp() as ingestion_timestamp,
        input_file_name() as source_file,
        'bronze' as data_layer,
        row_number() over (
            partition by {partition_key} 
            order by {timestamp_column} desc
        ) as rn
    FROM {source_table}
    WHERE {timestamp_column} > (
        SELECT COALESCE(max({timestamp_column}), '1970-01-01')
        FROM {catalog}.{schema}.{table_name}_bronze
    )
) AS source
ON target.{partition_key} = source.{partition_key}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""",
                parameters=[
                    "catalog",
                    "schema",
                    "table_name",
                    "source_table",
                    "partition_key",
                    "timestamp_column",
                ],
                category="ingestion",
                tags=["incremental", "merge", "deduplication"],
                examples=[
                    {
                        "description": "Incremental customer ingestion",
                        "parameters": {
                            "catalog": "hive_metastore",
                            "schema": "retail",
                            "table_name": "customers",
                            "source_table": "csv.`/mnt/databricks-datasets/retail-org/customers/`",
                            "partition_key": "customer_id",
                            "timestamp_column": "updated_at",
                        },
                    }
                ],
            ),
        }

    def _get_transformation_patterns(self) -> Dict[str, SQLPattern]:
        """Get data transformation patterns."""
        return {
            "silver_transformation": SQLPattern(
                name="Silver Transformation",
                description="Standard pattern for transforming data in silver layer",
                sql_template="""
CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name}_silver
USING DELTA
AS SELECT 
    {columns},
    current_timestamp() as transformation_timestamp,
    'silver' as data_layer
FROM {catalog}.{schema}.{table_name}_bronze
WHERE {quality_conditions}
""",
                parameters=[
                    "catalog",
                    "schema",
                    "table_name",
                    "columns",
                    "quality_conditions",
                ],
                category="transformation",
                tags=["silver", "transformation", "cleaning"],
                examples=[
                    {
                        "description": "Transform customer data to silver",
                        "parameters": {
                            "catalog": "hive_metastore",
                            "schema": "retail",
                            "table_name": "customers",
                            "columns": "customer_id, customer_name, email, phone, address, city, state, country",
                            "quality_conditions": "customer_id IS NOT NULL AND customer_name IS NOT NULL",
                        },
                    }
                ],
            ),
            "data_cleaning": SQLPattern(
                name="Data Cleaning",
                description="Pattern for cleaning and standardizing data",
                sql_template="""
SELECT 
    {id_column},
    TRIM(customer_name) as customer_name,
    LOWER(TRIM(email)) as email,
    REGEXP_REPLACE(phone, '[^0-9]', '') as phone_clean,
    INITCAP(TRIM(city)) as city,
    UPPER(TRIM(state)) as state,
    UPPER(TRIM(country)) as country,
    CASE 
        WHEN email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}$' 
        THEN 'valid' 
        ELSE 'invalid' 
    END as email_validity,
    current_timestamp() as cleaning_timestamp
FROM {source_table}
WHERE {quality_conditions}
""",
                parameters=["id_column", "source_table", "quality_conditions"],
                category="transformation",
                tags=["cleaning", "standardization", "validation"],
                examples=[
                    {
                        "description": "Clean customer data",
                        "parameters": {
                            "id_column": "customer_id",
                            "source_table": "hive_metastore.retail.customers_bronze",
                            "quality_conditions": "customer_id IS NOT NULL",
                        },
                    }
                ],
            ),
        }

    def _get_quality_patterns(self) -> Dict[str, SQLPattern]:
        """Get data quality patterns."""
        return {
            "completeness_check": SQLPattern(
                name="Completeness Check",
                description="Check for null values and data completeness",
                sql_template="""
SELECT 
    COUNT(*) as total_rows,
    COUNT({column}) as non_null_count,
    COUNT(*) - COUNT({column}) as null_count,
    ROUND(COUNT({column}) * 100.0 / COUNT(*), 2) as completeness_percentage
FROM {table}
""",
                parameters=["table", "column"],
                category="quality",
                tags=["completeness", "null_check", "quality"],
                examples=[
                    {
                        "description": "Check customer_id completeness",
                        "parameters": {
                            "table": "hive_metastore.retail.customers_bronze",
                            "column": "customer_id",
                        },
                    }
                ],
            ),
            "uniqueness_check": SQLPattern(
                name="Uniqueness Check",
                description="Check for duplicate values in key columns",
                sql_template="""
SELECT 
    {column},
    COUNT(*) as occurrence_count
FROM {table}
GROUP BY {column}
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
""",
                parameters=["table", "column"],
                category="quality",
                tags=["uniqueness", "duplicates", "quality"],
                examples=[
                    {
                        "description": "Check for duplicate customer IDs",
                        "parameters": {
                            "table": "hive_metastore.retail.customers_bronze",
                            "column": "customer_id",
                        },
                    }
                ],
            ),
            "data_type_check": SQLPattern(
                name="Data Type Check",
                description="Validate data types and format compliance",
                sql_template="""
SELECT 
    {column},
    CASE 
        WHEN {column} REGEXP '{regex_pattern}' THEN 'valid'
        ELSE 'invalid'
    END as validation_result
FROM {table}
WHERE {column} IS NOT NULL
""",
                parameters=["table", "column", "regex_pattern"],
                category="quality",
                tags=["validation", "regex", "quality"],
                examples=[
                    {
                        "description": "Validate email format",
                        "parameters": {
                            "table": "hive_metastore.retail.customers_bronze",
                            "column": "email",
                            "regex_pattern": "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
                        },
                    }
                ],
            ),
        }

    def _get_aggregation_patterns(self) -> Dict[str, SQLPattern]:
        """Get data aggregation patterns."""
        return {
            "gold_aggregation": SQLPattern(
                name="Gold Aggregation",
                description="Standard pattern for creating gold layer aggregations",
                sql_template="""
CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name}_gold
USING DELTA
AS SELECT 
    {group_by_columns},
    {aggregation_columns},
    current_timestamp() as aggregation_timestamp,
    'gold' as data_layer
FROM {catalog}.{schema}.{table_name}_silver
GROUP BY {group_by_columns}
""",
                parameters=[
                    "catalog",
                    "schema",
                    "table_name",
                    "group_by_columns",
                    "aggregation_columns",
                ],
                category="aggregation",
                tags=["gold", "aggregation", "kpis"],
                examples=[
                    {
                        "description": "Create customer KPIs",
                        "parameters": {
                            "catalog": "hive_metastore",
                            "schema": "retail",
                            "table_name": "customers",
                            "group_by_columns": "country, state, city",
                            "aggregation_columns": "COUNT(*) as customer_count, COUNT(DISTINCT customer_id) as unique_customers",
                        },
                    }
                ],
            ),
            "time_series_aggregation": SQLPattern(
                name="Time Series Aggregation",
                description="Pattern for time-based aggregations",
                sql_template="""
SELECT 
    DATE_TRUNC('{time_unit}', {timestamp_column}) as time_period,
    {group_by_columns},
    {aggregation_columns}
FROM {table}
WHERE {timestamp_column} >= '{start_date}' 
  AND {timestamp_column} < '{end_date}'
GROUP BY DATE_TRUNC('{time_unit}', {timestamp_column}), {group_by_columns}
ORDER BY time_period, {group_by_columns}
""",
                parameters=[
                    "table",
                    "timestamp_column",
                    "time_unit",
                    "start_date",
                    "end_date",
                    "group_by_columns",
                    "aggregation_columns",
                ],
                category="aggregation",
                tags=["time_series", "aggregation", "analytics"],
                examples=[
                    {
                        "description": "Daily sales aggregation",
                        "parameters": {
                            "table": "hive_metastore.retail.orders_silver",
                            "timestamp_column": "order_date",
                            "time_unit": "day",
                            "start_date": "2024-01-01",
                            "end_date": "2024-12-31",
                            "group_by_columns": "product_category",
                            "aggregation_columns": "COUNT(*) as order_count, SUM(order_amount) as total_sales",
                        },
                    }
                ],
            ),
        }

    def _get_validation_patterns(self) -> Dict[str, SQLPattern]:
        """Get data validation patterns."""
        return {
            "business_rule_validation": SQLPattern(
                name="Business Rule Validation",
                description="Validate business rules and constraints",
                sql_template="""
SELECT 
    'Business Rule Violation' as validation_type,
    {rule_description} as rule_description,
    COUNT(*) as violation_count
FROM {table}
WHERE {business_rule_condition}
""",
                parameters=["table", "rule_description", "business_rule_condition"],
                category="validation",
                tags=["business_rules", "validation", "compliance"],
                examples=[
                    {
                        "description": "Validate order amounts are positive",
                        "parameters": {
                            "table": "hive_metastore.retail.orders_bronze",
                            "rule_description": "Order amounts must be positive",
                            "business_rule_condition": "order_amount <= 0",
                        },
                    }
                ],
            ),
            "referential_integrity": SQLPattern(
                name="Referential Integrity",
                description="Check referential integrity between tables",
                sql_template="""
SELECT 
    'Referential Integrity Violation' as validation_type,
    COUNT(*) as orphaned_records
FROM {child_table} c
LEFT JOIN {parent_table} p ON c.{foreign_key} = p.{primary_key}
WHERE p.{primary_key} IS NULL
""",
                parameters=[
                    "child_table",
                    "parent_table",
                    "foreign_key",
                    "primary_key",
                ],
                category="validation",
                tags=["referential_integrity", "foreign_keys", "validation"],
                examples=[
                    {
                        "description": "Check orders reference valid customers",
                        "parameters": {
                            "child_table": "hive_metastore.retail.orders_bronze",
                            "parent_table": "hive_metastore.retail.customers_bronze",
                            "foreign_key": "customer_id",
                            "primary_key": "customer_id",
                        },
                    }
                ],
            ),
        }

    def get_pattern(self, pattern_name: str) -> Optional[SQLPattern]:
        """Get a specific SQL pattern by name.
        
        Args:
            pattern_name: Name of the pattern to retrieve.
            
        Returns:
            SQLPattern object if found, None otherwise.
            
        Example:
            >>> patterns = SQLPatterns()
            >>> pattern = patterns.get_pattern('bronze_ingestion')
            >>> print(pattern.name)
            'Bronze Ingestion'
        """
        return self.patterns.get(pattern_name)

    def list_patterns(self, category: Optional[str] = None) -> List[SQLPattern]:
        """List all patterns, optionally filtered by category.
        
        Args:
            category: Optional category filter (e.g., 'ingestion', 'transformation').
                     If None, returns all patterns.
                     
        Returns:
            List of SQLPattern objects matching the criteria.
            
        Example:
            >>> patterns = SQLPatterns()
            >>> ingestion_patterns = patterns.list_patterns('ingestion')
            >>> all_patterns = patterns.list_patterns()
        """
        if category:
            return [p for p in self.patterns.values() if p.category == category]
        return list(self.patterns.values())

    def render_pattern(self, pattern_name: str, parameters: Dict[str, Any]) -> str:
        """Render a SQL pattern with the given parameters.
        
        Args:
            pattern_name: Name of the pattern to render.
            parameters: Dictionary of parameters to substitute in the SQL template.
                       Must include all required parameters for the pattern.
                       
        Returns:
            Rendered SQL string with parameters substituted.
            
        Raises:
            ValueError: If pattern_name is not found or required parameters are missing.
            
        Example:
            >>> patterns = SQLPatterns()
            >>> sql = patterns.render_pattern('bronze_ingestion', {
            ...     'catalog': 'hive_metastore',
            ...     'schema': 'retail',
            ...     'table_name': 'customers',
            ...     'source_table': 'csv.`/mnt/databricks-datasets/retail-org/customers/`'
            ... })
            >>> print(sql)
        """
        pattern = self.get_pattern(pattern_name)
        if not pattern:
            raise ValueError(f"Pattern '{pattern_name}' not found")

        # Validate required parameters
        missing_params = [p for p in pattern.parameters if p not in parameters]
        if missing_params:
            raise ValueError(f"Missing required parameters: {missing_params}")

        # Render the SQL template
        return pattern.sql_template.format(**parameters)

    def get_pattern_examples(self, pattern_name: str) -> List[Dict[str, Any]]:
        """Get examples for a specific pattern.
        
        Args:
            pattern_name: Name of the pattern to get examples for.
            
        Returns:
            List of example dictionaries, each containing 'description' and 'parameters'.
            
        Example:
            >>> patterns = SQLPatterns()
            >>> examples = patterns.get_pattern_examples('bronze_ingestion')
            >>> for example in examples:
            ...     print(example['description'])
        """
        pattern = self.get_pattern(pattern_name)
        return pattern.examples if pattern else []

    def search_patterns(self, query: str) -> List[SQLPattern]:
        """Search patterns by name, description, or tags.
        
        Args:
            query: Search query string. Case-insensitive search in pattern names,
                   descriptions, and tags.
                   
        Returns:
            List of SQLPattern objects that match the search query.
            
        Example:
            >>> patterns = SQLPatterns()
            >>> bronze_patterns = patterns.search_patterns('bronze')
            >>> quality_patterns = patterns.search_patterns('quality')
        """
        query_lower = query.lower()
        results = []

        for pattern in self.patterns.values():
            if (
                query_lower in pattern.name.lower()
                or query_lower in pattern.description.lower()
                or any(query_lower in tag.lower() for tag in pattern.tags)
            ):
                results.append(pattern)

        return results
