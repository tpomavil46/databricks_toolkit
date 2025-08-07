"""
SQL Functions Module

This module provides reusable SQL functions for common operations
in Databricks, leveraging built-in functionality where possible.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import json


@dataclass
class SQLFunction:
    """Represents a SQL function with metadata."""

    name: str
    description: str
    sql_definition: str
    parameters: List[str]
    return_type: str
    category: str
    tags: List[str]
    examples: List[Dict[str, Any]]


class SQLFunctions:
    """
    Reusable SQL functions for common operations.

    This class provides SQL functions that leverage
    Databricks' built-in functionality and follow best practices.
    """

    def __init__(self):
        """Initialize SQL functions."""
        self.functions = self._load_functions()

    def _load_functions(self) -> Dict[str, SQLFunction]:
        """Load all SQL functions."""
        functions = {}

        # String Functions
        functions.update(self._get_string_functions())

        # Date/Time Functions
        functions.update(self._get_datetime_functions())

        # Numeric Functions
        functions.update(self._get_numeric_functions())

        # Data Quality Functions
        functions.update(self._get_quality_functions())

        # Business Logic Functions
        functions.update(self._get_business_functions())

        return functions

    def _get_string_functions(self) -> Dict[str, SQLFunction]:
        """Get string manipulation functions."""
        return {
            "clean_string": SQLFunction(
                name="clean_string",
                description="Clean and standardize string values",
                sql_definition="""
CREATE OR REPLACE FUNCTION clean_string(
    input_string STRING,
    remove_special_chars BOOLEAN DEFAULT TRUE,
    trim_whitespace BOOLEAN DEFAULT TRUE,
    to_lowercase BOOLEAN DEFAULT FALSE
) RETURNS STRING AS $$
    SELECT CASE 
        WHEN input_string IS NULL THEN NULL
        WHEN trim_whitespace THEN 
            CASE 
                WHEN remove_special_chars THEN 
                    CASE 
                        WHEN to_lowercase THEN LOWER(REGEXP_REPLACE(TRIM(input_string), '[^a-zA-Z0-9\\s]', ''))
                        ELSE REGEXP_REPLACE(TRIM(input_string), '[^a-zA-Z0-9\\s]', '')
                    END
                ELSE 
                    CASE 
                        WHEN to_lowercase THEN LOWER(TRIM(input_string))
                        ELSE TRIM(input_string)
                    END
            END
        ELSE 
            CASE 
                WHEN remove_special_chars THEN 
                    CASE 
                        WHEN to_lowercase THEN LOWER(REGEXP_REPLACE(input_string, '[^a-zA-Z0-9\\s]', ''))
                        ELSE REGEXP_REPLACE(input_string, '[^a-zA-Z0-9\\s]', '')
                    END
                ELSE 
                    CASE 
                        WHEN to_lowercase THEN LOWER(input_string)
                        ELSE input_string
                    END
            END
    END
$$;
""",
                parameters=[
                    "input_string",
                    "remove_special_chars",
                    "trim_whitespace",
                    "to_lowercase",
                ],
                return_type="STRING",
                category="string",
                tags=["cleaning", "standardization", "string"],
                examples=[
                    {
                        "description": "Clean customer name",
                        "usage": "SELECT clean_string('  John Doe!@#  ', TRUE, TRUE, FALSE) as cleaned_name",
                        "expected": "'John Doe'",
                    }
                ],
            ),
            "validate_email": SQLFunction(
                name="validate_email",
                description="Validate email format using regex",
                sql_definition="""
CREATE OR REPLACE FUNCTION validate_email(email STRING) RETURNS BOOLEAN AS $$
    SELECT CASE 
        WHEN email IS NULL THEN FALSE
        WHEN email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN TRUE
        ELSE FALSE
    END
$$;
""",
                parameters=["email"],
                return_type="BOOLEAN",
                category="string",
                tags=["validation", "email", "regex"],
                examples=[
                    {
                        "description": "Validate email format",
                        "usage": "SELECT validate_email('john.doe@company.com') as is_valid",
                        "expected": "TRUE",
                    }
                ],
            ),
            "extract_domain": SQLFunction(
                name="extract_domain",
                description="Extract domain from email address",
                sql_definition="""
CREATE OR REPLACE FUNCTION extract_domain(email STRING) RETURNS STRING AS $$
    SELECT CASE 
        WHEN email IS NULL THEN NULL
        WHEN email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' 
        THEN REGEXP_EXTRACT(email, '@([^@]+)$', 1)
        ELSE NULL
    END
$$;
""",
                parameters=["email"],
                return_type="STRING",
                category="string",
                tags=["extraction", "email", "domain"],
                examples=[
                    {
                        "description": "Extract domain from email",
                        "usage": "SELECT extract_domain('john.doe@company.com') as domain",
                        "expected": "'company.com'",
                    }
                ],
            ),
        }

    def _get_datetime_functions(self) -> Dict[str, SQLFunction]:
        """Get date/time manipulation functions."""
        return {
            "age_in_days": SQLFunction(
                name="age_in_days",
                description="Calculate age in days between two dates",
                sql_definition="""
CREATE OR REPLACE FUNCTION age_in_days(
    start_date DATE,
    end_date DATE DEFAULT CURRENT_DATE()
) RETURNS INT AS $$
    SELECT DATEDIFF(end_date, start_date)
$$;
""",
                parameters=["start_date", "end_date"],
                return_type="INT",
                category="datetime",
                tags=["age", "calculation", "date"],
                examples=[
                    {
                        "description": "Calculate customer age in days",
                        "usage": "SELECT age_in_days('1990-01-01') as age_days",
                        "expected": "Number of days since 1990-01-01",
                    }
                ],
            ),
            "business_days_between": SQLFunction(
                name="business_days_between",
                description="Calculate business days between two dates",
                sql_definition="""
CREATE OR REPLACE FUNCTION business_days_between(
    start_date DATE,
    end_date DATE
) RETURNS INT AS $$
    SELECT DATEDIFF(end_date, start_date) - 
           (DATEDIFF(end_date, start_date) / 7) * 2 -
           CASE WHEN DAYOFWEEK(start_date) = 1 THEN 1 ELSE 0 END -
           CASE WHEN DAYOFWEEK(end_date) = 7 THEN 1 ELSE 0 END
$$;
""",
                parameters=["start_date", "end_date"],
                return_type="INT",
                category="datetime",
                tags=["business_days", "calculation", "date"],
                examples=[
                    {
                        "description": "Calculate business days between dates",
                        "usage": "SELECT business_days_between('2024-01-01', '2024-01-31') as business_days",
                        "expected": "Number of business days",
                    }
                ],
            ),
            "is_weekend": SQLFunction(
                name="is_weekend",
                description="Check if a date falls on weekend",
                sql_definition="""
CREATE OR REPLACE FUNCTION is_weekend(check_date DATE) RETURNS BOOLEAN AS $$
    SELECT DAYOFWEEK(check_date) IN (1, 7)
$$;
""",
                parameters=["check_date"],
                return_type="BOOLEAN",
                category="datetime",
                tags=["weekend", "date", "boolean"],
                examples=[
                    {
                        "description": "Check if date is weekend",
                        "usage": "SELECT is_weekend('2024-01-06') as is_weekend",
                        "expected": "TRUE (if Saturday)",
                    }
                ],
            ),
        }

    def _get_numeric_functions(self) -> Dict[str, SQLFunction]:
        """Get numeric calculation functions."""
        return {
            "calculate_percentage": SQLFunction(
                name="calculate_percentage",
                description="Calculate percentage with null handling",
                sql_definition="""
CREATE OR REPLACE FUNCTION calculate_percentage(
    numerator DECIMAL(10,2),
    denominator DECIMAL(10,2),
    decimal_places INT DEFAULT 2
) RETURNS DECIMAL(10,2) AS $$
    SELECT CASE 
        WHEN denominator IS NULL OR denominator = 0 THEN NULL
        ELSE ROUND((numerator * 100.0 / denominator), decimal_places)
    END
$$;
""",
                parameters=["numerator", "denominator", "decimal_places"],
                return_type="DECIMAL(10,2)",
                category="numeric",
                tags=["percentage", "calculation", "null_safe"],
                examples=[
                    {
                        "description": "Calculate completion percentage",
                        "usage": "SELECT calculate_percentage(75, 100, 2) as completion_pct",
                        "expected": "75.00",
                    }
                ],
            ),
            "safe_division": SQLFunction(
                name="safe_division",
                description="Safe division with null handling",
                sql_definition="""
CREATE OR REPLACE FUNCTION safe_division(
    numerator DECIMAL(10,2),
    denominator DECIMAL(10,2),
    default_value DECIMAL(10,2) DEFAULT NULL
) RETURNS DECIMAL(10,2) AS $$
    SELECT CASE 
        WHEN denominator IS NULL OR denominator = 0 THEN default_value
        ELSE numerator / denominator
    END
$$;
""",
                parameters=["numerator", "denominator", "default_value"],
                return_type="DECIMAL(10,2)",
                category="numeric",
                tags=["division", "null_safe", "calculation"],
                examples=[
                    {
                        "description": "Safe division with default value",
                        "usage": "SELECT safe_division(10, 0, 0) as result",
                        "expected": "0",
                    }
                ],
            ),
            "is_in_range": SQLFunction(
                name="is_in_range",
                description="Check if value is within specified range",
                sql_definition="""
CREATE OR REPLACE FUNCTION is_in_range(
    value DECIMAL(10,2),
    min_value DECIMAL(10,2),
    max_value DECIMAL(10,2),
    include_bounds BOOLEAN DEFAULT TRUE
) RETURNS BOOLEAN AS $$
    SELECT CASE 
        WHEN value IS NULL THEN FALSE
        WHEN include_bounds THEN value BETWEEN min_value AND max_value
        ELSE value > min_value AND value < max_value
    END
$$;
""",
                parameters=["value", "min_value", "max_value", "include_bounds"],
                return_type="BOOLEAN",
                category="numeric",
                tags=["range", "validation", "boolean"],
                examples=[
                    {
                        "description": "Check if amount is in valid range",
                        "usage": "SELECT is_in_range(100, 0, 1000, TRUE) as is_valid",
                        "expected": "TRUE",
                    }
                ],
            ),
        }

    def _get_quality_functions(self) -> Dict[str, SQLFunction]:
        """Get data quality functions."""
        return {
            "data_quality_score": SQLFunction(
                name="data_quality_score",
                description="Calculate data quality score based on multiple checks",
                sql_definition="""
CREATE OR REPLACE FUNCTION data_quality_score(
    completeness_score DECIMAL(3,2),
    accuracy_score DECIMAL(3,2),
    consistency_score DECIMAL(3,2),
    validity_score DECIMAL(3,2),
    weights STRING DEFAULT '0.25,0.25,0.25,0.25'
) RETURNS DECIMAL(3,2) AS $$
    SELECT ROUND(
        (completeness_score * 0.25 + 
         accuracy_score * 0.25 + 
         consistency_score * 0.25 + 
         validity_score * 0.25), 2
    )
$$;
""",
                parameters=[
                    "completeness_score",
                    "accuracy_score",
                    "consistency_score",
                    "validity_score",
                    "weights",
                ],
                return_type="DECIMAL(3,2)",
                category="quality",
                tags=["quality_score", "calculation", "weighted"],
                examples=[
                    {
                        "description": "Calculate overall data quality score",
                        "usage": "SELECT data_quality_score(0.95, 0.90, 0.85, 0.92) as quality_score",
                        "expected": "0.91",
                    }
                ],
            ),
            "null_percentage": SQLFunction(
                name="null_percentage",
                description="Calculate percentage of null values in a column",
                sql_definition="""
CREATE OR REPLACE FUNCTION null_percentage(
    column_value STRING
) RETURNS DECIMAL(5,2) AS $$
    SELECT CASE 
        WHEN column_value IS NULL THEN 100.0
        ELSE 0.0
    END
$$;
""",
                parameters=["column_value"],
                return_type="DECIMAL(5,2)",
                category="quality",
                tags=["null_check", "percentage", "quality"],
                examples=[
                    {
                        "description": "Check null percentage for a column",
                        "usage": "SELECT AVG(null_percentage(customer_name)) as null_pct FROM customers",
                        "expected": "Average null percentage",
                    }
                ],
            ),
        }

    def _get_business_functions(self) -> Dict[str, SQLFunction]:
        """Get business logic functions."""
        return {
            "customer_segment": SQLFunction(
                name="customer_segment",
                description="Segment customers based on total spend",
                sql_definition="""
CREATE OR REPLACE FUNCTION customer_segment(
    total_spend DECIMAL(10,2)
) RETURNS STRING AS $$
    SELECT CASE 
        WHEN total_spend >= 10000 THEN 'Premium'
        WHEN total_spend >= 5000 THEN 'Gold'
        WHEN total_spend >= 1000 THEN 'Silver'
        ELSE 'Bronze'
    END
$$;
""",
                parameters=["total_spend"],
                return_type="STRING",
                category="business",
                tags=["segmentation", "customer", "business_logic"],
                examples=[
                    {
                        "description": "Segment customer by spend",
                        "usage": "SELECT customer_segment(7500) as segment",
                        "expected": "'Gold'",
                    }
                ],
            ),
            "order_status": SQLFunction(
                name="order_status",
                description="Determine order status based on business rules",
                sql_definition="""
CREATE OR REPLACE FUNCTION order_status(
    order_date DATE,
    ship_date DATE,
    delivery_date DATE,
    cancel_date DATE
) RETURNS STRING AS $$
    SELECT CASE 
        WHEN cancel_date IS NOT NULL THEN 'Cancelled'
        WHEN delivery_date IS NOT NULL THEN 'Delivered'
        WHEN ship_date IS NOT NULL THEN 'Shipped'
        WHEN order_date IS NOT NULL THEN 'Ordered'
        ELSE 'Unknown'
    END
$$;
""",
                parameters=["order_date", "ship_date", "delivery_date", "cancel_date"],
                return_type="STRING",
                category="business",
                tags=["order_status", "business_logic", "status"],
                examples=[
                    {
                        "description": "Determine order status",
                        "usage": "SELECT order_status('2024-01-01', '2024-01-02', NULL, NULL) as status",
                        "expected": "'Shipped'",
                    }
                ],
            ),
        }

    def get_function(self, function_name: str) -> Optional[SQLFunction]:
        """Get a specific SQL function by name."""
        return self.functions.get(function_name)

    def list_functions(self, category: Optional[str] = None) -> List[SQLFunction]:
        """List all functions, optionally filtered by category."""
        if category:
            return [f for f in self.functions.values() if f.category == category]
        return list(self.functions.values())

    def render_function_definition(
        self, function_name: str, parameters: Dict[str, Any] = None
    ) -> str:
        """Render a SQL function definition."""
        function = self.get_function(function_name)
        if not function:
            raise ValueError(f"Function '{function_name}' not found")

        return function.sql_definition

    def get_function_examples(self, function_name: str) -> List[Dict[str, Any]]:
        """Get examples for a specific function."""
        function = self.get_function(function_name)
        return function.examples if function else []

    def search_functions(self, query: str) -> List[SQLFunction]:
        """Search functions by name, description, or tags."""
        query_lower = query.lower()
        results = []

        for function in self.functions.values():
            if (
                query_lower in function.name.lower()
                or query_lower in function.description.lower()
                or any(query_lower in tag.lower() for tag in function.tags)
            ):
                results.append(function)

        return results

    def create_function_library(self, output_file: str = None) -> str:
        """Create a complete SQL function library file."""
        library_sql = "-- SQL Function Library\n"
        library_sql += "-- Generated by Databricks Toolkit\n\n"

        for function in self.functions.values():
            library_sql += f"-- {function.name}: {function.description}\n"
            library_sql += function.sql_definition + "\n\n"

        if output_file:
            with open(output_file, "w") as f:
                f.write(library_sql)

        return library_sql
