"""
Data Quality Checks Module

This module provides comprehensive data quality checks leveraging
Databricks' built-in functionality for data validation.
"""

from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class QualityCheck:
    """Represents a data quality check with metadata."""
    name: str
    description: str
    sql_query: str
    parameters: List[str]
    severity: str  # 'error', 'warning', 'info'
    category: str
    expected_result: str
    examples: List[Dict[str, Any]]


class DataQualityChecks:
    """
    Comprehensive data quality checks for Databricks.
    
    This class provides standardized data quality checks that leverage
    Databricks' built-in functionality and follow data quality best practices.
    """
    
    def __init__(self):
        """Initialize data quality checks."""
        self.checks = self._load_quality_checks()
    
    def _load_quality_checks(self) -> Dict[str, QualityCheck]:
        """Load all data quality checks."""
        checks = {}
        
        # Completeness Checks
        checks.update(self._get_completeness_checks())
        
        # Accuracy Checks
        checks.update(self._get_accuracy_checks())
        
        # Consistency Checks
        checks.update(self._get_consistency_checks())
        
        # Validity Checks
        checks.update(self._get_validity_checks())
        
        # Uniqueness Checks
        checks.update(self._get_uniqueness_checks())
        
        # Timeliness Checks
        checks.update(self._get_timeliness_checks())
        
        return checks
    
    def _get_completeness_checks(self) -> Dict[str, QualityCheck]:
        """Get completeness-related quality checks."""
        return {
            'null_check': QualityCheck(
                name='Null Value Check',
                description='Check for null values in critical columns',
                sql_query="""
SELECT 
    '{column}' as column_name,
    COUNT(*) as total_rows,
    COUNT({column}) as non_null_count,
    COUNT(*) - COUNT({column}) as null_count,
    ROUND(COUNT({column}) * 100.0 / COUNT(*), 2) as completeness_percentage
FROM {table}
""",
                parameters=['table', 'column'],
                severity='error',
                category='completeness',
                expected_result='completeness_percentage >= 95',
                examples=[
                    {
                        'description': 'Check customer_id completeness',
                        'parameters': {
                            'table': 'hive_metastore.retail.customers_bronze',
                            'column': 'customer_id'
                        }
                    }
                ]
            ),
            
            'empty_string_check': QualityCheck(
                name='Empty String Check',
                description='Check for empty strings in text columns',
                sql_query="""
SELECT 
    '{column}' as column_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN TRIM({column}) = '' THEN 1 END) as empty_string_count,
    ROUND(COUNT(CASE WHEN TRIM({column}) = '' THEN 1 END) * 100.0 / COUNT(*), 2) as empty_string_percentage
FROM {table}
WHERE {column} IS NOT NULL
""",
                parameters=['table', 'column'],
                severity='warning',
                category='completeness',
                expected_result='empty_string_percentage <= 5',
                examples=[
                    {
                        'description': 'Check customer_name for empty strings',
                        'parameters': {
                            'table': 'hive_metastore.retail.customers_bronze',
                            'column': 'customer_name'
                        }
                    }
                ]
            )
        }
    
    def _get_accuracy_checks(self) -> Dict[str, QualityCheck]:
        """Get accuracy-related quality checks."""
        return {
            'range_check': QualityCheck(
                name='Range Check',
                description='Check if numeric values are within expected range',
                sql_query="""
SELECT 
    '{column}' as column_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN {column} BETWEEN {min_value} AND {max_value} THEN 1 END) as in_range_count,
    COUNT(CASE WHEN {column} NOT BETWEEN {min_value} AND {max_value} THEN 1 END) as out_of_range_count,
    ROUND(COUNT(CASE WHEN {column} BETWEEN {min_value} AND {max_value} THEN 1 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
FROM {table}
WHERE {column} IS NOT NULL
""",
                parameters=['table', 'column', 'min_value', 'max_value'],
                severity='error',
                category='accuracy',
                expected_result='accuracy_percentage >= 95',
                examples=[
                    {
                        'description': 'Check order amounts are between 0 and 10000',
                        'parameters': {
                            'table': 'hive_metastore.retail.orders_bronze',
                            'column': 'order_amount',
                            'min_value': 0,
                            'max_value': 10000
                        }
                    }
                ]
            ),
            
            'format_check': QualityCheck(
                name='Format Check',
                description='Check if values match expected format using regex',
                sql_query="""
SELECT 
    '{column}' as column_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN {column} REGEXP '{regex_pattern}' THEN 1 END) as valid_format_count,
    COUNT(CASE WHEN {column} NOT REGEXP '{regex_pattern}' THEN 1 END) as invalid_format_count,
    ROUND(COUNT(CASE WHEN {column} REGEXP '{regex_pattern}' THEN 1 END) * 100.0 / COUNT(*), 2) as format_accuracy_percentage
FROM {table}
WHERE {column} IS NOT NULL
""",
                parameters=['table', 'column', 'regex_pattern'],
                severity='error',
                category='accuracy',
                expected_result='format_accuracy_percentage >= 95',
                examples=[
                    {
                        'description': 'Check email format',
                        'parameters': {
                            'table': 'hive_metastore.retail.customers_bronze',
                            'column': 'email',
                            'regex_pattern': '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
                        }
                    }
                ]
            )
        }
    
    def _get_consistency_checks(self) -> Dict[str, QualityCheck]:
        """Get consistency-related quality checks."""
        return {
            'case_consistency': QualityCheck(
                name='Case Consistency Check',
                description='Check for consistent case in text columns',
                sql_query="""
SELECT 
    '{column}' as column_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT {column}) as unique_values,
    COUNT(DISTINCT UPPER({column})) as unique_upper_values,
    CASE 
        WHEN COUNT(DISTINCT {column}) = COUNT(DISTINCT UPPER({column})) 
        THEN 'consistent' 
        ELSE 'inconsistent' 
    END as case_consistency
FROM {table}
WHERE {column} IS NOT NULL
""",
                parameters=['table', 'column'],
                severity='warning',
                category='consistency',
                expected_result='case_consistency = consistent',
                examples=[
                    {
                        'description': 'Check country names for case consistency',
                        'parameters': {
                            'table': 'hive_metastore.retail.customers_bronze',
                            'column': 'country'
                        }
                    }
                ]
            ),
            
            'data_type_consistency': QualityCheck(
                name='Data Type Consistency Check',
                description='Check if all values in a column are of the same data type',
                sql_query="""
SELECT 
    '{column}' as column_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN {column} REGEXP '^[0-9]+$' THEN 1 END) as numeric_count,
    COUNT(CASE WHEN {column} NOT REGEXP '^[0-9]+$' THEN 1 END) as non_numeric_count,
    CASE 
        WHEN COUNT(CASE WHEN {column} REGEXP '^[0-9]+$' THEN 1 END) = COUNT(*) 
        THEN 'consistent' 
        ELSE 'inconsistent' 
    END as type_consistency
FROM {table}
WHERE {column} IS NOT NULL
""",
                parameters=['table', 'column'],
                severity='error',
                category='consistency',
                expected_result='type_consistency = consistent',
                examples=[
                    {
                        'description': 'Check customer_id is consistently numeric',
                        'parameters': {
                            'table': 'hive_metastore.retail.customers_bronze',
                            'column': 'customer_id'
                        }
                    }
                ]
            )
        }
    
    def _get_validity_checks(self) -> Dict[str, QualityCheck]:
        """Get validity-related quality checks."""
        return {
            'domain_check': QualityCheck(
                name='Domain Value Check',
                description='Check if values are within expected domain',
                sql_query="""
SELECT 
    '{column}' as column_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN {column} IN ({valid_values}) THEN 1 END) as valid_count,
    COUNT(CASE WHEN {column} NOT IN ({valid_values}) THEN 1 END) as invalid_count,
    ROUND(COUNT(CASE WHEN {column} IN ({valid_values}) THEN 1 END) * 100.0 / COUNT(*), 2) as validity_percentage
FROM {table}
WHERE {column} IS NOT NULL
""",
                parameters=['table', 'column', 'valid_values'],
                severity='error',
                category='validity',
                expected_result='validity_percentage >= 95',
                examples=[
                    {
                        'description': 'Check status values are valid',
                        'parameters': {
                            'table': 'hive_metastore.retail.orders_bronze',
                            'column': 'status',
                            'valid_values': "'pending', 'completed', 'cancelled'"
                        }
                    }
                ]
            ),
            
            'length_check': QualityCheck(
                name='Length Check',
                description='Check if string values are within expected length range',
                sql_query="""
SELECT 
    '{column}' as column_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN LENGTH({column}) BETWEEN {min_length} AND {max_length} THEN 1 END) as valid_length_count,
    COUNT(CASE WHEN LENGTH({column}) NOT BETWEEN {min_length} AND {max_length} THEN 1 END) as invalid_length_count,
    ROUND(COUNT(CASE WHEN LENGTH({column}) BETWEEN {min_length} AND {max_length} THEN 1 END) * 100.0 / COUNT(*), 2) as length_validity_percentage
FROM {table}
WHERE {column} IS NOT NULL
""",
                parameters=['table', 'column', 'min_length', 'max_length'],
                severity='warning',
                category='validity',
                expected_result='length_validity_percentage >= 90',
                examples=[
                    {
                        'description': 'Check phone numbers are 10-15 digits',
                        'parameters': {
                            'table': 'hive_metastore.retail.customers_bronze',
                            'column': 'phone',
                            'min_length': 10,
                            'max_length': 15
                        }
                    }
                ]
            )
        }
    
    def _get_uniqueness_checks(self) -> Dict[str, QualityCheck]:
        """Get uniqueness-related quality checks."""
        return {
            'primary_key_uniqueness': QualityCheck(
                name='Primary Key Uniqueness Check',
                description='Check if primary key values are unique',
                sql_query="""
SELECT 
    '{column}' as column_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT {column}) as unique_values,
    COUNT(*) - COUNT(DISTINCT {column}) as duplicate_count,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT {column}) 
        THEN 'unique' 
        ELSE 'duplicates_found' 
    END as uniqueness_status
FROM {table}
""",
                parameters=['table', 'column'],
                severity='error',
                category='uniqueness',
                expected_result='uniqueness_status = unique',
                examples=[
                    {
                        'description': 'Check customer_id uniqueness',
                        'parameters': {
                            'table': 'hive_metastore.retail.customers_bronze',
                            'column': 'customer_id'
                        }
                    }
                ]
            ),
            
            'composite_key_uniqueness': QualityCheck(
                name='Composite Key Uniqueness Check',
                description='Check if composite key combinations are unique',
                sql_query="""
SELECT 
    '{columns}' as composite_key,
    COUNT(*) as total_rows,
    COUNT(DISTINCT CONCAT({columns})) as unique_combinations,
    COUNT(*) - COUNT(DISTINCT CONCAT({columns})) as duplicate_count,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT CONCAT({columns})) 
        THEN 'unique' 
        ELSE 'duplicates_found' 
    END as uniqueness_status
FROM {table}
""",
                parameters=['table', 'columns'],
                severity='error',
                category='uniqueness',
                expected_result='uniqueness_status = unique',
                examples=[
                    {
                        'description': 'Check order_id and product_id combination uniqueness',
                        'parameters': {
                            'table': 'hive_metastore.retail.order_items_bronze',
                            'columns': 'order_id, product_id'
                        }
                    }
                ]
            )
        }
    
    def _get_timeliness_checks(self) -> Dict[str, QualityCheck]:
        """Get timeliness-related quality checks."""
        return {
            'data_freshness': QualityCheck(
                name='Data Freshness Check',
                description='Check if data is fresh (not older than expected)',
                sql_query="""
SELECT 
    '{timestamp_column}' as timestamp_column,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN {timestamp_column} >= '{min_timestamp}' THEN 1 END) as fresh_count,
    COUNT(CASE WHEN {timestamp_column} < '{min_timestamp}' THEN 1 END) as stale_count,
    ROUND(COUNT(CASE WHEN {timestamp_column} >= '{min_timestamp}' THEN 1 END) * 100.0 / COUNT(*), 2) as freshness_percentage
FROM {table}
WHERE {timestamp_column} IS NOT NULL
""",
                parameters=['table', 'timestamp_column', 'min_timestamp'],
                severity='warning',
                category='timeliness',
                expected_result='freshness_percentage >= 80',
                examples=[
                    {
                        'description': 'Check if orders are from last 30 days',
                        'parameters': {
                            'table': 'hive_metastore.retail.orders_bronze',
                            'timestamp_column': 'order_date',
                            'min_timestamp': '2024-01-01'
                        }
                    }
                ]
            ),
            
            'update_frequency': QualityCheck(
                name='Update Frequency Check',
                description='Check if data is updated frequently enough',
                sql_query="""
SELECT 
    '{timestamp_column}' as timestamp_column,
    COUNT(*) as total_rows,
    MAX({timestamp_column}) as latest_update,
    MIN({timestamp_column}) as earliest_update,
    DATEDIFF(MAX({timestamp_column}), MIN({timestamp_column})) as days_span,
    COUNT(*) / DATEDIFF(MAX({timestamp_column}), MIN({timestamp_column})) as avg_updates_per_day
FROM {table}
WHERE {timestamp_column} IS NOT NULL
""",
                parameters=['table', 'timestamp_column'],
                severity='info',
                category='timeliness',
                expected_result='avg_updates_per_day >= 1',
                examples=[
                    {
                        'description': 'Check order update frequency',
                        'parameters': {
                            'table': 'hive_metastore.retail.orders_bronze',
                            'timestamp_column': 'updated_at'
                        }
                    }
                ]
            )
        }
    
    def get_check(self, check_name: str) -> Optional[QualityCheck]:
        """Get a specific quality check by name."""
        return self.checks.get(check_name)
    
    def list_checks(self, category: Optional[str] = None, severity: Optional[str] = None) -> List[QualityCheck]:
        """List all quality checks, optionally filtered by category or severity."""
        checks = self.checks.values()
        
        if category:
            checks = [c for c in checks if c.category == category]
        
        if severity:
            checks = [c for c in checks if c.severity == severity]
        
        return list(checks)
    
    def render_check(self, check_name: str, parameters: Dict[str, Any]) -> str:
        """Render a quality check with the given parameters."""
        check = self.get_check(check_name)
        if not check:
            raise ValueError(f"Quality check '{check_name}' not found")
        
        # Validate required parameters
        missing_params = [p for p in check.parameters if p not in parameters]
        if missing_params:
            raise ValueError(f"Missing required parameters: {missing_params}")
        
        # Render the SQL query
        return check.sql_query.format(**parameters)
    
    def run_quality_suite(self, table_name: str, checks: List[str], parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Run a suite of quality checks on a table."""
        results = {
            'table_name': table_name,
            'timestamp': datetime.now().isoformat(),
            'checks': {},
            'summary': {
                'total_checks': len(checks),
                'passed_checks': 0,
                'failed_checks': 0,
                'warning_checks': 0
            }
        }
        
        for check_name in checks:
            check = self.get_check(check_name)
            if check:
                # Add table parameter to all checks
                check_params = parameters.copy()
                check_params['table'] = table_name
                
                results['checks'][check_name] = {
                    'name': check.name,
                    'description': check.description,
                    'severity': check.severity,
                    'category': check.category,
                    'sql_query': self.render_check(check_name, check_params),
                    'parameters': check_params
                }
        
        return results
    
    def get_check_examples(self, check_name: str) -> List[Dict[str, Any]]:
        """Get examples for a specific quality check."""
        check = self.get_check(check_name)
        return check.examples if check else []
    
    def search_checks(self, query: str) -> List[QualityCheck]:
        """Search quality checks by name, description, or category."""
        query_lower = query.lower()
        results = []
        
        for check in self.checks.values():
            if (query_lower in check.name.lower() or
                query_lower in check.description.lower() or
                query_lower in check.category.lower()):
                results.append(check)
        
        return results 