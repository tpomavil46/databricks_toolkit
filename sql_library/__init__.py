"""
SQL Library Package

This package provides a comprehensive SQL library for Databricks including
standardized patterns, data quality checks, parameterized templates, and
reusable SQL functions.
"""

__version__ = "1.0.0"
__author__ = "Databricks Toolkit Team"

# Import main components
from .core.sql_patterns import SQLPatterns
from .core.data_quality import DataQualityChecks
from .core.sql_functions import SQLFunctions
from .core.sql_templates import SQLTemplates

__all__ = [
    'SQLPatterns',
    'DataQualityChecks', 
    'SQLFunctions',
    'SQLTemplates'
] 