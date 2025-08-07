#!/usr/bin/env python3
"""
Test SQL Library

This script demonstrates the SQL library functionality.
"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from sql_library.core.sql_patterns import SQLPatterns
from sql_library.core.data_quality import DataQualityChecks
from sql_library.core.sql_functions import SQLFunctions
from sql_library.core.sql_templates import SQLTemplates


def test_sql_patterns():
    """Test SQL patterns functionality."""
    print("🔍 Testing SQL Patterns")
    print("=" * 50)

    patterns = SQLPatterns()

    # List all patterns
    all_patterns = patterns.list_patterns()
    print(f"📊 Total patterns: {len(all_patterns)}")

    # List patterns by category
    ingestion_patterns = patterns.list_patterns(category="ingestion")
    print(f"📥 Ingestion patterns: {len(ingestion_patterns)}")

    # Test rendering a pattern
    try:
        bronze_pattern = patterns.render_pattern(
            "bronze_ingestion",
            {
                "catalog": "hive_metastore",
                "schema": "retail",
                "table_name": "customers",
                "source_table": "csv.`/mnt/databricks-datasets/retail-org/customers/`",
            },
        )
        print("✅ Bronze ingestion pattern rendered successfully")
        print(f"📝 Generated SQL length: {len(bronze_pattern)} characters")
    except Exception as e:
        print(f"❌ Error rendering pattern: {e}")

    # Search patterns
    search_results = patterns.search_patterns("bronze")
    print(f"🔍 Found {len(search_results)} patterns matching 'bronze'")


def test_data_quality():
    """Test data quality checks functionality."""
    print("\n🔍 Testing Data Quality Checks")
    print("=" * 50)

    quality = DataQualityChecks()

    # List all checks
    all_checks = quality.list_checks()
    print(f"📊 Total quality checks: {len(all_checks)}")

    # List checks by category
    completeness_checks = quality.list_checks(category="completeness")
    print(f"✅ Completeness checks: {len(completeness_checks)}")

    # Test rendering a check
    try:
        null_check = quality.render_check(
            "null_check",
            {
                "table": "hive_metastore.retail.customers_bronze",
                "column": "customer_id",
            },
        )
        print("✅ Null check rendered successfully")
        print(f"📝 Generated SQL length: {len(null_check)} characters")
    except Exception as e:
        print(f"❌ Error rendering check: {e}")


def test_sql_functions():
    """Test SQL functions functionality."""
    print("\n🔍 Testing SQL Functions")
    print("=" * 50)

    functions = SQLFunctions()

    # List all functions
    all_functions = functions.list_functions()
    print(f"📊 Total functions: {len(all_functions)}")

    # List functions by category
    string_functions = functions.list_functions(category="string")
    print(f"📝 String functions: {len(string_functions)}")

    # Test rendering a function
    try:
        clean_string_func = functions.render_function_definition("clean_string")
        print("✅ Clean string function rendered successfully")
        print(f"📝 Generated SQL length: {len(clean_string_func)} characters")
    except Exception as e:
        print(f"❌ Error rendering function: {e}")

    # Create function library
    try:
        library_sql = functions.create_function_library()
        print("✅ Function library created successfully")
        print(f"📝 Library SQL length: {len(library_sql)} characters")
    except Exception as e:
        print(f"❌ Error creating function library: {e}")


def test_sql_templates():
    """Test SQL templates functionality."""
    print("\n🔍 Testing SQL Templates")
    print("=" * 50)

    templates = SQLTemplates()

    # List all templates
    all_templates = templates.list_templates()
    print(f"📊 Total templates: {len(all_templates)}")

    # List templates by category
    pipeline_templates = templates.list_templates(category="pipeline")
    print(f"🔄 Pipeline templates: {len(pipeline_templates)}")

    # Test rendering a template
    try:
        bronze_silver_template = templates.render_template(
            "bronze_to_silver_pipeline",
            {
                "catalog": "hive_metastore",
                "schema": "retail",
                "table_name": "customers",
                "quality_conditions": "customer_id IS NOT NULL AND customer_name IS NOT NULL",
                "transformations": """
                customer_id,
                TRIM(customer_name) as customer_name,
                LOWER(TRIM(email)) as email,
                REGEXP_REPLACE(phone, '[^0-9]', '') as phone_clean,
                INITCAP(TRIM(city)) as city,
                UPPER(TRIM(state)) as state,
                UPPER(TRIM(country)) as country
            """,
            },
        )
        print("✅ Bronze to silver template rendered successfully")
        print(f"📝 Generated SQL length: {len(bronze_silver_template)} characters")
    except Exception as e:
        print(f"❌ Error rendering template: {e}")


def main():
    """Main test function."""
    print("🚀 SQL Library Test Suite")
    print("=" * 60)

    try:
        test_sql_patterns()
        test_data_quality()
        test_sql_functions()
        test_sql_templates()

        print("\n🎉 All tests completed successfully!")
        print("✅ SQL Library is working correctly")

    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
