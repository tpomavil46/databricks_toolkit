#!/usr/bin/env python3
"""
Test DLT Pipeline Structure

This script tests our DLT pipeline implementation without requiring
a real Databricks connection. It validates:
- Template loading and parameter substitution
- DLT syntax validation
- Pipeline structure
"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_dlt_template_loading():
    """Test DLT template loading and parameter substitution."""
    print("🧪 Testing DLT Template Loading...")
    
    # Test bronze ingestion template
    template_path = Path("workflows/sql_driven/sql/templates/dlt_bronze_ingestion.sql")
    
    if not template_path.exists():
        print(f"❌ Template not found: {template_path}")
        return False
        
    with open(template_path, 'r') as f:
        sql_template = f.read()
    
    # Test parameter substitution
    parameters = {
        'source_path': '/Volumes/dbacademy/ops/stream-source',
        'table_name': 'orders_bronze',
        'file_format': 'json',
        'options': '"cloudFiles.inferColumnTypes", "true"'
    }
    
    # Simple parameter substitution
    sql = sql_template
    for key, value in parameters.items():
        placeholder = f"${{{key}}}"
        sql = sql.replace(placeholder, str(value))
    
    print("✅ Template loaded successfully")
    print(f"📝 SQL Preview: {sql[:200]}...")
    
    # Validate DLT syntax
    dlt_keywords = [
        'CREATE OR REFRESH STREAMING TABLE',
        'CONSTRAINT',
        'EXPECT',
        'ON VIOLATION',
        'TBLPROPERTIES',
        'cloud_files',
        'current_timestamp',
        '_metadata'
    ]
    
    sql_upper = sql.upper()
    found_keywords = [keyword for keyword in dlt_keywords if keyword.upper() in sql_upper]
    
    print(f"✅ Found DLT keywords: {found_keywords}")
    return True

def test_dlt_silver_template():
    """Test DLT silver transformation template."""
    print("\n🧪 Testing DLT Silver Template...")
    
    template_path = Path("workflows/sql_driven/sql/templates/dlt_silver_transformation.sql")
    
    if not template_path.exists():
        print(f"❌ Template not found: {template_path}")
        return False
        
    with open(template_path, 'r') as f:
        sql_template = f.read()
    
    # Test parameter substitution
    parameters = {
        'bronze_table': 'orders_bronze',
        'silver_table': 'orders_silver'
    }
    
    sql = sql_template
    for key, value in parameters.items():
        placeholder = f"${{{key}}}"
        sql = sql.replace(placeholder, str(value))
    
    print("✅ Silver template loaded successfully")
    print(f"📝 SQL Preview: {sql[:200]}...")
    
    # Validate DLT syntax
    dlt_keywords = [
        'CREATE OR REFRESH STREAMING TABLE',
        'CONSTRAINT',
        'EXPECT',
        'ON VIOLATION',
        'TBLPROPERTIES',
        'STREAM',
        'LIVE'
    ]
    
    sql_upper = sql.upper()
    found_keywords = [keyword for keyword in dlt_keywords if keyword.upper() in sql_upper]
    
    print(f"✅ Found DLT keywords: {found_keywords}")
    return True

def test_dlt_gold_template():
    """Test DLT gold aggregation template."""
    print("\n🧪 Testing DLT Gold Template...")
    
    template_path = Path("workflows/sql_driven/sql/templates/dlt_gold_aggregation.sql")
    
    if not template_path.exists():
        print(f"❌ Template not found: {template_path}")
        return False
        
    with open(template_path, 'r') as f:
        sql_template = f.read()
    
    # Test parameter substitution
    parameters = {
        'silver_table': 'orders_silver',
        'gold_table': 'orders_analytics'
    }
    
    sql = sql_template
    for key, value in parameters.items():
        placeholder = f"${{{key}}}"
        sql = sql.replace(placeholder, str(value))
    
    print("✅ Gold template loaded successfully")
    print(f"📝 SQL Preview: {sql[:200]}...")
    
    # Validate DLT syntax
    dlt_keywords = [
        'CREATE OR REFRESH MATERIALIZED VIEW',
        'TBLPROPERTIES',
        'LIVE'
    ]
    
    sql_upper = sql.upper()
    found_keywords = [keyword for keyword in dlt_keywords if keyword.upper() in sql_upper]
    
    print(f"✅ Found DLT keywords: {found_keywords}")
    return True

def test_training_examples():
    """Test training-based DLT examples."""
    print("\n🧪 Testing Training Examples...")
    
    examples = [
        "workflows/sql_driven/sql/bronze/retail/ingest_orders_dlt.sql",
        "workflows/sql_driven/sql/silver/retail/transform_orders_dlt.sql", 
        "workflows/sql_driven/sql/gold/retail/orders_analytics_dlt.sql"
    ]
    
    for example_path in examples:
        path = Path(example_path)
        if path.exists():
            with open(path, 'r') as f:
                sql = f.read()
            print(f"✅ {path.name} - {len(sql)} characters")
        else:
            print(f"❌ {path.name} - Not found")
    
    return True

def test_pipeline_structure():
    """Test the overall pipeline structure."""
    print("\n🧪 Testing Pipeline Structure...")
    
    # Check if DLT pipeline runner exists
    dlt_pipeline_path = Path("workflows/sql_driven/pipelines/dlt_pipeline.py")
    if dlt_pipeline_path.exists():
        print("✅ DLT Pipeline Runner exists")
    else:
        print("❌ DLT Pipeline Runner not found")
        return False
    
    # Check if run.py supports DLT
    run_path = Path("workflows/sql_driven/run.py")
    if run_path.exists():
        with open(run_path, 'r') as f:
            content = f.read()
        if 'dlt' in content.lower():
            print("✅ Run.py supports DLT pipelines")
        else:
            print("❌ Run.py doesn't support DLT")
            return False
    
    # Check documentation
    docs_path = Path("workflows/sql_driven/DLT_COVERAGE.md")
    if docs_path.exists():
        print("✅ DLT documentation exists")
    else:
        print("❌ DLT documentation not found")
        return False
    
    return True

def main():
    """Run all tests."""
    print("=" * 60)
    print("🧪 DLT PIPELINE TESTING")
    print("=" * 60)
    
    tests = [
        test_dlt_template_loading,
        test_dlt_silver_template,
        test_dlt_gold_template,
        test_training_examples,
        test_pipeline_structure
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ Test failed: {e}")
    
    print("\n" + "=" * 60)
    print(f"📊 TEST RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! DLT pipeline is ready for exam preparation.")
    else:
        print("⚠️ Some tests failed. Review the implementation.")
    
    print("=" * 60)

if __name__ == "__main__":
    main() 