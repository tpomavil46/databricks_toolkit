"""
End-to-End Test with Actual Data

This script tests the ETL framework with real data to ensure
the complete pipeline works correctly.
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl.core.config import create_default_config
from etl.core.etl_pipeline import StandardETLPipeline
from etl.jobs.bronze.ingestion import BronzeIngestionJob


def test_bronze_ingestion():
    """Test bronze ingestion with actual data."""
    print("🛢️  Testing Bronze Ingestion with Real Data")
    print("=" * 60)

    try:
        # Create configuration
        config = create_default_config("test_retail", "dev")

        # Create pipeline
        pipeline = StandardETLPipeline(config)

        # Test with retail data
        source_path = "dbfs:/databricks-datasets/retail-org/customers/"

        print(f"📁 Source: {source_path}")
        print(f"🏷️  Project: {config.project_name}")
        print(f"🌍 Environment: {config.table_config.environment}")
        print("🔄 Running bronze ingestion...")

        # Run bronze ingestion with auto_drop=True
        results = pipeline.run_bronze_ingestion(source_path, auto_drop=True)

        print(f"✅ Bronze ingestion completed successfully!")
        print(f"📊 Table: {results['table_name']}")
        print(f"📈 Rows: {results['row_count']:,}")
        print(
            f"🎯 Quality Score: {results['validation_results']['quality_score']:.1f}/100"
        )

        return results["table_name"]

    except Exception as e:
        print(f"❌ Bronze ingestion failed: {str(e)}")
        return None


def test_silver_transformation(bronze_table):
    """Test silver transformation with actual data."""
    print("\n⚙️  Testing Silver Transformation with Real Data")
    print("=" * 60)

    try:
        # Create configuration
        config = create_default_config("test_retail", "dev")

        # Create pipeline
        pipeline = StandardETLPipeline(config)

        # Define transformations
        transformations = {
            "null_strategy": {"customer_name": "fill_default", "state": "fill_default"},
            "type_mapping": {"units_purchased": "integer"},
            "filter_conditions": {"customer_name": {"not_equals": ""}},
        }

        print(f"📁 Source: {bronze_table}")
        print(f"🔄 Running silver transformation...")

        # Run silver transformation with auto_drop=True
        results = pipeline.run_silver_transformation(
            bronze_table, transformations=transformations, auto_drop=True
        )

        print(f"✅ Silver transformation completed successfully!")
        print(f"📊 Table: {results['table_name']}")
        print(f"📈 Rows: {results['row_count']:,}")
        print(
            f"🎯 Quality Score: {results['validation_results']['quality_score']:.1f}/100"
        )

        return results["table_name"]

    except Exception as e:
        print(f"❌ Silver transformation failed: {str(e)}")
        return None


def test_gold_aggregation(silver_table):
    """Test gold aggregation with actual data."""
    print("\n🏆 Testing Gold Aggregation with Real Data")
    print("=" * 60)

    try:
        # Create configuration
        config = create_default_config("test_retail", "dev")

        # Create pipeline
        pipeline = StandardETLPipeline(config)

        # Define aggregations
        from pyspark.sql.functions import sum, count

        aggregations = {
            "group_by": ["state"],
            "aggregations": {"total_units": sum, "customer_count": count},
        }

        print(f"📁 Source: {silver_table}")
        print(f"🔄 Running gold aggregation...")

        # Run gold aggregation with auto_drop=True
        results = pipeline.run_gold_aggregation(
            silver_table, aggregations=aggregations, auto_drop=True
        )

        print(f"✅ Gold aggregation completed successfully!")
        print(f"📊 Table: {results['table_name']}")
        print(f"📈 Rows: {results['row_count']:,}")
        print(
            f"🎯 Quality Score: {results['validation_results']['quality_score']:.1f}/100"
        )

        return results["table_name"]

    except Exception as e:
        print(f"❌ Gold aggregation failed: {str(e)}")
        return None


def test_individual_jobs():
    """Test individual job classes with actual data."""
    print("\n🔧 Testing Individual Job Classes with Real Data")
    print("=" * 60)

    try:
        # Test bronze job
        print("🛢️  Testing Bronze Job...")
        bronze_job = BronzeIngestionJob("test_retail", "dev")
        bronze_results = bronze_job.run(
            "dbfs:/databricks-datasets/retail-org/customers/", auto_drop=True
        )
        print(f"✅ Bronze job completed: {bronze_results['table_name']}")

        # Test silver job
        print("⚙️  Testing Silver Job...")
        from etl.jobs.silver.transformation import SilverTransformationJob

        silver_job = SilverTransformationJob("test_retail", "dev")
        silver_results = silver_job.run(bronze_results["table_name"], auto_drop=True)
        print(f"✅ Silver job completed: {silver_results['table_name']}")

        # Test gold job
        print("🏆 Testing Gold Job...")
        from etl.jobs.gold.aggregation import GoldAggregationJob

        gold_job = GoldAggregationJob("test_retail", "dev")
        gold_results = gold_job.run(silver_results["table_name"], auto_drop=True)
        print(f"✅ Gold job completed: {gold_results['table_name']}")

        return True

    except Exception as e:
        print(f"❌ Individual job test failed: {str(e)}")
        return False


def test_full_pipeline():
    """Test complete ETL pipeline with actual data."""
    print("\n🚀 Testing Full ETL Pipeline with Real Data")
    print("=" * 60)

    try:
        # Create configuration
        config = create_default_config("test_retail", "dev")

        # Create pipeline
        pipeline = StandardETLPipeline(config)

        # Define transformations and aggregations
        transformations = {
            "null_strategy": {"customer_name": "fill_default", "state": "fill_default"},
            "type_mapping": {"units_purchased": "integer"},
            "filter_conditions": {"customer_name": {"not_equals": ""}},
        }

        from pyspark.sql.functions import sum, count

        aggregations = {
            "group_by": ["state"],
            "aggregations": {"total_units": sum, "customer_count": count},
        }

        source_path = "dbfs:/databricks-datasets/retail-org/customers/"

        print(f"📁 Source: {source_path}")
        print(f"🔄 Running full pipeline...")

        # Run full pipeline with auto_drop=True
        results = pipeline.run_full_pipeline(
            source_path, transformations, aggregations, auto_drop=True
        )

        print(f"✅ Full pipeline completed successfully!")
        print(f"📊 Total rows processed: {results['total_rows_processed']:,}")
        print(f"🎯 Overall status: {results['overall_status']}")

        # Print pipeline summary
        pipeline.print_pipeline_summary()

        return True

    except Exception as e:
        print(f"❌ Full pipeline test failed: {str(e)}")
        return False


def main():
    """Run all data tests."""
    print("🧪 ETL Framework Data Test Suite")
    print("=" * 60)

    tests = [
        ("Bronze Ingestion Test", test_bronze_ingestion),
        ("Individual Jobs Test", test_individual_jobs),
        ("Full Pipeline Test", test_full_pipeline),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n🧪 Running: {test_name}")
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"❌ {test_name} FAILED with exception: {str(e)}")

    print(f"\n📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print(
            "🎉 All data tests passed! ETL framework is working correctly with real data."
        )
        return True
    else:
        print("⚠️  Some data tests failed. Please review the errors above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
