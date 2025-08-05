"""
Example Usage of Standardized ETL Framework

This example demonstrates how to use the new standardized ETL framework
with parameterized configurations and reusable patterns.
"""

import os
from typing import Dict, Any
from etl.core.config import PipelineConfig, create_default_config
from etl.core.etl_pipeline import StandardETLPipeline
from etl.jobs.bronze.ingestion import BronzeIngestionJob
from etl.jobs.silver.transformation import SilverTransformationJob
from etl.jobs.gold.aggregation import GoldAggregationJob


def example_bronze_ingestion():
    """Example of bronze ingestion using the standardized framework."""
    print("🛢️  Example: Bronze Ingestion")
    print("=" * 50)
    
    # Create configuration
    config = create_default_config("retail", "dev")
    
    # Create pipeline
    pipeline = StandardETLPipeline(config)
    
    # Run bronze ingestion
    source_path = "dbfs:/databricks-datasets/retail-org/customers/"
    results = pipeline.run_bronze_ingestion(source_path)
    
    print(f"✅ Bronze ingestion completed")
    print(f"📊 Table: {results['table_name']}")
    print(f"📈 Rows: {results['row_count']:,}")
    print(f"🎯 Quality Score: {results['validation_results']['quality_score']:.1f}/100")


def example_silver_transformation():
    """Example of silver transformation using the standardized framework."""
    print("\n⚙️  Example: Silver Transformation")
    print("=" * 50)
    
    # Create configuration
    config = create_default_config("retail", "dev")
    
    # Create pipeline
    pipeline = StandardETLPipeline(config)
    
    # Define transformations
    transformations = {
        'null_strategy': {
            'customer_name': 'fill_default',
            'state': 'fill_default',
            'units_purchased': 'fill_mean'
        },
        'type_mapping': {
            'units_purchased': 'integer',
            'loyalty_segment': 'integer'
        },
        'filter_conditions': {
            'customer_name': {'not_equals': ''},
            'state': {'not_equals': ''}
        },
        'deduplication_keys': ['customer_id'],
        'column_mapping': {
            'customer_name': 'customer_name_clean',
            'units_purchased': 'total_units'
        }
    }
    
    # Run silver transformation
    source_table = "retail_dev_data_bronze"
    results = pipeline.run_silver_transformation(source_table, transformations=transformations)
    
    print(f"✅ Silver transformation completed")
    print(f"📊 Table: {results['table_name']}")
    print(f"📈 Rows: {results['row_count']:,}")
    print(f"🎯 Quality Score: {results['validation_results']['quality_score']:.1f}/100")


def example_gold_aggregation():
    """Example of gold aggregation using the standardized framework."""
    print("\n🏆 Example: Gold Aggregation")
    print("=" * 50)
    
    # Create configuration
    config = create_default_config("retail", "dev")
    
    # Create pipeline
    pipeline = StandardETLPipeline(config)
    
    # Define aggregations
    from pyspark.sql.functions import sum, avg, count, max
    
    aggregations = {
        'group_by': ['state', 'loyalty_segment'],
        'aggregations': {
            'total_units': sum,
            'avg_units': avg,
            'customer_count': count,
            'max_units': max
        }
    }
    
    # Run gold aggregation
    source_table = "retail_dev_data_silver"
    results = pipeline.run_gold_aggregation(source_table, aggregations=aggregations)
    
    print(f"✅ Gold aggregation completed")
    print(f"📊 Table: {results['table_name']}")
    print(f"📈 Rows: {results['row_count']:,}")
    print(f"🎯 Quality Score: {results['validation_results']['quality_score']:.1f}/100")


def example_full_pipeline():
    """Example of complete ETL pipeline using the standardized framework."""
    print("\n🚀 Example: Full ETL Pipeline")
    print("=" * 50)
    
    # Create configuration
    config = create_default_config("retail", "dev")
    
    # Create pipeline
    pipeline = StandardETLPipeline(config)
    
    # Define transformations and aggregations
    transformations = {
        'null_strategy': {
            'customer_name': 'fill_default',
            'state': 'fill_default'
        },
        'type_mapping': {
            'units_purchased': 'integer'
        },
        'filter_conditions': {
            'customer_name': {'not_equals': ''}
        }
    }
    
    from pyspark.sql.functions import sum, count
    
    aggregations = {
        'group_by': ['state'],
        'aggregations': {
            'total_units': sum,
            'customer_count': count
        }
    }
    
    # Run full pipeline
    source_path = "dbfs:/databricks-datasets/retail-org/customers/"
    results = pipeline.run_full_pipeline(source_path, transformations, aggregations)
    
    print(f"✅ Full ETL pipeline completed")
    print(f"📊 Total rows processed: {results['total_rows_processed']:,}")
    print(f"🎯 Overall status: {results['overall_status']}")


def example_individual_jobs():
    """Example of using individual job classes."""
    print("\n🔧 Example: Individual Job Classes")
    print("=" * 50)
    
    # Bronze ingestion job
    bronze_job = BronzeIngestionJob("retail", "dev")
    bronze_results = bronze_job.run("dbfs:/databricks-datasets/retail-org/customers/")
    print(f"✅ Bronze job completed: {bronze_results['table_name']}")
    
    # Silver transformation job
    silver_job = SilverTransformationJob("retail", "dev")
    silver_results = silver_job.run(bronze_results['table_name'])
    print(f"✅ Silver job completed: {silver_results['table_name']}")
    
    # Gold aggregation job
    gold_job = GoldAggregationJob("retail", "dev")
    gold_results = gold_job.run(silver_results['table_name'])
    print(f"✅ Gold job completed: {gold_results['table_name']}")


def example_environment_configuration():
    """Example of environment-based configuration."""
    print("\n🌍 Example: Environment Configuration")
    print("=" * 50)
    
    # Set environment variables
    os.environ['ENVIRONMENT'] = 'staging'
    os.environ['DATABRICKS_CLUSTER_ID'] = '5802-005055-h7vtizbe'
    os.environ['QUALITY_THRESHOLD'] = '0.98'
    
    # Create configuration for staging environment
    config = create_default_config("retail", "staging")
    
    print(f"📁 Project: {config.project_name}")
    print(f"🌍 Environment: {config.table_config.environment}")
    print(f"🔧 Cluster ID: {config.cluster_config.cluster_id}")
    print(f"🎯 Quality Threshold: {config.validation_config.quality_threshold}")
    
    # Save configuration to file
    config.save_to_file("config/retail/staging.json")
    print(f"💾 Configuration saved to config/retail/staging.json")


def main():
    """Run all examples."""
    print("🎯 ETL Framework Examples")
    print("=" * 60)
    
    try:
        # Run examples
        example_bronze_ingestion()
        example_silver_transformation()
        example_gold_aggregation()
        example_full_pipeline()
        example_individual_jobs()
        example_environment_configuration()
        
        print("\n✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Example failed: {str(e)}")
        print("💡 Make sure your Databricks connection is configured correctly")


if __name__ == "__main__":
    main() 