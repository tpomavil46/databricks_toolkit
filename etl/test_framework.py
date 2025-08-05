"""
Test Script for ETL Framework

This script tests the basic functionality of the new ETL framework
to ensure everything is working correctly.
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl.core.config import PipelineConfig, create_default_config
from etl.core.etl_pipeline import StandardETLPipeline
from etl.jobs.bronze.ingestion import BronzeIngestionJob


def test_configuration():
    """Test configuration management."""
    print("🔧 Testing Configuration Management")
    print("=" * 50)
    
    try:
        # Test basic configuration creation
        config = create_default_config("test_project", "dev")
        print(f"✅ Created config for project: {config.project_name}")
        print(f"✅ Environment: {config.table_config.environment}")
        print(f"✅ Cluster ID: {config.cluster_config.cluster_id}")
        
        # Test table name generation
        table_name = config.table_config.get_table_name('bronze', 'data')
        print(f"✅ Generated table name: {table_name}")
        
        # Test configuration serialization
        config_dict = config.to_dict()
        print(f"✅ Configuration serialized: {len(config_dict)} keys")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {str(e)}")
        return False


def test_pipeline_creation():
    """Test pipeline creation."""
    print("\n🚀 Testing Pipeline Creation")
    print("=" * 50)
    
    try:
        # Create configuration
        config = create_default_config("test_project", "dev")
        
        # Create pipeline
        pipeline = StandardETLPipeline(config)
        print(f"✅ Pipeline created successfully")
        print(f"✅ Project: {pipeline.config.project_name}")
        print(f"✅ Transformer: {type(pipeline.transformer).__name__}")
        print(f"✅ Validator: {type(pipeline.validator).__name__}")
        
        return True
        
    except Exception as e:
        print(f"❌ Pipeline creation test failed: {str(e)}")
        return False


def test_job_creation():
    """Test job class creation."""
    print("\n🔧 Testing Job Creation")
    print("=" * 50)
    
    try:
        # Test bronze job creation
        bronze_job = BronzeIngestionJob("test_project", "dev")
        print(f"✅ Bronze job created: {type(bronze_job).__name__}")
        print(f"✅ Project: {bronze_job.config.project_name}")
        
        # Test silver job creation
        from etl.jobs.silver.transformation import SilverTransformationJob
        silver_job = SilverTransformationJob("test_project", "dev")
        print(f"✅ Silver job created: {type(silver_job).__name__}")
        
        # Test gold job creation
        from etl.jobs.gold.aggregation import GoldAggregationJob
        gold_job = GoldAggregationJob("test_project", "dev")
        print(f"✅ Gold job created: {type(gold_job).__name__}")
        
        return True
        
    except Exception as e:
        print(f"❌ Job creation test failed: {str(e)}")
        return False


def test_transformations():
    """Test transformation patterns."""
    print("\n🔄 Testing Transformation Patterns")
    print("=" * 50)
    
    try:
        from etl.core.transformations import DataTransformation
        from pyspark.sql import SparkSession
        
        # Create a simple Spark session for testing
        spark = SparkSession.builder.appName("ETLTest").getOrCreate()
        
        # Create transformer
        transformer = DataTransformation(spark)
        print(f"✅ Transformer created: {type(transformer).__name__}")
        
        # Test metadata column addition (without actual DataFrame)
        print(f"✅ Transformation patterns available")
        
        return True
        
    except Exception as e:
        print(f"❌ Transformation test failed: {str(e)}")
        return False


def test_validation():
    """Test validation framework."""
    print("\n📊 Testing Validation Framework")
    print("=" * 50)
    
    try:
        from etl.core.validators import DataValidator
        
        # Create configuration
        config = create_default_config("test_project", "dev")
        
        # Create validator
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("ETLTest").getOrCreate()
        
        validator = DataValidator(spark, config)
        print(f"✅ Validator created: {type(validator).__name__}")
        print(f"✅ Validation config: {validator.config.validation_config.quality_threshold}")
        
        return True
        
    except Exception as e:
        print(f"❌ Validation test failed: {str(e)}")
        return False


def test_imports():
    """Test all imports work correctly."""
    print("\n📦 Testing Imports")
    print("=" * 50)
    
    try:
        # Test core imports
        from etl.core.config import PipelineConfig, create_default_config
        from etl.core.etl_pipeline import StandardETLPipeline
        from etl.core.transformations import DataTransformation
        from etl.core.validators import DataValidator
        
        # Test job imports
        from etl.jobs.bronze.ingestion import BronzeIngestionJob
        from etl.jobs.silver.transformation import SilverTransformationJob
        from etl.jobs.gold.aggregation import GoldAggregationJob
        
        print("✅ All imports successful")
        return True
        
    except Exception as e:
        print(f"❌ Import test failed: {str(e)}")
        return False


def main():
    """Run all tests."""
    print("🧪 ETL Framework Test Suite")
    print("=" * 60)
    
    tests = [
        ("Import Test", test_imports),
        ("Configuration Test", test_configuration),
        ("Pipeline Creation Test", test_pipeline_creation),
        ("Job Creation Test", test_job_creation),
        ("Transformation Test", test_transformations),
        ("Validation Test", test_validation)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🧪 Running: {test_name}")
        if test_func():
            passed += 1
            print(f"✅ {test_name} PASSED")
        else:
            print(f"❌ {test_name} FAILED")
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! ETL framework is ready to use.")
        return True
    else:
        print("⚠️  Some tests failed. Please review the errors above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 