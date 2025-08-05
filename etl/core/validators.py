"""
Data Validation Framework for ETL Operations

This module provides standardized data validation functions for ETL operations,
ensuring data quality and consistency across all pipeline stages.
"""

from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, isnan, when, isnull, desc, asc
from pyspark.sql.types import StructType, StructField
import pandas as pd
from utils.logger import log_function_call


class DataValidator:
    """
    Standardized data validation framework for ETL operations.
    """
    
    def __init__(self, spark: SparkSession, config: 'PipelineConfig'):
        """
        Initialize the data validator.
        
        Args:
            spark: Spark session
            config: Pipeline configuration
        """
        self.spark = spark
        self.config = config
        self.validation_results = {}
    
    @log_function_call
    def validate_bronze_data(self, df: DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Validate bronze layer data quality.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table being validated
            
        Returns:
            Dictionary containing validation results
        """
        print(f"🔍 Validating Bronze Data: {table_name}")
        
        validation_results = {
            'table_name': table_name,
            'layer': 'bronze',
            'total_rows': df.count(),
            'total_columns': len(df.columns),
            'schema': self._get_schema_info(df),
            'null_analysis': self._analyze_nulls(df),
            'data_type_analysis': self._analyze_data_types(df),
            'quality_score': 0.0,
            'issues': [],
            'recommendations': []
        }
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(validation_results)
        validation_results['quality_score'] = quality_score
        
        # Generate recommendations
        validation_results['recommendations'] = self._generate_recommendations(validation_results)
        
        self.validation_results['bronze'] = validation_results
        return validation_results
    
    @log_function_call
    def validate_silver_data(self, df: DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Validate silver layer data quality.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table being validated
            
        Returns:
            Dictionary containing validation results
        """
        print(f"🔍 Validating Silver Data: {table_name}")
        
        validation_results = {
            'table_name': table_name,
            'layer': 'silver',
            'total_rows': df.count(),
            'total_columns': len(df.columns),
            'schema': self._get_schema_info(df),
            'null_analysis': self._analyze_nulls(df),
            'data_type_analysis': self._analyze_data_types(df),
            'business_rules': self._validate_business_rules(df),
            'quality_score': 0.0,
            'issues': [],
            'recommendations': []
        }
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(validation_results)
        validation_results['quality_score'] = quality_score
        
        # Generate recommendations
        validation_results['recommendations'] = self._generate_recommendations(validation_results)
        
        self.validation_results['silver'] = validation_results
        return validation_results
    
    @log_function_call
    def validate_gold_data(self, df: DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Validate gold layer data quality.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table being validated
            
        Returns:
            Dictionary containing validation results
        """
        print(f"🔍 Validating Gold Data: {table_name}")
        
        validation_results = {
            'table_name': table_name,
            'layer': 'gold',
            'total_rows': df.count(),
            'total_columns': len(df.columns),
            'schema': self._get_schema_info(df),
            'null_analysis': self._analyze_nulls(df),
            'data_type_analysis': self._analyze_data_types(df),
            'kpi_validation': self._validate_kpis(df),
            'quality_score': 0.0,
            'issues': [],
            'recommendations': []
        }
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(validation_results)
        validation_results['quality_score'] = quality_score
        
        # Generate recommendations
        validation_results['recommendations'] = self._generate_recommendations(validation_results)
        
        self.validation_results['gold'] = validation_results
        return validation_results
    
    def _get_schema_info(self, df: DataFrame) -> Dict[str, Any]:
        """Get detailed schema information."""
        schema_info = {
            'columns': [],
            'data_types': {},
            'nullable_columns': []
        }
        
        for field in df.schema.fields:
            schema_info['columns'].append(field.name)
            schema_info['data_types'][field.name] = str(field.dataType)
            if field.nullable:
                schema_info['nullable_columns'].append(field.name)
        
        return schema_info
    
    def _analyze_nulls(self, df: DataFrame) -> Dict[str, Any]:
        """Analyze null values in the DataFrame."""
        null_analysis = {}
        total_rows = df.count()
        
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
            
            null_analysis[column] = {
                'null_count': null_count,
                'null_percentage': null_percentage,
                'has_nulls': null_count > 0,
                'exceeds_threshold': null_percentage > self.config.validation_config.null_threshold * 100
            }
        
        return null_analysis
    
    def _analyze_data_types(self, df: DataFrame) -> Dict[str, Any]:
        """Analyze data types and potential issues."""
        data_type_analysis = {}
        
        for field in df.schema.fields:
            column_name = field.name
            data_type = str(field.dataType)
            
            # Check for potential data type issues
            issues = []
            if 'StringType' in data_type:
                # Check for empty strings
                empty_count = df.filter(col(column_name) == "").count()
                if empty_count > 0:
                    issues.append(f"Contains {empty_count} empty strings")
            
            data_type_analysis[column_name] = {
                'data_type': data_type,
                'issues': issues,
                'has_issues': len(issues) > 0
            }
        
        return data_type_analysis
    
    def _validate_business_rules(self, df: DataFrame) -> Dict[str, Any]:
        """Validate business rules for silver layer."""
        business_rules = {
            'primary_key_check': self._check_primary_key(df),
            'foreign_key_check': self._check_foreign_keys(df),
            'data_range_check': self._check_data_ranges(df),
            'format_check': self._check_data_formats(df)
        }
        
        return business_rules
    
    def _validate_kpis(self, df: DataFrame) -> Dict[str, Any]:
        """Validate KPIs for gold layer."""
        kpi_validation = {
            'metric_consistency': self._check_metric_consistency(df),
            'aggregation_accuracy': self._check_aggregation_accuracy(df),
            'trend_analysis': self._check_trend_analysis(df)
        }
        
        return kpi_validation
    
    def _check_primary_key(self, df: DataFrame) -> Dict[str, Any]:
        """Check for primary key uniqueness."""
        # This is a placeholder - implement based on your business rules
        return {'status': 'not_implemented', 'message': 'Primary key check not configured'}
    
    def _check_foreign_keys(self, df: DataFrame) -> Dict[str, Any]:
        """Check foreign key relationships."""
        # This is a placeholder - implement based on your business rules
        return {'status': 'not_implemented', 'message': 'Foreign key check not configured'}
    
    def _check_data_ranges(self, df: DataFrame) -> Dict[str, Any]:
        """Check data ranges for numeric columns."""
        # This is a placeholder - implement based on your business rules
        return {'status': 'not_implemented', 'message': 'Data range check not configured'}
    
    def _check_data_formats(self, df: DataFrame) -> Dict[str, Any]:
        """Check data format consistency."""
        # This is a placeholder - implement based on your business rules
        return {'status': 'not_implemented', 'message': 'Data format check not configured'}
    
    def _check_metric_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Check metric consistency in gold layer."""
        # This is a placeholder - implement based on your business rules
        return {'status': 'not_implemented', 'message': 'Metric consistency check not configured'}
    
    def _check_aggregation_accuracy(self, df: DataFrame) -> Dict[str, Any]:
        """Check aggregation accuracy."""
        # This is a placeholder - implement based on your business rules
        return {'status': 'not_implemented', 'message': 'Aggregation accuracy check not configured'}
    
    def _check_trend_analysis(self, df: DataFrame) -> Dict[str, Any]:
        """Check for data trends and anomalies."""
        # This is a placeholder - implement based on your business rules
        return {'status': 'not_implemented', 'message': 'Trend analysis not configured'}
    
    def _calculate_quality_score(self, validation_results: Dict[str, Any]) -> float:
        """Calculate overall data quality score."""
        score = 100.0
        
        # Deduct points for null issues
        null_analysis = validation_results.get('null_analysis', {})
        for column, null_info in null_analysis.items():
            if null_info.get('exceeds_threshold', False):
                score -= 10.0
        
        # Deduct points for data type issues
        data_type_analysis = validation_results.get('data_type_analysis', {})
        for column, type_info in data_type_analysis.items():
            if type_info.get('has_issues', False):
                score -= 5.0
        
        # Ensure score doesn't go below 0
        return max(0.0, score)
    
    def _generate_recommendations(self, validation_results: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        # Check null threshold issues
        null_analysis = validation_results.get('null_analysis', {})
        for column, null_info in null_analysis.items():
            if null_info.get('exceeds_threshold', False):
                recommendations.append(f"Column '{column}' has {null_info['null_percentage']:.1f}% nulls - consider data cleaning")
        
        # Check data type issues
        data_type_analysis = validation_results.get('data_type_analysis', {})
        for column, type_info in data_type_analysis.items():
            if type_info.get('has_issues', False):
                recommendations.append(f"Column '{column}' has data type issues - review data quality")
        
        # Check quality score
        quality_score = validation_results.get('quality_score', 0.0)
        if quality_score < self.config.validation_config.quality_threshold * 100:
            recommendations.append(f"Overall quality score ({quality_score:.1f}) below threshold - review data quality")
        
        return recommendations
    
    def print_validation_report(self, layer: str) -> None:
        """Print a formatted validation report."""
        if layer not in self.validation_results:
            print(f"❌ No validation results found for layer: {layer}")
            return
        
        results = self.validation_results[layer]
        
        print(f"\n📊 Validation Report for {results['table_name']}")
        print("=" * 60)
        print(f"Layer: {results['layer']}")
        print(f"Total Rows: {results['total_rows']:,}")
        print(f"Total Columns: {results['total_columns']}")
        print(f"Quality Score: {results['quality_score']:.1f}/100")
        
        if results['issues']:
            print(f"\n⚠️ Issues Found:")
            for issue in results['issues']:
                print(f"  - {issue}")
        
        if results['recommendations']:
            print(f"\n💡 Recommendations:")
            for rec in results['recommendations']:
                print(f"  - {rec}")
        
        print("=" * 60)
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get a summary of all validation results."""
        summary = {
            'total_layers_validated': len(self.validation_results),
            'layers': list(self.validation_results.keys()),
            'overall_quality_score': 0.0,
            'critical_issues': [],
            'warnings': []
        }
        
        if self.validation_results:
            total_score = sum(results.get('quality_score', 0) for results in self.validation_results.values())
            summary['overall_quality_score'] = total_score / len(self.validation_results)
        
        return summary 