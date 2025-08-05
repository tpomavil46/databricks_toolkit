"""
Standardized Transformation Patterns for ETL Operations

This module provides common transformation patterns that can be reused
across different ETL pipelines, ensuring consistency and maintainability.
"""

from typing import Dict, List, Any, Optional, Callable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, upper, lower, trim, regexp_replace
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType, DateType
from utils.logger import log_function_call


class DataTransformation:
    """
    Standardized data transformation patterns for ETL operations.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the data transformation framework.
        
        Args:
            spark: Spark session
        """
        self.spark = spark
    
    @log_function_call
    def add_metadata_columns(self, df: DataFrame, source_path: str, layer: str) -> DataFrame:
        """
        Add standard metadata columns to DataFrame.
        
        Args:
            df: Input DataFrame
            source_path: Source data path
            layer: Data layer (bronze, silver, gold)
            
        Returns:
            DataFrame with metadata columns
        """
        return df.withColumn("_ingestion_timestamp", current_timestamp()) \
                .withColumn("_source_path", lit(source_path)) \
                .withColumn("_data_layer", lit(layer)) \
                .withColumn("_processing_date", current_timestamp().cast("date"))
    
    @log_function_call
    def clean_string_columns(self, df: DataFrame, columns: List[str] = None) -> DataFrame:
        """
        Clean string columns by trimming whitespace and standardizing case.
        
        Args:
            df: Input DataFrame
            columns: List of columns to clean (if None, clean all string columns)
            
        Returns:
            DataFrame with cleaned string columns
        """
        if columns is None:
            # Get all string columns
            string_columns = [field.name for field in df.schema.fields 
                           if isinstance(field.dataType, StringType)]
        else:
            string_columns = columns
        
        result_df = df
        for column in string_columns:
            if column in df.columns:
                result_df = result_df.withColumn(
                    column,
                    trim(upper(col(column)))
                )
        
        return result_df
    
    @log_function_call
    def handle_null_values(self, df: DataFrame, null_strategy: Dict[str, str] = None) -> DataFrame:
        """
        Handle null values based on specified strategy.
        
        Args:
            df: Input DataFrame
            null_strategy: Dictionary mapping column names to strategies
                          ('drop', 'fill_default', 'fill_mean', 'fill_median')
            
        Returns:
            DataFrame with null values handled
        """
        if null_strategy is None:
            # Default strategy: fill string columns with 'UNKNOWN', numeric with 0
            null_strategy = {}
            for field in df.schema.fields:
                if isinstance(field.dataType, StringType):
                    null_strategy[field.name] = 'fill_default'
                elif isinstance(field.dataType, (IntegerType, DoubleType)):
                    null_strategy[field.name] = 'fill_default'
        
        result_df = df
        for column, strategy in null_strategy.items():
            if column in df.columns:
                if strategy == 'drop':
                    result_df = result_df.filter(col(column).isNotNull())
                elif strategy == 'fill_default':
                    if isinstance(df.schema[column].dataType, StringType):
                        result_df = result_df.withColumn(column, when(col(column).isNull(), lit('UNKNOWN')).otherwise(col(column)))
                    else:
                        result_df = result_df.withColumn(column, when(col(column).isNull(), lit(0)).otherwise(col(column)))
                elif strategy == 'fill_mean':
                    # Calculate mean and fill nulls
                    mean_value = df.select(col(column)).filter(col(column).isNotNull()).agg({column: 'avg'}).collect()[0][0]
                    result_df = result_df.withColumn(column, when(col(column).isNull(), lit(mean_value)).otherwise(col(column)))
                elif strategy == 'fill_median':
                    # Calculate median and fill nulls
                    median_value = df.select(col(column)).filter(col(column).isNotNull()).agg({column: 'percentile'}).collect()[0][0]
                    result_df = result_df.withColumn(column, when(col(column).isNull(), lit(median_value)).otherwise(col(column)))
        
        return result_df
    
    @log_function_call
    def convert_data_types(self, df: DataFrame, type_mapping: Dict[str, str] = None) -> DataFrame:
        """
        Convert data types based on mapping.
        
        Args:
            df: Input DataFrame
            type_mapping: Dictionary mapping column names to target types
            
        Returns:
            DataFrame with converted data types
        """
        if type_mapping is None:
            return df
        
        result_df = df
        for column, target_type in type_mapping.items():
            if column in df.columns:
                if target_type == 'string':
                    result_df = result_df.withColumn(column, col(column).cast(StringType()))
                elif target_type == 'integer':
                    result_df = result_df.withColumn(column, col(column).cast(IntegerType()))
                elif target_type == 'double':
                    result_df = result_df.withColumn(column, col(column).cast(DoubleType()))
                elif target_type == 'timestamp':
                    result_df = result_df.withColumn(column, col(column).cast(TimestampType()))
                elif target_type == 'date':
                    result_df = result_df.withColumn(column, col(column).cast(DateType()))
        
        return result_df
    
    @log_function_call
    def remove_duplicates(self, df: DataFrame, key_columns: List[str] = None, keep: str = 'first') -> DataFrame:
        """
        Remove duplicate rows based on key columns.
        
        Args:
            df: Input DataFrame
            key_columns: Columns to use for duplicate detection (if None, use all columns)
            keep: Which duplicate to keep ('first' or 'last')
            
        Returns:
            DataFrame with duplicates removed
        """
        if key_columns is None:
            key_columns = df.columns
        
        if keep == 'first':
            return df.dropDuplicates(key_columns)
        else:
            # For 'last', we need to reverse the order
            return df.orderBy(*[col(c).desc() for c in key_columns]).dropDuplicates(key_columns)
    
    @log_function_call
    def filter_data(self, df: DataFrame, filter_conditions: Dict[str, Any] = None) -> DataFrame:
        """
        Apply filter conditions to DataFrame.
        
        Args:
            df: Input DataFrame
            filter_conditions: Dictionary of column conditions
                             {'column': {'operator': 'value'}}
                             operators: 'equals', 'not_equals', 'greater_than', 'less_than', 'contains', 'not_contains'
            
        Returns:
            Filtered DataFrame
        """
        if filter_conditions is None:
            return df
        
        result_df = df
        for column, condition in filter_conditions.items():
            if column in df.columns:
                for operator, value in condition.items():
                    if operator == 'equals':
                        result_df = result_df.filter(col(column) == value)
                    elif operator == 'not_equals':
                        result_df = result_df.filter(col(column) != value)
                    elif operator == 'greater_than':
                        result_df = result_df.filter(col(column) > value)
                    elif operator == 'less_than':
                        result_df = result_df.filter(col(column) < value)
                    elif operator == 'contains':
                        result_df = result_df.filter(col(column).contains(value))
                    elif operator == 'not_contains':
                        result_df = result_df.filter(~col(column).contains(value))
        
        return result_df
    
    @log_function_call
    def rename_columns(self, df: DataFrame, column_mapping: Dict[str, str]) -> DataFrame:
        """
        Rename columns based on mapping.
        
        Args:
            df: Input DataFrame
            column_mapping: Dictionary mapping old column names to new names
            
        Returns:
            DataFrame with renamed columns
        """
        result_df = df
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                result_df = result_df.withColumnRenamed(old_name, new_name)
        
        return result_df
    
    @log_function_call
    def select_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Select specific columns from DataFrame.
        
        Args:
            df: Input DataFrame
            columns: List of column names to select
            
        Returns:
            DataFrame with selected columns
        """
        available_columns = [col for col in columns if col in df.columns]
        return df.select(*available_columns)
    
    @log_function_call
    def apply_custom_transformation(self, df: DataFrame, transformation_func: Callable) -> DataFrame:
        """
        Apply a custom transformation function.
        
        Args:
            df: Input DataFrame
            transformation_func: Custom transformation function
            
        Returns:
            Transformed DataFrame
        """
        return transformation_func(df)
    
    @log_function_call
    def create_bronze_table(self, df: DataFrame, table_name: str, source_path: str) -> DataFrame:
        """
        Create a standardized bronze table with metadata.
        
        Args:
            df: Input DataFrame
            table_name: Name for the bronze table
            source_path: Source data path
            
        Returns:
            Bronze DataFrame with metadata
        """
        # Add metadata columns
        bronze_df = self.add_metadata_columns(df, source_path, "bronze")
        
        # Basic cleaning
        bronze_df = self.clean_string_columns(bronze_df)
        
        return bronze_df
    
    @log_function_call
    def create_silver_table(self, df: DataFrame, table_name: str, transformations: Dict[str, Any] = None) -> DataFrame:
        """
        Create a standardized silver table with business logic transformations.
        
        Args:
            df: Input DataFrame
            table_name: Name for the silver table
            transformations: Dictionary of transformations to apply
            
        Returns:
            Silver DataFrame with business logic applied
        """
        silver_df = df
        
        if transformations:
            # Apply null handling
            if 'null_strategy' in transformations:
                silver_df = self.handle_null_values(silver_df, transformations['null_strategy'])
            
            # Apply data type conversions
            if 'type_mapping' in transformations:
                silver_df = self.convert_data_types(silver_df, transformations['type_mapping'])
            
            # Apply filters
            if 'filter_conditions' in transformations:
                silver_df = self.filter_data(silver_df, transformations['filter_conditions'])
            
            # Remove duplicates
            if 'deduplication_keys' in transformations:
                silver_df = self.remove_duplicates(silver_df, transformations['deduplication_keys'])
            
            # Rename columns
            if 'column_mapping' in transformations:
                silver_df = self.rename_columns(silver_df, transformations['column_mapping'])
            
            # Select specific columns
            if 'select_columns' in transformations:
                silver_df = self.select_columns(silver_df, transformations['select_columns'])
        
        # Add metadata
        silver_df = self.add_metadata_columns(silver_df, "silver_transformation", "silver")
        
        return silver_df
    
    @log_function_call
    def create_gold_table(self, df: DataFrame, table_name: str, aggregations: Dict[str, Any] = None) -> DataFrame:
        """
        Create a standardized gold table with aggregations and KPIs.
        
        Args:
            df: Input DataFrame
            table_name: Name for the gold table
            aggregations: Dictionary of aggregation configurations
            
        Returns:
            Gold DataFrame with aggregations and KPIs
        """
        gold_df = df
        
        if aggregations:
            # Apply grouping and aggregations
            if 'group_by' in aggregations and 'aggregations' in aggregations:
                group_cols = aggregations['group_by']
                agg_cols = aggregations['aggregations']
                
                # Build aggregation expressions
                agg_exprs = []
                for col_name, agg_func in agg_cols.items():
                    if col_name in df.columns:
                        agg_exprs.append(agg_func(col(col_name)).alias(f"{col_name}_{agg_func.__name__}"))
                
                if agg_exprs:
                    gold_df = df.groupBy(*group_cols).agg(*agg_exprs)
        
        # Add metadata
        gold_df = self.add_metadata_columns(gold_df, "gold_aggregation", "gold")
        
        return gold_df 