"""
SQL Templates Module

This module provides parameterized SQL templates for common operations
in Databricks, leveraging built-in functionality where possible.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from pathlib import Path
import json


@dataclass
class SQLTemplate:
    """Represents a SQL template with metadata."""
    name: str
    description: str
    sql_template: str
    parameters: List[str]
    category: str
    tags: List[str]
    examples: List[Dict[str, Any]]


class SQLTemplates:
    """
    Parameterized SQL templates for common operations.
    
    This class provides reusable SQL templates that leverage
    Databricks' built-in functionality and follow best practices.
    """
    
    def __init__(self):
        """Initialize SQL templates."""
        self.templates = self._load_templates()
    
    def _load_templates(self) -> Dict[str, SQLTemplate]:
        """Load all SQL templates."""
        templates = {}
        
        # Data Pipeline Templates
        templates.update(self._get_pipeline_templates())
        
        # Analytics Templates
        templates.update(self._get_analytics_templates())
        
        # Data Quality Templates
        templates.update(self._get_quality_templates())
        
        # Reporting Templates
        templates.update(self._get_reporting_templates())
        
        # Maintenance Templates
        templates.update(self._get_maintenance_templates())
        
        return templates
    
    def _get_pipeline_templates(self) -> Dict[str, SQLTemplate]:
        """Get data pipeline templates."""
        return {
            'bronze_to_silver_pipeline': SQLTemplate(
                name='Bronze to Silver Pipeline',
                description='Complete pipeline from bronze to silver layer',
                sql_template="""
-- Bronze to Silver Pipeline
-- Parameters: {catalog}, {schema}, {table_name}, {quality_conditions}, {transformations}

-- Step 1: Create silver table with transformations
CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name}_silver
USING DELTA
AS SELECT 
    {transformations},
    current_timestamp() as transformation_timestamp,
    'silver' as data_layer
FROM {catalog}.{schema}.{table_name}_bronze
WHERE {quality_conditions};

-- Step 2: Create silver table statistics
ANALYZE TABLE {catalog}.{schema}.{table_name}_silver COMPUTE STATISTICS FOR ALL COLUMNS;

-- Step 3: Log pipeline completion
INSERT INTO {catalog}.{schema}.pipeline_log (
    table_name, 
    pipeline_type, 
    status, 
    records_processed,
    execution_timestamp
)
SELECT 
    '{table_name}_silver' as table_name,
    'bronze_to_silver' as pipeline_type,
    'completed' as status,
    COUNT(*) as records_processed,
    current_timestamp() as execution_timestamp
FROM {catalog}.{schema}.{table_name}_silver;
""",
                parameters=['catalog', 'schema', 'table_name', 'quality_conditions', 'transformations'],
                category='pipeline',
                tags=['bronze', 'silver', 'pipeline', 'transformation'],
                examples=[
                    {
                        'description': 'Transform customer data from bronze to silver',
                        'parameters': {
                            'catalog': 'hive_metastore',
                            'schema': 'retail',
                            'table_name': 'customers',
                            'quality_conditions': 'customer_id IS NOT NULL AND customer_name IS NOT NULL',
                            'transformations': '''
                                customer_id,
                                TRIM(customer_name) as customer_name,
                                LOWER(TRIM(email)) as email,
                                REGEXP_REPLACE(phone, '[^0-9]', '') as phone_clean,
                                INITCAP(TRIM(city)) as city,
                                UPPER(TRIM(state)) as state,
                                UPPER(TRIM(country)) as country
                            '''
                        }
                    }
                ]
            ),
            
            'incremental_merge_pipeline': SQLTemplate(
                name='Incremental Merge Pipeline',
                description='Incremental merge pipeline with deduplication',
                sql_template="""
-- Incremental Merge Pipeline
-- Parameters: {catalog}, {schema}, {table_name}, {source_table}, {partition_key}, {timestamp_column}

-- Step 1: Merge new/updated records
MERGE INTO {catalog}.{schema}.{table_name}_silver AS target
USING (
    SELECT 
        *,
        current_timestamp() as merge_timestamp,
        row_number() over (
            partition by {partition_key} 
            order by {timestamp_column} desc
        ) as rn
    FROM {source_table}
    WHERE {timestamp_column} > (
        SELECT COALESCE(max({timestamp_column}), '1970-01-01')
        FROM {catalog}.{schema}.{table_name}_silver
    )
) AS source
ON target.{partition_key} = source.{partition_key}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHERE source.rn = 1;

-- Step 2: Update table statistics
ANALYZE TABLE {catalog}.{schema}.{table_name}_silver COMPUTE STATISTICS FOR ALL COLUMNS;

-- Step 3: Log merge completion
INSERT INTO {catalog}.{schema}.pipeline_log (
    table_name, 
    pipeline_type, 
    status, 
    records_processed,
    execution_timestamp
)
SELECT 
    '{table_name}_silver' as table_name,
    'incremental_merge' as pipeline_type,
    'completed' as status,
    COUNT(*) as records_processed,
    current_timestamp() as execution_timestamp
FROM {catalog}.{schema}.{table_name}_silver;
""",
                parameters=['catalog', 'schema', 'table_name', 'source_table', 'partition_key', 'timestamp_column'],
                category='pipeline',
                tags=['incremental', 'merge', 'pipeline', 'deduplication'],
                examples=[
                    {
                        'description': 'Incremental merge for customer data',
                        'parameters': {
                            'catalog': 'hive_metastore',
                            'schema': 'retail',
                            'table_name': 'customers',
                            'source_table': 'csv.`/mnt/databricks-datasets/retail-org/customers/`',
                            'partition_key': 'customer_id',
                            'timestamp_column': 'updated_at'
                        }
                    }
                ]
            )
        }
    
    def _get_analytics_templates(self) -> Dict[str, SQLTemplate]:
        """Get analytics templates."""
        return {
            'customer_analytics': SQLTemplate(
                name='Customer Analytics',
                description='Comprehensive customer analytics dashboard',
                sql_template="""
-- Customer Analytics Dashboard
-- Parameters: {catalog}, {schema}, {start_date}, {end_date}

CREATE OR REPLACE TABLE {catalog}.{schema}.customer_analytics_dashboard
USING DELTA
AS SELECT 
    c.customer_id,
    c.customer_name,
    c.country,
    c.state,
    c.city,
    COUNT(o.order_id) as total_orders,
    SUM(o.order_amount) as total_spend,
    AVG(o.order_amount) as avg_order_value,
    MAX(o.order_date) as last_order_date,
    MIN(o.order_date) as first_order_date,
    DATEDIFF(MAX(o.order_date), MIN(o.order_date)) as customer_lifetime_days,
    CASE 
        WHEN SUM(o.order_amount) >= 10000 THEN 'Premium'
        WHEN SUM(o.order_amount) >= 5000 THEN 'Gold'
        WHEN SUM(o.order_amount) >= 1000 THEN 'Silver'
        ELSE 'Bronze'
    END as customer_segment,
    current_timestamp() as analytics_timestamp
FROM {catalog}.{schema}.customers_silver c
LEFT JOIN {catalog}.{schema}.orders_silver o ON c.customer_id = o.customer_id
WHERE o.order_date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY c.customer_id, c.customer_name, c.country, c.state, c.city;
""",
                parameters=['catalog', 'schema', 'start_date', 'end_date'],
                category='analytics',
                tags=['customer', 'analytics', 'dashboard', 'segmentation'],
                examples=[
                    {
                        'description': 'Generate customer analytics for 2024',
                        'parameters': {
                            'catalog': 'hive_metastore',
                            'schema': 'retail',
                            'start_date': '2024-01-01',
                            'end_date': '2024-12-31'
                        }
                    }
                ]
            ),
            
            'sales_analytics': SQLTemplate(
                name='Sales Analytics',
                description='Comprehensive sales analytics and KPIs',
                sql_template="""
-- Sales Analytics Dashboard
-- Parameters: {catalog}, {schema}, {start_date}, {end_date}, {group_by_columns}

CREATE OR REPLACE TABLE {catalog}.{schema}.sales_analytics_dashboard
USING DELTA
AS SELECT 
    {group_by_columns},
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(o.order_amount) as total_revenue,
    AVG(o.order_amount) as avg_order_value,
    SUM(o.order_amount) / COUNT(DISTINCT o.customer_id) as revenue_per_customer,
    COUNT(o.order_id) / COUNT(DISTINCT o.customer_id) as orders_per_customer,
    DATE_TRUNC('month', o.order_date) as month,
    current_timestamp() as analytics_timestamp
FROM {catalog}.{schema}.orders_silver o
WHERE o.order_date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY {group_by_columns}, DATE_TRUNC('month', o.order_date)
ORDER BY month, {group_by_columns};
""",
                parameters=['catalog', 'schema', 'start_date', 'end_date', 'group_by_columns'],
                category='analytics',
                tags=['sales', 'analytics', 'kpis', 'revenue'],
                examples=[
                    {
                        'description': 'Generate sales analytics by product category',
                        'parameters': {
                            'catalog': 'hive_metastore',
                            'schema': 'retail',
                            'start_date': '2024-01-01',
                            'end_date': '2024-12-31',
                            'group_by_columns': 'p.product_category, p.product_name'
                        }
                    }
                ]
            )
        }
    
    def _get_quality_templates(self) -> Dict[str, SQLTemplate]:
        """Get data quality templates."""
        return {
            'comprehensive_quality_check': SQLTemplate(
                name='Comprehensive Quality Check',
                description='Complete data quality assessment for a table',
                sql_template="""
-- Comprehensive Data Quality Check
-- Parameters: {catalog}, {schema}, {table_name}, {key_columns}, {numeric_columns}, {text_columns}

CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name}_quality_report
USING DELTA
AS SELECT 
    'completeness' as quality_dimension,
    column_name,
    total_rows,
    non_null_count,
    null_count,
    ROUND(completeness_percentage, 2) as quality_score
FROM (
    SELECT 
        'total' as column_name,
        COUNT(*) as total_rows,
        COUNT(*) as non_null_count,
        0 as null_count,
        100.0 as completeness_percentage
    FROM {catalog}.{schema}.{table_name}
    
    UNION ALL
    
    SELECT 
        '{key_columns}' as column_name,
        COUNT(*) as total_rows,
        COUNT({key_columns}) as non_null_count,
        COUNT(*) - COUNT({key_columns}) as null_count,
        ROUND(COUNT({key_columns}) * 100.0 / COUNT(*), 2) as completeness_percentage
    FROM {catalog}.{schema}.{table_name}
    
    UNION ALL
    
    SELECT 
        '{numeric_columns}' as column_name,
        COUNT(*) as total_rows,
        COUNT({numeric_columns}) as non_null_count,
        COUNT(*) - COUNT({numeric_columns}) as null_count,
        ROUND(COUNT({numeric_columns}) * 100.0 / COUNT(*), 2) as completeness_percentage
    FROM {catalog}.{schema}.{table_name}
    
    UNION ALL
    
    SELECT 
        '{text_columns}' as column_name,
        COUNT(*) as total_rows,
        COUNT({text_columns}) as non_null_count,
        COUNT(*) - COUNT({text_columns}) as null_count,
        ROUND(COUNT({text_columns}) * 100.0 / COUNT(*), 2) as completeness_percentage
    FROM {catalog}.{schema}.{table_name}
);

-- Add uniqueness check
INSERT INTO {catalog}.{schema}.{table_name}_quality_report
SELECT 
    'uniqueness' as quality_dimension,
    '{key_columns}' as column_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT {key_columns}) as non_null_count,
    COUNT(*) - COUNT(DISTINCT {key_columns}) as null_count,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT {key_columns}) THEN 100.0
        ELSE ROUND(COUNT(DISTINCT {key_columns}) * 100.0 / COUNT(*), 2)
    END as quality_score
FROM {catalog}.{schema}.{table_name};
""",
                parameters=['catalog', 'schema', 'table_name', 'key_columns', 'numeric_columns', 'text_columns'],
                category='quality',
                tags=['quality_check', 'completeness', 'uniqueness', 'assessment'],
                examples=[
                    {
                        'description': 'Comprehensive quality check for customers table',
                        'parameters': {
                            'catalog': 'hive_metastore',
                            'schema': 'retail',
                            'table_name': 'customers_silver',
                            'key_columns': 'customer_id',
                            'numeric_columns': 'customer_id',
                            'text_columns': 'customer_name, email'
                        }
                    }
                ]
            )
        }
    
    def _get_reporting_templates(self) -> Dict[str, SQLTemplate]:
        """Get reporting templates."""
        return {
            'executive_summary': SQLTemplate(
                name='Executive Summary Report',
                description='High-level executive summary with key metrics',
                sql_template="""
-- Executive Summary Report
-- Parameters: {catalog}, {schema}, {report_date}

CREATE OR REPLACE TABLE {catalog}.{schema}.executive_summary_{report_date}
USING DELTA
AS SELECT 
    'Executive Summary' as report_type,
    '{report_date}' as report_date,
    COUNT(DISTINCT customer_id) as total_customers,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(order_amount) as total_revenue,
    AVG(order_amount) as avg_order_value,
    COUNT(DISTINCT order_id) / COUNT(DISTINCT customer_id) as orders_per_customer,
    SUM(order_amount) / COUNT(DISTINCT customer_id) as revenue_per_customer,
    COUNT(DISTINCT CASE WHEN order_date >= DATE_SUB('{report_date}', 30) THEN customer_id END) as active_customers_30d,
    COUNT(DISTINCT CASE WHEN order_date >= DATE_SUB('{report_date}', 90) THEN customer_id END) as active_customers_90d,
    current_timestamp() as report_generated_at
FROM {catalog}.{schema}.orders_silver
WHERE order_date <= '{report_date}';
""",
                parameters=['catalog', 'schema', 'report_date'],
                category='reporting',
                tags=['executive', 'summary', 'kpis', 'report'],
                examples=[
                    {
                        'description': 'Generate executive summary for 2024-12-31',
                        'parameters': {
                            'catalog': 'hive_metastore',
                            'schema': 'retail',
                            'report_date': '2024-12-31'
                        }
                    }
                ]
            )
        }
    
    def _get_maintenance_templates(self) -> Dict[str, SQLTemplate]:
        """Get maintenance templates."""
        return {
            'table_optimization': SQLTemplate(
                name='Table Optimization',
                description='Optimize table performance and storage',
                sql_template="""
-- Table Optimization
-- Parameters: {catalog}, {schema}, {table_name}

-- Step 1: Optimize table
OPTIMIZE {catalog}.{schema}.{table_name};

-- Step 2: Z-order optimization for common query columns
OPTIMIZE {catalog}.{schema}.{table_name} ZORDER BY ({zorder_columns});

-- Step 3: Update table statistics
ANALYZE TABLE {catalog}.{schema}.{table_name} COMPUTE STATISTICS FOR ALL COLUMNS;

-- Step 4: Log optimization
INSERT INTO {catalog}.{schema}.maintenance_log (
    table_name,
    operation_type,
    execution_timestamp,
    status
)
VALUES (
    '{table_name}',
    'optimization',
    current_timestamp(),
    'completed'
);
""",
                parameters=['catalog', 'schema', 'table_name', 'zorder_columns'],
                category='maintenance',
                tags=['optimization', 'performance', 'maintenance'],
                examples=[
                    {
                        'description': 'Optimize customers table',
                        'parameters': {
                            'catalog': 'hive_metastore',
                            'schema': 'retail',
                            'table_name': 'customers_silver',
                            'zorder_columns': 'customer_id, country, state'
                        }
                    }
                ]
            ),
            
            'data_retention': SQLTemplate(
                name='Data Retention Policy',
                description='Implement data retention policy',
                sql_template="""
-- Data Retention Policy
-- Parameters: {catalog}, {schema}, {table_name}, {retention_days}, {date_column}

-- Step 1: Archive old data
CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name}_archive
USING DELTA
AS SELECT * FROM {catalog}.{schema}.{table_name}
WHERE {date_column} < DATE_SUB(current_date(), {retention_days});

-- Step 2: Delete old data from main table
DELETE FROM {catalog}.{schema}.{table_name}
WHERE {date_column} < DATE_SUB(current_date(), {retention_days});

-- Step 3: Log retention operation
INSERT INTO {catalog}.{schema}.maintenance_log (
    table_name,
    operation_type,
    execution_timestamp,
    status,
    details
)
VALUES (
    '{table_name}',
    'data_retention',
    current_timestamp(),
    'completed',
    'Retained {retention_days} days of data'
);
""",
                parameters=['catalog', 'schema', 'table_name', 'retention_days', 'date_column'],
                category='maintenance',
                tags=['retention', 'archive', 'cleanup', 'maintenance'],
                examples=[
                    {
                        'description': 'Implement 2-year retention for orders',
                        'parameters': {
                            'catalog': 'hive_metastore',
                            'schema': 'retail',
                            'table_name': 'orders_silver',
                            'retention_days': 730,
                            'date_column': 'order_date'
                        }
                    }
                ]
            )
        }
    
    def get_template(self, template_name: str) -> Optional[SQLTemplate]:
        """Get a specific SQL template by name."""
        return self.templates.get(template_name)
    
    def list_templates(self, category: Optional[str] = None) -> List[SQLTemplate]:
        """List all templates, optionally filtered by category."""
        if category:
            return [t for t in self.templates.values() if t.category == category]
        return list(self.templates.values())
    
    def render_template(self, template_name: str, parameters: Dict[str, Any]) -> str:
        """Render a SQL template with the given parameters."""
        template = self.get_template(template_name)
        if not template:
            raise ValueError(f"Template '{template_name}' not found")
        
        # Validate required parameters
        missing_params = [p for p in template.parameters if p not in parameters]
        if missing_params:
            raise ValueError(f"Missing required parameters: {missing_params}")
        
        # Render the SQL template
        return template.sql_template.format(**parameters)
    
    def get_template_examples(self, template_name: str) -> List[Dict[str, Any]]:
        """Get examples for a specific template."""
        template = self.get_template(template_name)
        return template.examples if template else []
    
    def search_templates(self, query: str) -> List[SQLTemplate]:
        """Search templates by name, description, or tags."""
        query_lower = query.lower()
        results = []
        
        for template in self.templates.values():
            if (query_lower in template.name.lower() or
                query_lower in template.description.lower() or
                any(query_lower in tag.lower() for tag in template.tags)):
                results.append(template)
        
        return results
    
    def create_template_library(self, output_file: Optional[str] = None) -> str:
        """Create a complete SQL template library file."""
        library_sql = "-- SQL Template Library\n"
        library_sql += "-- Generated by Databricks Toolkit\n\n"
        
        for template in self.templates.values():
            library_sql += f"-- {template.name}: {template.description}\n"
            library_sql += f"-- Parameters: {', '.join(template.parameters)}\n"
            library_sql += f"-- Category: {template.category}\n"
            library_sql += f"-- Tags: {', '.join(template.tags)}\n\n"
            library_sql += template.sql_template + "\n\n"
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(library_sql)
        
        return library_sql 