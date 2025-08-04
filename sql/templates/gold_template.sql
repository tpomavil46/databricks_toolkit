-- Gold Layer Template
-- This template can be used for any gold layer KPI generation
-- Parameters: ${silver_table_name}, ${gold_table_name}, ${vendor_filter_condition}

-- Generate KPIs and business metrics from silver data
CREATE OR REPLACE TABLE ${gold_table_name}
AS SELECT 
    -- Add your domain-specific grouping columns here
    -- Example: customer_id, product_id, region, etc.
    
    -- Add count metrics
    COUNT(*) as total_records,
    
    -- Add aggregation metrics
    -- Example: AVG(amount) as avg_amount, SUM(quantity) as total_quantity,
    
    -- Add business KPIs
    -- Example: COUNT(CASE WHEN category = 'high' THEN 1 END) as high_value_count,
    
    -- Add data quality KPIs
    COUNT(CASE WHEN data_quality_flag = 'valid' THEN 1 END) as valid_records,
    COUNT(CASE WHEN data_quality_flag = 'invalid' THEN 1 END) as invalid_records,
    
    -- Add time-based KPIs
    DATE(transformation_timestamp) as record_date,
    
    -- Audit fields
    current_timestamp() as kpi_generation_timestamp,
    'gold_layer' as data_layer
    
FROM ${silver_table_name}
${vendor_filter_condition}
    
GROUP BY 
    -- Add your grouping columns here
    -- Example: customer_id, product_id, region,
    DATE(transformation_timestamp) 