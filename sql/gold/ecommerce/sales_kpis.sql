-- Ecommerce Gold Layer: Sales KPI Generation
-- This SQL file demonstrates parameterized KPI generation for ecommerce sales
-- Parameters: ${silver_table_name}, ${gold_table_name}, ${vendor_filter_condition}

-- Generate KPIs and business metrics from silver data
CREATE OR REPLACE TABLE ${gold_table_name}
AS SELECT 
    -- Customer KPIs
    customer_id,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_spend,
    AVG(total_amount) as avg_order_value,
    MAX(total_amount) as max_order_value,
    MIN(total_amount) as min_order_value,
    
    -- Product KPIs
    product_id,
    SUM(quantity) as total_quantity_sold,
    AVG(unit_price) as avg_unit_price,
    
    -- Business KPIs
    COUNT(CASE WHEN order_category = 'high_value' THEN 1 END) as high_value_orders,
    COUNT(CASE WHEN order_category = 'medium_value' THEN 1 END) as medium_value_orders,
    COUNT(CASE WHEN order_category = 'low_value' THEN 1 END) as low_value_orders,
    
    -- Data Quality KPIs
    COUNT(CASE WHEN data_quality_flag = 'valid' THEN 1 END) as valid_records,
    COUNT(CASE WHEN data_quality_flag = 'invalid' THEN 1 END) as invalid_records,
    
    -- Time-based KPIs
    DATE(transformation_timestamp) as order_date,
    
    -- Audit fields
    current_timestamp() as kpi_generation_timestamp,
    'gold_layer' as data_layer
    
FROM ${silver_table_name}
${vendor_filter_condition}
    
GROUP BY 
    customer_id,
    product_id,
    DATE(transformation_timestamp) 