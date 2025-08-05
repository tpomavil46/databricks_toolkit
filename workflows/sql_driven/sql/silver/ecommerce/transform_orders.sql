-- Ecommerce Silver Layer: Order Data Transformation
-- This SQL file demonstrates parameterized data transformation for ecommerce orders
-- Parameters: ${bronze_table_name}, ${silver_table_name}, ${vendor_filter_condition}

-- Transform bronze data into silver layer with cleaning and standardization
CREATE OR REPLACE TABLE ${silver_table_name}
AS SELECT 
    -- Clean and standardize column names
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    order_date,
    status,
    
    -- Add calculated fields
    quantity * unit_price as calculated_total,
    
    -- Add data quality flags
    CASE 
        WHEN order_id IS NOT NULL AND customer_id IS NOT NULL THEN 'valid'
        ELSE 'invalid'
    END as data_quality_flag,
    
    -- Add business logic
    CASE 
        WHEN total_amount > 1000 THEN 'high_value'
        WHEN total_amount > 100 THEN 'medium_value'
        ELSE 'low_value'
    END as order_category,
    
    -- Add audit fields
    current_timestamp() as transformation_timestamp,
    'silver_layer' as data_layer
    
FROM ${bronze_table_name}
${vendor_filter_condition} 