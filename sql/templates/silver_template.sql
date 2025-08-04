-- Silver Layer Template
-- This template can be used for any silver layer transformation
-- Parameters: ${bronze_table_name}, ${silver_table_name}, ${vendor_filter_condition}

-- Transform bronze data into silver layer with cleaning and standardization
CREATE OR REPLACE TABLE ${silver_table_name}
AS SELECT 
    -- Add your domain-specific columns here
    -- Example: customer_id, order_id, product_id, etc.
    
    -- Add calculated fields
    -- Example: CASE WHEN condition THEN 'value' ELSE 'other' END as calculated_field,
    
    -- Add data quality flags
    CASE 
        WHEN primary_key IS NOT NULL THEN 'valid'
        ELSE 'invalid'
    END as data_quality_flag,
    
    -- Add business logic
    -- Example: CASE WHEN amount > threshold THEN 'high' ELSE 'low' END as category,
    
    -- Add audit fields
    current_timestamp() as transformation_timestamp,
    'silver_layer' as data_layer
    
FROM ${bronze_table_name}
${vendor_filter_condition} 