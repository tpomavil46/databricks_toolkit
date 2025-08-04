-- Silver Layer: Data Transformation
-- This SQL file demonstrates parameterized data transformation for the silver layer
-- Parameters: ${bronze_table_name}, ${silver_table_name}, ${vendor_filter}

-- Transform bronze data into silver layer with cleaning and standardization
CREATE OR REPLACE TABLE ${silver_table_name}
AS SELECT 
    -- Clean and standardize column names
    customer_id,
    customer_name,
    state,
    city,
    postcode,
    region,
    district,
    lon,
    lat,
    units_purchased,
    loyalty_segment,
    
    -- Add calculated fields
    CASE 
        WHEN loyalty_segment >= 2 THEN 'high_value'
        WHEN loyalty_segment >= 1 THEN 'medium_value'
        ELSE 'low_value'
    END as customer_segment,
    
    -- Add data quality flags
    CASE 
        WHEN customer_id IS NOT NULL AND customer_name IS NOT NULL THEN 'valid'
        ELSE 'invalid'
    END as data_quality_flag,
    
    -- Add location quality
    CASE 
        WHEN lon IS NOT NULL AND lat IS NOT NULL THEN 'geocoded'
        ELSE 'not_geocoded'
    END as location_quality,
    
    -- Add business logic
    CASE 
        WHEN units_purchased > 20 THEN 'high_volume'
        WHEN units_purchased > 10 THEN 'medium_volume'
        ELSE 'low_volume'
    END as purchase_volume,
    
    -- Add audit fields
    current_timestamp() as transformation_timestamp,
    'silver_layer' as data_layer
    
FROM ${bronze_table_name}
${vendor_filter_condition} 