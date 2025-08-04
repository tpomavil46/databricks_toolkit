-- Gold Layer: KPI Generation
-- This SQL file demonstrates parameterized KPI generation for the gold layer
-- Parameters: ${silver_table_name}, ${gold_table_name}, ${vendor_filter}

-- Generate KPIs and business metrics from silver data
CREATE OR REPLACE TABLE ${gold_table_name}
AS SELECT 
    -- Geographic KPIs
    state,
    region,
    district,
    COUNT(*) as total_customers,
    AVG(units_purchased) as avg_units_purchased,
    SUM(units_purchased) as total_units_purchased,
    
    -- Customer Segment KPIs
    customer_segment,
    COUNT(*) as segment_count,
    AVG(units_purchased) as avg_units_by_segment,
    
    -- Loyalty KPIs
    loyalty_segment,
    COUNT(*) as loyalty_count,
    AVG(units_purchased) as avg_units_by_loyalty,
    
    -- Purchase Volume KPIs
    purchase_volume,
    COUNT(*) as volume_count,
    
    -- Data Quality KPIs
    COUNT(CASE WHEN data_quality_flag = 'valid' THEN 1 END) as valid_records,
    COUNT(CASE WHEN data_quality_flag = 'invalid' THEN 1 END) as invalid_records,
    
    -- Location Quality KPIs
    COUNT(CASE WHEN location_quality = 'geocoded' THEN 1 END) as geocoded_records,
    COUNT(CASE WHEN location_quality = 'not_geocoded' THEN 1 END) as not_geocoded_records,
    
    -- Time-based KPIs
    DATE(transformation_timestamp) as customer_date,
    
    -- Audit fields
    current_timestamp() as kpi_generation_timestamp,
    'gold_layer' as data_layer
    
FROM ${silver_table_name}
${vendor_filter_condition}
    
GROUP BY 
    state,
    region,
    district,
    customer_segment,
    loyalty_segment,
    purchase_volume,
    DATE(transformation_timestamp) 