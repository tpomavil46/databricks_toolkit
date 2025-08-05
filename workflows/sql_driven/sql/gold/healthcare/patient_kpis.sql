-- Healthcare Gold Layer: Patient KPI Generation
-- This SQL file demonstrates parameterized KPI generation for healthcare patients
-- Parameters: ${silver_table_name}, ${gold_table_name}, ${vendor_filter}

-- Generate KPIs and business metrics from silver data
CREATE OR REPLACE TABLE ${gold_table_name}
AS SELECT 
    -- Hospital Ward KPIs
    hospital_ward,
    COUNT(*) as total_patients,
    AVG(length_of_stay) as avg_length_of_stay,
    SUM(length_of_stay) as total_bed_days,
    
    -- Doctor KPIs
    doctor_id,
    COUNT(*) as patients_under_care,
    AVG(length_of_stay) as avg_patient_los,
    
    -- Stay Category KPIs
    stay_category,
    COUNT(*) as category_count,
    AVG(length_of_stay) as avg_los_by_category,
    
    -- Severity KPIs
    severity_level,
    COUNT(*) as severity_count,
    AVG(length_of_stay) as avg_los_by_severity,
    
    -- Data Quality KPIs
    COUNT(CASE WHEN data_quality_flag = 'valid' THEN 1 END) as valid_records,
    COUNT(CASE WHEN data_quality_flag = 'invalid' THEN 1 END) as invalid_records,
    
    -- Time-based KPIs
    DATE(transformation_timestamp) as patient_date,
    
    -- Audit fields
    current_timestamp() as kpi_generation_timestamp,
    'gold_layer' as data_layer
    
FROM ${silver_table_name}

GROUP BY 
    hospital_ward,
    doctor_id,
    stay_category,
    severity_level,
    DATE(transformation_timestamp) 