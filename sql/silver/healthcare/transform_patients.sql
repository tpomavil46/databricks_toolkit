-- Healthcare Silver Layer: Patient Data Transformation
-- This SQL file demonstrates parameterized data transformation for healthcare patients
-- Parameters: ${bronze_table_name}, ${silver_table_name}, ${vendor_filter}

-- Transform bronze data into silver layer with cleaning and standardization
CREATE OR REPLACE TABLE ${silver_table_name}
AS SELECT 
    -- Clean and standardize column names
    patient_id,
    patient_name,
    age,
    gender,
    diagnosis,
    treatment,
    admission_date,
    discharge_date,
    length_of_stay,
    hospital_ward,
    doctor_id,
    
    -- Add calculated fields
    DATEDIFF(discharge_date, admission_date) as calculated_los,
    
    -- Add data quality flags
    CASE 
        WHEN patient_id IS NOT NULL AND patient_name IS NOT NULL THEN 'valid'
        ELSE 'invalid'
    END as data_quality_flag,
    
    -- Add business logic
    CASE 
        WHEN length_of_stay > 30 THEN 'long_stay'
        WHEN length_of_stay > 7 THEN 'medium_stay'
        ELSE 'short_stay'
    END as stay_category,
    
    -- Add severity classification
    CASE 
        WHEN diagnosis LIKE '%emergency%' OR diagnosis LIKE '%critical%' THEN 'high_severity'
        WHEN diagnosis LIKE '%surgery%' OR diagnosis LIKE '%procedure%' THEN 'medium_severity'
        ELSE 'low_severity'
    END as severity_level,
    
    -- Add audit fields
    current_timestamp() as transformation_timestamp,
    'silver_layer' as data_layer
    
FROM ${bronze_table_name}
${vendor_filter_condition} 