-- Ecommerce Bronze Layer: Order Data Ingestion
-- This SQL file demonstrates parameterized data ingestion for ecommerce orders
-- Parameters: ${input_path}, ${bronze_table_name}

-- Create bronze table from raw data
CREATE OR REPLACE TABLE ${bronze_table_name}
AS SELECT 
    *,
    current_timestamp() as ingestion_timestamp,
    'bronze_layer' as data_layer
FROM `${input_path}` 