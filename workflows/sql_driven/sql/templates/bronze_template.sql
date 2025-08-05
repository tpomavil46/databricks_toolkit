-- Bronze Layer Template
-- This template can be used for any bronze layer ingestion
-- Parameters: ${input_path}, ${bronze_table_name}

-- Create bronze table from raw data
CREATE OR REPLACE TABLE ${bronze_table_name}
AS SELECT 
    *,
    current_timestamp() as ingestion_timestamp,
    'bronze_layer' as data_layer
FROM `${input_path}` 