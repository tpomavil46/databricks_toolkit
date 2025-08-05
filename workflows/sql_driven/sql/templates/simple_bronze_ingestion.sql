-- Simple Bronze Ingestion Template (Spark SQL)
-- This template works with regular Spark SQL (no Auto Loader)
-- Parameters: ${source_path}, ${table_name}

-- Create bronze table from raw data using regular Spark SQL
CREATE OR REPLACE TABLE ${table_name}
AS SELECT 
  *,
  current_timestamp() AS processing_time,
  'manual_ingestion' AS source_file,
  0 AS file_size,
  current_timestamp() AS file_timestamp
FROM ${source_path} 