-- DLT Orders Ingestion (Based on Training Example)
-- This demonstrates DLT-specific syntax for orders ingestion with Auto Loader
-- Parameters: ${source_path}

-- DLT Streaming Table with Auto Loader for Orders
CREATE OR REFRESH STREAMING TABLE orders_bronze
  (CONSTRAINT valid_timestamp EXPECT (processing_time IS NOT NULL) ON VIOLATION FAIL UPDATE)
  (CONSTRAINT valid_source EXPECT (source_file IS NOT NULL) ON VIOLATION DROP)
COMMENT "Bronze layer orders ingestion using Auto Loader for incremental processing"
TBLPROPERTIES ("quality" = "bronze", "layer" = "bronze")
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file,
  _metadata.file_size AS file_size,
  _metadata.file_modification_time AS file_timestamp
FROM cloud_files("${source_path}/orders", "json", map("cloudFiles.inferColumnTypes", "true")) 