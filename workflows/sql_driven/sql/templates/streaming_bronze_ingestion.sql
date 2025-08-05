-- Streaming Bronze Ingestion Template (DLT)
-- This template uses DLT streaming tables with Auto Loader for incremental processing
-- Parameters: ${source_path}, ${table_name}, ${file_format}, ${options}

-- DLT Streaming Table with Auto Loader for incremental data ingestion
CREATE OR REFRESH STREAMING TABLE ${table_name}
  (CONSTRAINT valid_timestamp EXPECT (processing_time IS NOT NULL) ON VIOLATION FAIL UPDATE)
  (CONSTRAINT valid_source EXPECT (source_file IS NOT NULL) ON VIOLATION DROP)
COMMENT "Bronze layer streaming ingestion using Auto Loader for incremental processing"
TBLPROPERTIES ("quality" = "bronze", "layer" = "bronze")
AS
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file,
  _metadata.file_size AS file_size,
  _metadata.file_modification_time AS file_timestamp
FROM cloud_files("dbfs:${source_path}", "${file_format}", map("cloudFiles.inferColumnTypes", "true", "cloudFiles.schemaLocation", "dbfs:/tmp/schema_location")) 