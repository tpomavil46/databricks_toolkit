-- Standard Bronze Ingestion Template (Spark SQL)
-- This template works in regular Spark SQL context
-- Parameters: ${source_path}, ${table_name}, ${file_format}, ${options}

-- Create bronze table from raw data with Auto Loader
CREATE OR REPLACE TABLE ${table_name}
AS SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file,
  _metadata.file_size AS file_size,
  _metadata.file_modification_time AS file_timestamp
FROM cloud_files("dbfs:${source_path}", "${file_format}", map("cloudFiles.inferColumnTypes", "true", "cloudFiles.schemaLocation", "dbfs:/tmp/schema_location")) 