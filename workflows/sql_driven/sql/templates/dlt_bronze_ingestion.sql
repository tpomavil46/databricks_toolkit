-- DLT Bronze Ingestion Template with Auto Loader
-- This template demonstrates DLT-specific syntax for bronze layer ingestion
-- Parameters: ${source_path}, ${table_name}, ${file_format}, ${options}

-- DLT Streaming Table with Auto Loader
CREATE OR REFRESH STREAMING TABLE ${table_name}
  (CONSTRAINT valid_timestamp EXPECT (processing_time IS NOT NULL) ON VIOLATION FAIL UPDATE)
  (CONSTRAINT valid_source EXPECT (source_file IS NOT NULL) ON VIOLATION DROP)
COMMENT "Bronze layer ingestion using Auto Loader for incremental processing"
TBLPROPERTIES ("quality" = "bronze", "layer" = "bronze")
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file,
  _metadata.file_size AS file_size,
  _metadata.file_modification_time AS file_timestamp
FROM cloud_files("${source_path}", "${file_format}", map(${options})) 