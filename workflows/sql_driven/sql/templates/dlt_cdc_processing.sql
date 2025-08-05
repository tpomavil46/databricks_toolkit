-- DLT CDC Processing Template
-- This template demonstrates DLT-specific syntax for Change Data Capture processing
-- Parameters: ${bronze_table}, ${silver_table}, ${key_column}

-- DLT Streaming Table with CDC Processing
CREATE OR REFRESH STREAMING TABLE ${silver_table}
  (CONSTRAINT valid_operation EXPECT (operation IN ('INSERT', 'UPDATE', 'DELETE')) ON VIOLATION DROP)
  (CONSTRAINT valid_timestamp EXPECT (timestamp IS NOT NULL) ON VIOLATION DROP)
COMMENT "Silver layer CDC processing with APPLY CHANGES"
TBLPROPERTIES ("quality" = "silver", "layer" = "silver")
AS 
SELECT 
  *,
  current_timestamp() AS cdc_processing_time
FROM STREAM(LIVE.${bronze_table})

-- Apply Changes for CDC Processing
APPLY CHANGES INTO LIVE.${silver_table}_cdc
FROM STREAM(LIVE.${bronze_table})
KEYS (${key_column})
APPLY AS DELETE WHEN operation = "DELETE"
ELSE APPLY AS INSERT 