-- Streaming Silver Transformation Template (DLT)
-- This template uses DLT streaming tables for incremental data transformation
-- Parameters: ${bronze_table}, ${silver_table}

-- DLT Streaming Table with Data Quality Constraints for incremental processing
CREATE OR REFRESH STREAMING TABLE ${silver_table}
  (CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE)
  (CONSTRAINT valid_timestamp EXPECT (timestamp > "2021-01-01") ON VIOLATION DROP)
  (CONSTRAINT valid_price EXPECT (price >= 0) ON VIOLATION DROP)
COMMENT "Silver layer streaming transformation with data quality enforcement"
TBLPROPERTIES ("quality" = "silver", "layer" = "silver")
AS
SELECT 
  -- Core business fields
  id,
  name,
  price,
  category,
  timestamp,
  
  -- Data type casting and validation
  CAST(timestamp AS TIMESTAMP) AS order_timestamp,
  CAST(price AS DECIMAL(10,2)) AS clean_price,
  
  -- Business logic
  CASE 
    WHEN price >= 100 THEN 'high_value'
    WHEN price >= 50 THEN 'medium_value'
    ELSE 'low_value'
  END AS product_category,
  
  -- Data quality flags
  CASE 
    WHEN timestamp > "2021-01-01" THEN 'valid_date'
    ELSE 'invalid_date'
  END AS date_quality_flag,
  
  CASE 
    WHEN price >= 0 THEN 'valid_price'
    ELSE 'invalid_price'
  END AS price_quality_flag,
  
  -- Metadata
  current_timestamp() AS silver_processing_time,
  'silver_layer' AS data_layer
  
FROM STREAM(LIVE.${bronze_table})
WHERE timestamp > "2021-01-01"
  AND price >= 0
  AND id IS NOT NULL 