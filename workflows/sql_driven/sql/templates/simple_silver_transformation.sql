-- Simple Silver Transformation Template (Spark SQL)
-- This template works with regular Spark SQL and matches our test data
-- Parameters: ${bronze_table}, ${silver_table}

-- Create silver table with data quality checks
CREATE OR REPLACE TABLE ${silver_table}
AS SELECT 
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
  
FROM ${bronze_table}
WHERE timestamp > "2021-01-01"
  AND price >= 0
  AND id IS NOT NULL 