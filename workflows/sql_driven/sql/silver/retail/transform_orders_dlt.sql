-- DLT Orders Transformation (Based on Training Example)
-- This demonstrates DLT-specific syntax for orders transformation with data quality
-- Parameters: ${bronze_table}

-- DLT Streaming Table with Data Quality Constraints
CREATE OR REFRESH STREAMING TABLE orders_silver
  (CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE)
  (CONSTRAINT valid_amount EXPECT (amount >= 0) ON VIOLATION DROP)
  (CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP)
COMMENT "Silver layer orders transformation with data quality enforcement"
TBLPROPERTIES ("quality" = "silver", "layer" = "silver")
AS 
SELECT 
  -- Core business fields
  order_id,
  customer_id,
  product_id,
  amount,
  
  -- Data type casting and validation
  timestamp(order_timestamp) AS order_timestamp,
  CAST(amount AS DECIMAL(10,2)) AS clean_amount,
  
  -- Business logic and enrichment
  CASE 
    WHEN amount >= 1000 THEN 'high_value'
    WHEN amount >= 100 THEN 'medium_value'
    ELSE 'low_value'
  END AS order_category,
  
  -- Data quality flags
  CASE 
    WHEN order_timestamp > "2021-01-01" THEN 'valid_date'
    ELSE 'invalid_date'
  END AS date_quality_flag,
  
  CASE 
    WHEN amount >= 0 THEN 'valid_amount'
    ELSE 'invalid_amount'
  END AS amount_quality_flag,
  
  -- Metadata
  current_timestamp() AS silver_processing_time,
  'silver_layer' AS data_layer
  
FROM STREAM(LIVE.${bronze_table})
WHERE order_timestamp > "2021-01-01"
  AND amount >= 0
  AND customer_id IS NOT NULL 