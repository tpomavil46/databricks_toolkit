-- DLT Silver Transformation Template
-- This template demonstrates DLT-specific syntax for silver layer transformation
-- Parameters: ${bronze_table}, ${silver_table}, ${quality_level}

-- DLT Streaming Table with Data Quality Constraints
CREATE OR REFRESH STREAMING TABLE ${silver_table}
  (CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE)
  (CONSTRAINT valid_timestamp EXPECT (timestamp > "2021-01-01") ON VIOLATION DROP)
  (CONSTRAINT valid_email EXPECT (rlike(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')) ON VIOLATION DROP)
  (CONSTRAINT valid_amount EXPECT (amount >= 0) ON VIOLATION DROP)
COMMENT "Silver layer transformation with data quality enforcement"
TBLPROPERTIES ("quality" = "silver", "layer" = "silver")
AS 
SELECT 
  -- Core business fields
  id,
  customer_id,
  product_id,
  amount,
  timestamp,
  
  -- Data quality and enrichment
  CASE 
    WHEN email IS NOT NULL AND rlike(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') 
    THEN email 
    ELSE NULL 
  END AS clean_email,
  
  -- Data type casting and validation
  CAST(timestamp AS TIMESTAMP) AS order_timestamp,
  CAST(amount AS DECIMAL(10,2)) AS clean_amount,
  
  -- Business logic
  CASE 
    WHEN amount >= 1000 THEN 'high_value'
    WHEN amount >= 100 THEN 'medium_value'
    ELSE 'low_value'
  END AS order_category,
  
  -- Metadata
  current_timestamp() AS silver_processing_time,
  'silver_layer' AS data_layer
  
FROM STREAM(LIVE.${bronze_table})
WHERE id IS NOT NULL 
  AND timestamp > "2021-01-01" 