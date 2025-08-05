-- Standard Silver Transformation Template (Spark SQL)
-- This template works in regular Spark SQL context
-- Parameters: ${bronze_table}, ${silver_table}

-- Create silver table with data quality checks
CREATE OR REPLACE TABLE ${silver_table}
AS SELECT 
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
  
  -- Data quality flags
  CASE 
    WHEN timestamp > "2021-01-01" THEN 'valid_date'
    ELSE 'invalid_date'
  END AS date_quality_flag,
  
  CASE 
    WHEN amount >= 0 THEN 'valid_amount'
    ELSE 'invalid_amount'
  END AS amount_quality_flag,
  
  -- Metadata
  current_timestamp() AS silver_processing_time,
  'silver_layer' AS data_layer
  
FROM ${bronze_table}
WHERE timestamp > "2021-01-01"
  AND amount >= 0
  AND customer_id IS NOT NULL 