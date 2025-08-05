-- Simple Gold Aggregation Template (Spark SQL)
-- This template works with regular Spark SQL and matches our test data
-- Parameters: ${silver_table}, ${gold_table}

-- Create gold table with business analytics
CREATE OR REPLACE TABLE ${gold_table}
AS SELECT 
  -- Time-based aggregations
  date(order_timestamp) AS order_date,
  hour(order_timestamp) AS order_hour,
  
  -- Product aggregations
  id,
  name,
  category,
  COUNT(*) AS total_products,
  SUM(clean_price) AS total_revenue,
  AVG(clean_price) AS avg_price,
  MAX(clean_price) AS max_price,
  MIN(clean_price) AS min_price,
  
  -- Category analysis
  product_category,
  COUNT(CASE WHEN product_category = 'high_value' THEN 1 END) AS high_value_products,
  COUNT(CASE WHEN product_category = 'medium_value' THEN 1 END) AS medium_value_products,
  COUNT(CASE WHEN product_category = 'low_value' THEN 1 END) AS low_value_products,
  
  -- Quality metrics
  COUNT(CASE WHEN date_quality_flag = 'valid_date' THEN 1 END) AS valid_date_products,
  COUNT(CASE WHEN price_quality_flag = 'valid_price' THEN 1 END) AS valid_price_products,
  
  -- Revenue analysis
  SUM(CASE WHEN product_category = 'high_value' THEN clean_price ELSE 0 END) AS high_value_revenue,
  SUM(CASE WHEN product_category = 'medium_value' THEN clean_price ELSE 0 END) AS medium_value_revenue,
  SUM(CASE WHEN product_category = 'low_value' THEN clean_price ELSE 0 END) AS low_value_revenue,
  
  -- Metadata
  current_timestamp() AS gold_processing_time,
  'gold_layer' AS data_layer
  
FROM ${silver_table}
WHERE order_timestamp IS NOT NULL
GROUP BY 
  date(order_timestamp),
  hour(order_timestamp),
  id,
  name,
  category,
  product_category 