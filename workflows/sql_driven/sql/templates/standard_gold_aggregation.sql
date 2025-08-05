-- Standard Gold Aggregation Template (Spark SQL)
-- This template works in regular Spark SQL context
-- Parameters: ${silver_table}, ${gold_table}

-- Create gold table with business analytics
CREATE OR REPLACE TABLE ${gold_table}
AS SELECT 
  -- Time-based aggregations
  date(order_timestamp) AS order_date,
  hour(order_timestamp) AS order_hour,
  
  -- Customer aggregations
  customer_id,
  COUNT(*) AS total_orders,
  SUM(clean_amount) AS total_revenue,
  AVG(clean_amount) AS avg_order_value,
  MAX(clean_amount) AS max_order_value,
  MIN(clean_amount) AS min_order_value,
  
  -- Product aggregations
  product_id,
  COUNT(DISTINCT customer_id) AS unique_customers,
  
  -- Category analysis
  order_category,
  COUNT(CASE WHEN order_category = 'high_value' THEN 1 END) AS high_value_orders,
  COUNT(CASE WHEN order_category = 'medium_value' THEN 1 END) AS medium_value_orders,
  COUNT(CASE WHEN order_category = 'low_value' THEN 1 END) AS low_value_orders,
  
  -- Quality metrics
  COUNT(CASE WHEN date_quality_flag = 'valid_date' THEN 1 END) AS valid_date_orders,
  COUNT(CASE WHEN amount_quality_flag = 'valid_amount' THEN 1 END) AS valid_amount_orders,
  
  -- Revenue analysis
  SUM(CASE WHEN order_category = 'high_value' THEN clean_amount ELSE 0 END) AS high_value_revenue,
  SUM(CASE WHEN order_category = 'medium_value' THEN clean_amount ELSE 0 END) AS medium_value_revenue,
  SUM(CASE WHEN order_category = 'low_value' THEN clean_amount ELSE 0 END) AS low_value_revenue,
  
  -- Metadata
  current_timestamp() AS gold_processing_time,
  'gold_layer' AS data_layer
  
FROM ${silver_table}
WHERE order_timestamp IS NOT NULL
GROUP BY 
  date(order_timestamp),
  hour(order_timestamp),
  customer_id,
  product_id,
  order_category 