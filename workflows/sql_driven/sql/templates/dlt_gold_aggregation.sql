-- DLT Gold Aggregation Template
-- This template demonstrates DLT-specific syntax for gold layer aggregation
-- Parameters: ${silver_table}, ${gold_table}, ${aggregation_type}

-- DLT Materialized View for Business Insights
CREATE OR REFRESH MATERIALIZED VIEW ${gold_table}
COMMENT "Gold layer aggregation for business insights and analytics"
TBLPROPERTIES ("quality" = "gold", "layer" = "gold")
AS 
SELECT 
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
  COUNT(CASE WHEN clean_email IS NOT NULL THEN 1 END) AS orders_with_email,
  COUNT(CASE WHEN clean_email IS NULL THEN 1 END) AS orders_without_email,
  
  -- Metadata
  current_timestamp() AS gold_processing_time,
  'gold_layer' AS data_layer
  
FROM LIVE.${silver_table}
WHERE order_timestamp IS NOT NULL
GROUP BY 
  date(order_timestamp),
  hour(order_timestamp),
  customer_id,
  product_id,
  order_category 