-- Streaming Gold Aggregation Template (DLT)
-- This template uses DLT materialized views for real-time analytics
-- Parameters: ${silver_table}, ${gold_table}

-- DLT Materialized View for real-time business analytics
CREATE OR REFRESH MATERIALIZED VIEW ${gold_table}
COMMENT "Gold layer materialized view for real-time business insights"
TBLPROPERTIES ("quality" = "gold", "layer" = "gold")
AS
SELECT 
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
  
FROM LIVE.${silver_table}
WHERE order_timestamp IS NOT NULL
GROUP BY 
  date(order_timestamp),
  hour(order_timestamp),
  id,
  name,
  category,
  product_category 