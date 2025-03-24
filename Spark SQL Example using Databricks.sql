-- Databricks notebook source
SELECT current_date

-- COMMAND ----------

SELECT * FROM TEXT.`/public/retail_db/orders/`

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders(
  order_id INT,
  order_date STRING,
  order_customer_id INT,
  order_status STRING
)USING CSV
OPTIONS(
  path='/public/retail_db/orders',
  sep=','
)

-- COMMAND ----------

DESCRIBE orders

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW order_items(
  order_item_id INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_subtotal FLOAT,
  order_item_product_price FLOAT
)USING CSV
OPTIONS(
  path='/public/retail_db/order_items',
  sep=','
)

-- COMMAND ----------

SELECT DISTINCT order_status FROM orders

-- COMMAND ----------

SELECT o.order_date, oi.order_item_product_id, round(sum(oi.order_item_subtotal), 2) revenue
FROM orders o 
JOIN order_items oi ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY 1, 2
ORDER BY 1, 3 DESC

-- COMMAND ----------

INSERT OVERWRITE DIRECTORY 'dbfs:/public/retail_db/daily_product_revenue'
USING PARQUET
SELECT o.order_date, oi.order_item_product_id, round(sum(oi.order_item_subtotal), 2) revenue
FROM orders o 
JOIN order_items oi ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY 1, 2

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/public/retail_db/daily_product_revenue

-- COMMAND ----------

SELECT * FROM PARQUET.`/public/retail_db/daily_product_revenue/`
ORDER BY order_date, revenue DESC

-- COMMAND ----------

