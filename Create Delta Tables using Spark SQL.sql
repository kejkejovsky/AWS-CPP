-- Databricks notebook source
DROP DATABASE IF EXISTS itversity_retail_db CASCADE

-- COMMAND ----------

SET spark.sql.warehouse.dir

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/Volume

-- COMMAND ----------

CREATE DATABASE itversity_retail_db

-- COMMAND ----------

DESCRIBE DATABASE itversity_retail_db

-- COMMAND ----------

DROP DATABASE IF EXISTS itversity_retail_db

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS itversity_retail_db MANAGED LOCATION 'gs://databricks-3258320380614508/3258320380614508/public/warehouse/itversity_retail_db.db'

-- COMMAND ----------

USE itversity_retail_db

-- COMMAND ----------

CREATE TABLE itversity_retail_db.orders (
  order_id INT,
  order_date STRING,
  order_customer_id INT,
  order_status STRING
)USING DELTA

-- COMMAND ----------

DESCRIBE FORMATTED orders

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/public/warehouse/itversity_retail_db.db/

-- COMMAND ----------

SHOW CREATE TABLE orders

-- COMMAND ----------

SELECT * FROM JSON.`dbfs:/public/retail_db_json/orders`

-- COMMAND ----------

COPY INTO orders
FROM (
  SELECT 
    CAST(order_id AS INT) AS order_id,
    order_date,
    CAST(order_customer_id AS INT),
    order_status
  FROM 'dbfs:/public/retail_db_json/orders'
)
FILEFORMAT = JSON

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/public/warehouse/itversity_retail_db.db/__unitystorage/schemas/c7d787d0-5572-4d0a-996a-5abb18eb1f09/tables/3e1c5c24-4aaa-4134-a4e1-d76534c4c9cd/

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

SELECT count(*) FROM JSON.`dbfs:/public/retail_db_json/orders`

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE  TABLE order_items(
  order_item_id BIGINT,
  order_item_order_id BIGINT,
  order_item_product_id BIGINT,
  order_item_quantity BIGINT,
  order_item_subtotal DOUBLE,
  order_item_product_price DOUBLE
) USING DELTA


-- COMMAND ----------

DESCRIBE FORMATTED order_items

-- COMMAND ----------

DROP TABLE order_items

-- COMMAND ----------

COPY INTO order_items
FROM 'dbfs:/public/retail_db_json/order_items'
FILEFORMAT = JSON

-- COMMAND ----------

