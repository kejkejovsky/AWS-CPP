# Databricks notebook source
print('Hello world')

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TEMPORARY VIEW orders(
# MAGIC   order_id INT,
# MAGIC   order_date STRING,
# MAGIC   order_customer_id INT,
# MAGIC   order_status STRING
# MAGIC ) USING CSV
# MAGIC OPTIONS (
# MAGIC   path='dbfs:/public/retail_db/orders/'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT order_status, count(*) as order_count
# MAGIC FROM orders
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

