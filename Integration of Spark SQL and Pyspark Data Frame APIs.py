# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS itversity_retail CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE itversity_retail

# COMMAND ----------

# MAGIC %sql
# MAGIC USE itversity_retail

# COMMAND ----------

orders = spark.read.csv('dbfs:/public/retail_db/orders', schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING')

# COMMAND ----------

orders.createOrReplaceTempView('orders_v')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables

# COMMAND ----------

display(
    spark.sql(
        """
          SELECT order_status, count(*) order_count
          FROM orders_v
          GROUP BY order_status
          ORDER BY 2 DESC
          """
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_status, count(*) order_count
# MAGIC FROM orders_v
# MAGIC GROUP BY order_status
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

from pyspark.sql.functions import count, lit

# COMMAND ----------

display(orders.groupBy('order_status').agg(count(lit(1)).alias('order_count')))

# COMMAND ----------

order_count_by_status = orders.groupBy('order_status').agg(count(lit(1)).alias('order_count'))

# COMMAND ----------

order_count_by_status.write.saveAsTable('itversity_retail.order_count_by_status', format='delta', mode='overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM order_count_by_status

# COMMAND ----------

