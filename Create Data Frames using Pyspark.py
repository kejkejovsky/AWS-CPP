# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %fs ls /public/retail_db/orders

# COMMAND ----------

display(spark.read.csv('dbfs:/public/retail_db/orders'))

# COMMAND ----------

spark.read.csv('dbfs:/public/retail_db/orders', schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING')

# COMMAND ----------

display(spark.read.csv('dbfs:/public/retail_db/orders', schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING'))

# COMMAND ----------

sales = [[1, 1000, 12.5], [2, 1200, 10], [3, 750, 20]]

# COMMAND ----------

sale = sales[0]

# COMMAND ----------

sale[1] * sale[2] / 100

# COMMAND ----------

commision_amounts = []
for sale in sales:
    commision_amounts.append(sale[1] * sale[2] / 100)

commision_amounts

# COMMAND ----------

import pandas as pd

# COMMAND ----------

sales_df = pd.DataFrame(sales, columns=['sale_id', 'sale_quantity', 'sale_price'])

# COMMAND ----------

sales_df.apply(lambda rec: (rec['sale_quantity'] * rec['sale_price'])/100, axis = 1)

# COMMAND ----------

df = spark.read.csv('dbfs:/public/retail_db/orders', schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING')

# COMMAND ----------

df.columns

# COMMAND ----------

display(df.select('*'))

# COMMAND ----------

display(df.select('order_status').distinct().orderBy('order_status'))

# COMMAND ----------

df.select('order_date', 'order_status').distinct().orderBy('order_date', 'order_status').count()

# COMMAND ----------

help(df.drop)

# COMMAND ----------

display(df.drop('order_customer_id', 'order_status'))

# COMMAND ----------

from pyspark.sql.functions import date_format, cast

# COMMAND ----------

display(df.select('order_id', 'order_date', cast('int', date_format('order_date', 'yyyyMM')).alias('order_month')))

# COMMAND ----------

display(df.withColumn('order_month', cast('int', date_format('order_date', 'yyyyMM'))))

# COMMAND ----------

display(df.drop('order_customer_id').withColumn('order_month', cast('int', date_format('order_date', 'yyyyMM'))))

# COMMAND ----------

df.write.format('delta').save('dbfs:/public/retail_db_delta/orders')

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db_delta/orders

# COMMAND ----------

display(spark.read.format('delta').load('dbfs:/public/retail_db_delta/orders'))

# COMMAND ----------

