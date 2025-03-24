# Databricks notebook source
[
    {'user_id': 1, 'user_fname': 'Scott', 'user_lname': 'Tiger'},
    {'user_id': 2, 'user_fname': 'Donald', 'user_lname': 'Duck'},
    {'user_id': 3, 'user_fname': 'Mickey', 'user_lname': 'Mouse'}
]

# COMMAND ----------

# MAGIC %fs ls dbfs:/public

# COMMAND ----------

orders_df = spark.read.csv('dbfs:/public/retail_db/orders', schema='order_id INT, order_date STRING, order_customer_id INT, order_status STRING')

# COMMAND ----------

display(orders_df)

# COMMAND ----------

from pyspark.sql.functions import date_format, cast

# COMMAND ----------

orders_df.withColumn('order_month', date_format('order_date', 'yyyyMM').cast('int')).printSchema()

# COMMAND ----------

orders_df.withColumn('order_month', cast('int', date_format('order_date', 'yyyyMM'))).printSchema()

# COMMAND ----------

display(orders_df.select('order_status').distinct())

# COMMAND ----------

orders_df.count()

# COMMAND ----------

orders_df.filter("order_status = 'COMPLETE'").count()

# COMMAND ----------

orders_df.filter("order_status IN ('COMPLETE', 'CLOSED') AND date_format(order_date, 'yyyyMM') = 201401").count()

# COMMAND ----------

from pyspark.sql.functions import count, col

# COMMAND ----------

display(orders_df.groupBy('order_status').agg(count('order_id').alias('order_count')).orderBy(col('order_count').desc()))

# COMMAND ----------

order_items_df = spark.read.csv('dbfs:/public/retail_db/order_items', schema='order_item_id INT, order_item_order_id INT, order_item_product_id INT, order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT')

# COMMAND ----------

display(order_items_df)

# COMMAND ----------

from pyspark.sql.functions import sum, round

# COMMAND ----------

display(order_items_df.groupBy('order_item_order_id').agg(round(sum('order_item_subtotal'), 2).alias('order_revenue')).orderBy('order_item_order_id'))

# COMMAND ----------

display(orders_df.groupBy(date_format('order_date', 'yyyyMM').cast('int').alias('order_month')).agg(count('order_id').alias('order_count')).orderBy('order_month'))

# COMMAND ----------

display(orders_df.orderBy(col('order_customer_id').desc(), col('order_date')))

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/online_retail/data-001

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/online_retail/data-001', header=True, inferSchema=True)

# COMMAND ----------

df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.filter('CustomerID IS NULL'))

# COMMAND ----------

display(df.filter('StockCode = 71053'))

# COMMAND ----------

display(df.filter('StockCode = 71053').orderBy('CustomerID'))

# COMMAND ----------

display(df.filter('StockCode = 71053').orderBy(col('CustomerID').asc_nulls_last()))

# COMMAND ----------

