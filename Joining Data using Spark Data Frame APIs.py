# Databricks notebook source
# MAGIC %fs ls dbfs:/public/retail_db

# COMMAND ----------

orders_df = spark.read.csv('dbfs:/public/retail_db/orders', schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING')

# COMMAND ----------

display(orders_df)

# COMMAND ----------

orders_df.count()

# COMMAND ----------

order_items_df = spark.read.csv('dbfs:/public/retail_db/order_items', schema='order_item_id INT, order_item_order_id INT, order_item_product_id INT, order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT')

# COMMAND ----------

display(order_items_df)

# COMMAND ----------

order_items_df.count()

# COMMAND ----------

help(orders_df.join)

# COMMAND ----------

order_details_df = orders_df.join(order_items_df, orders_df.order_id == order_items_df.order_item_order_id)

# COMMAND ----------

display(order_details_df.select('order_id', 'order_date', 'order_customer_id', 'order_item_subtotal'))

# COMMAND ----------

display(order_details_df.select(orders_df['*'], order_items_df['order_item_subtotal']))

# COMMAND ----------

orders_df.filter("order_status IN ('COMPLETE', 'CLOSED') AND date_format(order_date, 'yyyyMM') = 201401").count()

# COMMAND ----------

from pyspark.sql.functions import sum, round, col

# COMMAND ----------

display(orders_df.filter("order_status IN ('COMPLETE', 'CLOSED') AND date_format(order_date, 'yyyyMM') = 201401").\
    join(order_items_df, orders_df.order_id == order_items_df.order_item_order_id).\
    groupBy('order_date').\
    agg(round(sum('order_item_subtotal'), 2).alias('revenue')).\
    orderBy(col('revenue').desc()))

# COMMAND ----------

