# Databricks notebook source
orders_df = spark.read.csv('dbfs:/public/retail_db/orders', schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING')

# COMMAND ----------

order_items_df = spark.read.csv('dbfs:/public/retail_db/order_items', schema='order_item_id INT, order_item_order_id INT, order_item_product_id INT, order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT')

# COMMAND ----------

from pyspark.sql.functions import round, sum, col

# COMMAND ----------

daily_revenue_df = orders_df.filter("order_status IN ('COMPLETE', 'CLOSED')").\
    join(order_items_df, orders_df.order_id == order_items_df.order_item_order_id).\
    groupBy('order_date').\
    agg(round(sum('order_item_subtotal'), 2).alias('revenue'))

# COMMAND ----------

display(daily_revenue_df.orderBy('order_date'))

# COMMAND ----------

daily_product_revenue_df = orders_df.filter("order_status IN ('COMPLETE', 'CLOSED')").\
    join(order_items_df, orders_df.order_id == order_items_df.order_item_order_id).\
    groupBy('order_date', 'order_item_product_id').\
    agg(round(sum('order_item_subtotal'), 2).alias('revenue'))

# COMMAND ----------

display(daily_product_revenue_df.filter("order_date = '2014-01-01'").orderBy('order_date', col('revenue').desc()))

# COMMAND ----------

from pyspark.sql.functions import dense_rank, rank, col

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

display(
    daily_product_revenue_df.\
        filter("order_date = '2014-01-01'").\
        withColumn('drnk', dense_rank().over(Window.orderBy(col('revenue').desc()))).
        orderBy('order_date', col('revenue').desc())
)

# COMMAND ----------

display(
    daily_product_revenue_df.\
        withColumn('drnk', dense_rank().over(Window.partitionBy('order_date').orderBy(col('revenue').desc()))).
        orderBy('order_date', col('revenue').desc())
)

# COMMAND ----------

display(
    daily_product_revenue_df.\
        filter("order_date = '2014-01-01'").\
        withColumn('drnk', dense_rank().over(Window.orderBy(col('revenue').desc()))).\
        filter("drnk <= 5")
        .orderBy('order_date', col('revenue').desc())
)

# COMMAND ----------

display(
    daily_product_revenue_df.\
        withColumn('drnk', dense_rank().over(Window.partitionBy('order_date').orderBy(col('revenue').desc()))).\
        filter("drnk <= 5").\
        orderBy('order_date', col('revenue').desc())
)

# COMMAND ----------

from pyspark.sql.functions import date_format, sum, round

# COMMAND ----------

display(
    daily_product_revenue_df.\
        filter("date_format(order_date, 'yyyyMM') = 201401").\
        groupBy(date_format('order_date', 'yyyyMM').alias('order_month'), 'order_item_product_id').\
        agg(round(sum('revenue'), 2).alias('revenue')).\
        withColumn('drnk', dense_rank().over(Window.orderBy(col('revenue').desc()))).\
        filter("drnk <= 5")
)

# COMMAND ----------

display(
    daily_product_revenue_df.\
        groupBy(date_format('order_date', 'yyyyMM').alias('order_month'), 'order_item_product_id').\
        agg(round(sum('revenue'), 2).alias('revenue')).\
        withColumn('drnk', dense_rank().over(Window.partitionBy('order_month').orderBy(col('revenue').desc()))).\
        filter("drnk <= 5")
)

# COMMAND ----------

spec = Window.partitionBy('order_month').orderBy(col('revenue').desc())

# COMMAND ----------

display(
    daily_product_revenue_df.\
        groupBy(date_format('order_date', 'yyyyMM').alias('order_month'), 'order_item_product_id').\
        agg(round(sum('revenue'), 2).alias('revenue')).\
        withColumn('drnk', dense_rank().over(spec)).\
        filter("drnk <= 5")
)

# COMMAND ----------

