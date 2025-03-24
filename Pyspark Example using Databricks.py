# Databricks notebook source
# MAGIC %fs ls dbfs:/public/retail_db/schemas.json

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM TEXT.`dbfs:/public/retail_db/schemas.json`

# COMMAND ----------

schemas_text = spark.read.text('dbfs:/public/retail_db/schemas.json', wholetext=True).first().value

# COMMAND ----------

import json

# COMMAND ----------

column_details = json.loads(schemas_text)['orders']

# COMMAND ----------

column_details

# COMMAND ----------

columns = [col['column_name'] for col in sorted(column_details, key=lambda x: x['column_position'])]

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM CSV.`dbfs:/public/retail_db/orders`

# COMMAND ----------

spark.read.csv('dbfs:/public/retail_db/orders', inferSchema=True).toDF(*columns).show()

# COMMAND ----------

orders = spark.read.csv('dbfs:/public/retail_db/orders', inferSchema=True).toDF(*columns)

# COMMAND ----------

from pyspark.sql.functions import count, col

# COMMAND ----------

orders.groupBy('order_status').agg(count('*').alias('order_count')).orderBy(col('order_count').desc()).show()

# COMMAND ----------

def get_columns(schemas_file, ds_name):
    schema_text = spark.read.text(schemas_file, wholetext=True).first().value
    schemas = json.loads(schema_text)
    column_details = schemas[ds_name]
    columns = [col['column_name'] for col in sorted(column_details, key=lambda x: x['column_position'])]
    return columns

# COMMAND ----------

get_columns('dbfs:/public/retail_db/schemas.json', 'orders')

# COMMAND ----------

ds_list = [
    'departments', 'categories', 'products',
    'customers', 'orders', 'order_items'
]

# COMMAND ----------

base_dir = 'dbfs:/public/retail_db'

# COMMAND ----------

for  ds in ds_list:
    print(f'Processing {ds} data')
    columns = get_columns(f'{base_dir}/schemas.json', ds)
    df = spark.read.csv(f'{base_dir}/{ds}', inferSchema=True).toDF(*columns)
    df.write.mode('overwrite').parquet(f'{base_dir}_parquet/{ds}')

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db_parquet/

# COMMAND ----------

