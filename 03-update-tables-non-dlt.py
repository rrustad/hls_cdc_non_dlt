# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from demo_resources import table_exists

# COMMAND ----------

dbutils.widgets.text('catalog', 'hls_ingest')
catalog = dbutils.widgets.get('catalog')

dbutils.widgets.text('schema', 'clarity')
schema = dbutils.widgets.get('schema')

ingest_path = f"/Volumes/{catalog}/{schema}/ingest/"
source_path = f"/Volumes/{catalog}/{schema}/source/"

# COMMAND ----------

spark.sql(f'use {catalog}.{schema}')

# COMMAND ----------

tables = [src_path[1].replace('/','') for src_path in dbutils.fs.ls(ingest_path)]
tables

# COMMAND ----------

def upsertToEncounters(microBatchOutputDF, batchId):
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("updates")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchOutputDF.sparkSession.sql(f"""
    MERGE INTO {catalog}.{schema}.encounters t
    USING updates s
    ON s.Id = t.Id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)
  
stream = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    # The schema location directory keeps track of your data schema over time
    .option("cloudFiles.schemaLocation", f"{source_path}/schemas/encounters")
    .load(f"{ingest_path}/encounters")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{source_path}/checkpoints/encounters")
    .trigger(availableNow=True)
    )

if table_exists(f"{catalog}.{schema}.encounters", spark):
  (stream
    .foreachBatch(upsertToEncounters)
    .outputMode("update")
    .start()
    .awaitTermination()
  )
else:
    (stream
    .outputMode("append")
    # no need to specify location - encounters is a managed table
    .toTable("encounters")
    .awaitTermination()
    )

# COMMAND ----------

spark.sql(f'describe history {catalog}.{schema}.encounters').display()

# COMMAND ----------

# Less complex code because we're updating them via append only
for table in tables:
  if table != 'encounters':
    print(table)
    (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      # The schema location directory keeps track of your data schema over time
      .option("cloudFiles.schemaLocation", f"{source_path}/schemas/{table}")
      .load(f"{ingest_path}/{table}")
      .writeStream
      .option("checkpointLocation", f"{source_path}/checkpoints/{table}")
      .trigger(availableNow=True)
      .toTable(f"{catalog}.{schema}.{table}")
      .awaitTermination()
    )

# COMMAND ----------


