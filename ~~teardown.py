# Databricks notebook source
dbutils.widgets.text('external_location_path', 'abfss://hls-ingest@oneenvadls.dfs.core.windows.net/')
external_location_path = dbutils.widgets.get('external_location_path')

dbutils.widgets.text('external_location_cred', 'oneenv-adls')
external_location_cred = dbutils.widgets.get('external_location_cred')

dbutils.widgets.text('external_location_name', 'hls_ingest')
external_location_name = dbutils.widgets.get('external_location_name')

dbutils.widgets.text('catalog', 'hls_ingest')
catalog = dbutils.widgets.get('catalog')

dbutils.widgets.text('schema', 'clarity')
schema = dbutils.widgets.get('schema')

# COMMAND ----------

spark.sql(f'use {catalog}.{schema}')

# COMMAND ----------

dbutils.fs.rm(f'/Volumes/{catalog}/{schema}/ingest',True)
dbutils.fs.rm(f'/Volumes/{catalog}/{schema}/landing',True)
dbutils.fs.rm(f'/Volumes/{catalog}/{schema}/source',True)

# COMMAND ----------

spark.sql('drop table encounters')

# COMMAND ----------

spark.sql(f"""
drop VOLUME IF EXISTS {catalog}.{schema}.source;
""")

# COMMAND ----------

spark.sql(f"""
drop VOLUME IF EXISTS {catalog}.{schema}.ingest;
""")

# COMMAND ----------

spark.sql(f"""
DROP VOLUME IF EXISTS {catalog}.{schema}.landing_zone
""")

# COMMAND ----------

spark.sql(f"""
DROP schema IF EXISTS {catalog}.{schema} cascade
""")

# COMMAND ----------

spark.sql(f"""
DROP catalog IF EXISTS {catalog} CASCADE
""")

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: Deleting catalog in managed storage doesn't delete the managed storage. - for now delete manually

# COMMAND ----------

dbutils.fs.rm(external_location_path, True)

# COMMAND ----------


