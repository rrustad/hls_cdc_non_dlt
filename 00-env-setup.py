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

dbutils.widgets.text('init', 'False')
init = dbutils.widgets.get('init') == 'True'
 
 

# COMMAND ----------

if init == False:
  dbutils.notebook.exit(1)

# COMMAND ----------

spark.sql(f"""
CREATE EXTERNAL LOCATION  IF NOT EXISTS  {external_location_name}
    URL '{external_location_path}'
    WITH (STORAGE  CREDENTIAL `{external_location_cred}`) 
""")

# COMMAND ----------

catalog_path = external_location_path + 'catalog'
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog}
MANAGED LOCATION '{catalog_path}'
""")

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
""")

# COMMAND ----------

landing_zone_path = external_location_path+'landing'
dbutils.fs.mkdirs(landing_zone_path)
spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.landing
LOCATION '{landing_zone_path}';
""")

# COMMAND ----------

ingest_path = external_location_path+'ingest'
dbutils.fs.mkdirs(ingest_path)
spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.ingest
LOCATION '{ingest_path}';
""")

# COMMAND ----------

source_path = external_location_path+'source'
dbutils.fs.mkdirs(source_path)
spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.source
LOCATION '{source_path}';
""")
