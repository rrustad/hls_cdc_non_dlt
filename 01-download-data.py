# Databricks notebook source
from demo_resources import download_file_from_git, is_folder_empty
from pyspark.sql.functions import col

# COMMAND ----------

reset_all_data = True

# COMMAND ----------

landing_volume_folder = "/Volumes/hls_ingest/clarity/landing"

# COMMAND ----------

folders = ["/encounters", "/patients", "/conditions", "/medications", "/immunizations"]

# COMMAND ----------

if reset_all_data or any([is_folder_empty(landing_volume_folder+f, dbutils) for f in folders]):
  if reset_all_data:
    assert len(landing_volume_folder) > 5
    dbutils.fs.rm(landing_volume_folder, True)
  for f in folders:
      download_file_from_git(landing_volume_folder+f, "databricks-demos", "dbdemos-dataset", "/hls/synthea/landing_zone_parquet"+f)
  (
    spark.read
    .parquet('/Volumes/hls_ingest/clarity/landing/encounters/')
    .withColumn('START', col('START').cast('timestamp'))
    .withColumn('STOP', col('STOP').cast('timestamp'))
    .write
    .mode('overwrite')
    .parquet('/Volumes/hls_ingest/clarity/landing/encounters/')
  )

else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")

# COMMAND ----------


