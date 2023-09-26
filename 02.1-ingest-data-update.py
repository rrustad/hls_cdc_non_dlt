# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as f

# COMMAND ----------

dbutils.widgets.text('catalog', 'hls_ingest')
catalog = dbutils.widgets.get('catalog')

dbutils.widgets.text('schema', 'clarity')
schema = dbutils.widgets.get('schema')

landed_path = f"/Volumes/{catalog}/{schema}/landing/"
source_path = f"/Volumes/{catalog}/{schema}/source/"
ingest_path = f"/Volumes/{catalog}/{schema}/ingest/"

# COMMAND ----------

# MAGIC %md
# MAGIC 1. We're assuming that the cutoff to recieve new data for today is at midnight
# MAGIC 2. Let's assume it's midnight now, and we're updating the data. (ex. Midnight January 3)
# MAGIC 3. That means that the max timestamp of the dataset is sometime late 2 days ago (Late January 1)
# MAGIC 4. The data that we're focused on is yesterday's data (all data from Jan 2)

# COMMAND ----------

# We're assuming data is updated at midnight each night, so the data we're loading is from yesterday
yesterday = (
  spark.read.parquet(ingest_path+'encounters/')
  .select(f.date_trunc('dd', f.max('START')) + f.expr('INTERVAL 1 DAY'))
  ).collect()[0][0]
yesterday

# COMMAND ----------

# These are encounters that happened today
todays_encounters = (
  spark.read.parquet(landed_path+'encounters')
  .filter(f.date_trunc('dd',col('START')) == yesterday)
  # We're getting rid of discharges that happen in the future
  # We have current discharge info up until right now
  .withColumn('STOP', f.when(col('STOP') > yesterday + f.expr('INTERVAL 1 DAY'), None).otherwise(col('STOP')))
  )

# COMMAND ----------

todays_encounters.count()

# COMMAND ----------

# These are encounters that started in the past, but ended today
# We need to update the current encounter information
todays_discharge_updates = (
  spark.read.parquet(landed_path+'encounters')
  .filter(f.date_trunc('dd',col('STOP')) == yesterday)
  .filter(f.date_trunc('dd',col('START')) != yesterday)
  )


# COMMAND ----------

todays_discharge_updates.count()

# COMMAND ----------

(
  todays_encounters
  .union(todays_discharge_updates)
  .write
  .format('parquet')
  .mode('append')
  .save(ingest_path+'encounters')
  )

# COMMAND ----------

todays_encounters.createOrReplaceTempView('todays_encounters')

# for initial load, you have to specify the schema because the ingest table is empty
spark.read.parquet(landed_path+'/patients').createOrReplaceTempView('patients')
spark.read.schema(spark.table('patients').schema).parquet(ingest_path+'/patients').createOrReplaceTempView('patients_incremental')

df = spark.sql(
"""select * from patients
where patients.id in (select distinct PATIENT from todays_encounters)
and patients.id not in (select patients_incremental.id from patients_incremental)"""
)

df.write.mode('append').parquet(ingest_path+'/patients')

# COMMAND ----------


def update_enc_based_table(table):

  # for initial load, you have to specify the schema because the ingest table is empty
  spark.read.parquet(landed_path+f'/{table}').createOrReplaceTempView(f'{table}')
  spark.read.schema(spark.table(f'{table}').schema).parquet(ingest_path+f'/{table}').createOrReplaceTempView(f'{table}_incremental')

  df = spark.sql(
  f"""select * from {table}
  where {table}.ENCOUNTER in (select distinct id from todays_encounters)
  and {table}.ENCOUNTER not in (select distinct ENCOUNTER from {table}_incremental)
  """
  ).coalesce(1)

  df.write.mode('append').parquet(ingest_path+f'/{table}')

  

for table in dbutils.fs.ls(landed_path):
  if table.name not in ['encounters/', 'patients/']:
    print(table.name)
    update_enc_based_table(table.name.replace('/',''))

# COMMAND ----------


