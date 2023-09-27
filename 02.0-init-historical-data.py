# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from demo_resources import table_exists

# COMMAND ----------

dbutils.widgets.text('catalog', 'hls_ingest')
catalog = dbutils.widgets.get('catalog')

dbutils.widgets.text('schema', 'clarity')
schema = dbutils.widgets.get('schema')

landed_path = f"/Volumes/{catalog}/{schema}/landing/"
ingest_path = f"/Volumes/{catalog}/{schema}/ingest/"

# COMMAND ----------

if table_exists(f"{catalog}.{schema}.encounters", spark):
  dbutils.notebook.exit(0)

# COMMAND ----------

tables = [directory.name.replace('/','') for directory in dbutils.fs.ls(landed_path)]
tables

# COMMAND ----------

for dir_ in dbutils.fs.ls(landed_path):
  dbutils.fs.mkdirs(ingest_path+dir_.name)

# COMMAND ----------

encounters = (
  spark.read.parquet(landed_path+'encounters')
)
encounters.createOrReplaceTempView('encounters')

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(START) from encounters

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_trunc('dd', START) from encounters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Setup
# MAGIC We can see that the last date in the synthetic data is 7/3/23, and it has patient history going back over 100 years. It appears the dataset starts for real about 10 years prior (July '13) to the last date. For the sake of this demo, we'll cut off the last year to simulate "new data" coming in

# COMMAND ----------

demo_start_encounter_date = spark.sql("""
SELECT max(date_trunc('dd', START) - INTERVAL 365 DAY) FROM encounters
""").collect()[0][0]

demo_start_encounter_date

# COMMAND ----------

historical_encounters = (
  encounters
  .filter(col('START') <= demo_start_encounter_date)
  )

current_demo_time = (
  historical_encounters
  .select(f.date_trunc('dd', f.max('START')) + f.expr('INTERVAL 1 DAY'))
  ).collect()[0][0]

(
  historical_encounters
  .withColumn('STOP', f.when(col('STOP') > current_demo_time, None).otherwise(col('STOP')))
  .write
  .mode('overwrite')
  .parquet(ingest_path+"encounters")
  )

# COMMAND ----------

# Load in the historical encounter information
spark.read.parquet(ingest_path+'encounters/').createOrReplaceTempView('encounters_incremental')

# for initial load, you have to specify the schema because the ingest table is empty
spark.read.parquet(landed_path+'/patients').createOrReplaceTempView('patients')
spark.read.schema(spark.table('patients').schema).parquet(ingest_path+'/patients').createOrReplaceTempView('patients_incremental')

df = spark.sql(
"""select * from patients
where patients.id in (select distinct PATIENT from encounters_incremental)
and patients.id not in (select patients_incremental.id from patients_incremental)"""
)

df.write.mode('append').parquet(ingest_path+'/patients')

# COMMAND ----------


def update_enc_based_table(table):
  # Load in the historical encounter information
  spark.read.parquet(ingest_path+'encounters/').createOrReplaceTempView('encounters_incremental')

  # for initial load, you have to specify the schema because the ingest table is empty
  spark.read.parquet(landed_path+f'/{table}').createOrReplaceTempView(f'{table}')
  spark.read.schema(spark.table(f'{table}').schema).parquet(ingest_path+f'/{table}').createOrReplaceTempView(f'{table}_incremental')

  df = spark.sql(
  f"""select * from {table}
  where {table}.ENCOUNTER in (select distinct id from encounters_incremental)
  and {table}.ENCOUNTER not in (select distinct ENCOUNTER from {table}_incremental)
  """
  ).coalesce(1)

  df.write.mode('overwrite').parquet(ingest_path+f'/{table}')

  

for table in dbutils.fs.ls(landed_path):
  if table.name not in ['encounters/', 'patients/']:
    print(table.name)
    update_enc_based_table(table.name.replace('/',''))
