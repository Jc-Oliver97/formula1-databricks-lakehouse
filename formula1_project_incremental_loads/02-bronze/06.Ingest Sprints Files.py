# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructors.json file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Define and enforce schema
# MAGIC 3. Add Metadata columns
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC 4. Write to bronze delta table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

for f in dbutils.fs.ls(f"{landing_folder_path}/{v_batch_id}"):
    if f.name.startswith("sprints_"):
        dbutils.fs.mv(f.path, f"{landing_folder_path}/{v_batch_id}/sprints/{f.name}")

# COMMAND ----------

source_file = f'{landing_folder_path}/{v_batch_id}/sprints'
table_name = f'{catalog_name}.{bronze_schema}.sprints'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read de JSON using the dataframe reader API

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
sprints_schema = StructType([
    StructField('date', DateType()),
    StructField('raceName', StringType()),
    StructField('round', IntegerType()),
    StructField('season', IntegerType()),
    StructField('url', StringType()),
    StructField('constructorId', StringType()),
    StructField('driverId', StringType()),
    StructField('grid', IntegerType()),
    StructField('laps', IntegerType()),
    StructField('number', IntegerType()),
    StructField('points', DoubleType()),
    StructField('position', IntegerType()),
    StructField('positionText', StringType()),
    StructField('status', StringType())

    
])

# COMMAND ----------

sprints_df = (
    spark.read
    .format('json')
    .option('mode', 'FAILFAST')
    .option('multiLine', True)
    .schema(sprints_schema)
    .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add Metadata Columns
# MAGIC  - Source file
# MAGIC - Ingestion Timestamp

# COMMAND ----------

sprints_final_df = add_ingetion_metadata(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Write to bronze delta table

# COMMAND ----------

write_to_bronze(sprints_final_df, table_name, v_batch_id)

# COMMAND ----------

display(spark.table(table_name))