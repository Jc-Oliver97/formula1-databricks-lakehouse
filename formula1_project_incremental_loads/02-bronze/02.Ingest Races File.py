# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Races.csv file
# MAGIC 1. Rwad the file using spark dataframe reader API
# MAGIC 2. Add Metadata columns
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC 3. Write to bronze delta table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f'{landing_folder_path}/{v_batch_id}/races.csv'
table_name = f'{catalog_name}.{bronze_schema}.races'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read de CSV using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

races_schema = StructType([
    StructField('season', IntegerType(), True),
    StructField('round', IntegerType(), True),
    StructField('url', StringType(), True),
    StructField('raceName', StringType(), True),
    StructField('date', DateType(), True),
    StructField('circuitId', StringType(), True)
])

# COMMAND ----------

races_df = (
    spark.read
    .format('csv')
    .option('header', 'true')
    .option('mode', 'FAILFAST')
    #.option('inferSchema', 'true')
    .schema(races_schema)
    .load(source_file)
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add Metadata Columns
# MAGIC  - Source file
# MAGIC - Ingestion Timestamp

# COMMAND ----------

races_final_df = add_ingetion_metadata(races_df)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Write to bronze delta table

# COMMAND ----------

write_to_bronze(races_final_df,table_name,v_batch_id)

# COMMAND ----------

display(spark.table(table_name))