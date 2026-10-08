# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Drivers.json file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Define and enforce schema (preserve nested structure)
# MAGIC 3. Add Metadata columns
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

source_file = f'{landing_folder_path}/{v_batch_id}/drivers.json'
table_name = f'{catalog_name}.{bronze_schema}.drivers'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read de JSON using the dataframe reader API

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType, StructField, StringType, DateType

name_schema = StructType([
    StructField('givenName', StringType()),
    StructField("familyName", StringType())
])
driver_schema = StructType([
    StructField('driverId', StringType()),
    StructField('name', name_schema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())
])


# COMMAND ----------

drivers_df = (
    spark.read
    .format('json')
    .option('mode', 'FAILFAST')
    #.option('inferSchema', 'true')
    .schema(driver_schema)
    .load(source_file)
)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add Metadata Columns
# MAGIC  - Source file
# MAGIC - Ingestion Timestamp

# COMMAND ----------

drivers_final_df = add_ingetion_metadata(drivers_df)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Write to bronze delta table

# COMMAND ----------

write_to_bronze(drivers_final_df, table_name, v_batch_id)

# COMMAND ----------

display(spark.table(table_name))