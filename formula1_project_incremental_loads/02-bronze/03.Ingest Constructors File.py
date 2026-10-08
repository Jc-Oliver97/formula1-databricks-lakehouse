# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructors.json file
# MAGIC 1. Read the file using spark dataframe reader API
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

source_file = f'{landing_folder_path}/{v_batch_id}/constructors.json'
table_name = f'{catalog_name}.{bronze_schema}.constructors'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read de JSON using the dataframe reader API

# COMMAND ----------

# Define the schema
constructors_schema = """constructorId STRING,
                         name STRING,
                         nationality STRING,
                          url STRING
                          """

# COMMAND ----------

constructors_df = (
    spark.read
    .format('json')
    .option('mode', 'FAILFAST')
    #.option('inferSchema', 'true')
    .schema(constructors_schema)
    .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add Metadata Columns
# MAGIC  - Source file
# MAGIC - Ingestion Timestamp

# COMMAND ----------

constructors_final_df = add_ingetion_metadata(constructors_df)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Write to bronze delta table

# COMMAND ----------

write_to_bronze(constructors_final_df, table_name, v_batch_id)

# COMMAND ----------

display(spark.table(table_name))