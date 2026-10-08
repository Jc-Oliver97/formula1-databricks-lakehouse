# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Constructors Data
# MAGIC 1. Read bronze Drivers table
# MAGIC 2. Keep only columns required for analytics (Drop url column)
# MAGIC 3. Standardize column names using snake_case (driverId -> driver_id, dateOfBirth -> date_of_birth)
# MAGIC 4. Concatenate name.givenname and name.familynave to create a column called given_name in Title Case
# MAGIC 5. Remove duplicates
# MAGIC 6. Transform values of columns nationality to Title Case
# MAGIC 7. Write the transformed data to silver constructors table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read bronze drivers table

# COMMAND ----------

# circuits_df = spark.read.option('versionAsOf', 0).table(bronze_table) # reader API allows for chained fucntions like .option
drivers_df = spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2. Keep only thr required columns (drop url)

# COMMAND ----------

from pyspark.sql import functions as F

drivers_select_df = drivers_df.drop(
    F.col('url')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 & 4. Standardize column names

# COMMAND ----------

# circuits_renamed_df = circuits_select_df.withColumnRenamed('circuitId', 'circuit_id') \
#                                         .withColumnRenamed('circuitName', 'circuit_name') \
#                                         .withColumnRenamed('lat', 'latitude') \
#                                         .withColumnRenamed('long', 'longitude')
drivers_renamed_df = drivers_select_df.withColumnsRenamed({"driverId": "driver_id",
                                                             "dateOfBirth": "date_of_birth"}
                                                             
)
                                                             
                                        

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5. Concatenate given and family name

# COMMAND ----------

drivers_concat_df = (
    drivers_renamed_df
        .withColumn("driver_name", F.concat_ws(" ", F.col('name.givenName'), F.col('name.familyName')))
        .drop('name')
)


# COMMAND ----------

# circuits_valid_df = circuits_renamed_df.filter(
#     "circuit_id IS NOT NULL"
# ) SQL Statement
drivers_valid_df = drivers_concat_df.filter(F.col('driver_id').isNotNull()) #Column expressions



# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 6. Remove duplicate records

# COMMAND ----------

drivers_distinct_df = drivers_valid_df.dropDuplicates(["driver_id"])


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 7. Transform values of nationality to TitleCase

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
    .withColumn('nationality', F.initcap(F.col('nationality')))
    .withColumn('driver_name', F.initcap(F.col('driver_name')))
    
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 8. Write the transformed data to silver constructors table

# COMMAND ----------

write_to_silver(
    input_df=drivers_final_df,
    target_table=silver_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_id",
        "driver_name",
        "date_of_birth",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))