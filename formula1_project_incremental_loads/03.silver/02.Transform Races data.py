# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Races Data
# MAGIC 1. Read bronze races table
# MAGIC 2. Keep only columns required for analytics (Drop url column)
# MAGIC 3. Standardize column names using snake_case (circuitId -> circuit_id, raceName -> race_name)
# MAGIC 4. Rename columns to make them more meaningful (date -> race_date)
# MAGIC 5. Filter out rows where circuit_id is NULL (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns race_name to Title Case
# MAGIC 8. Write the transformed data to silver circuits table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read bronze races table

# COMMAND ----------

# circuits_df = spark.read.option('versionAsOf', 0).table(bronze_table) # reader API allows for chained fucntions like .option
races_df = spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2. Keep only thr required columns (drop url)

# COMMAND ----------

from pyspark.sql import functions as F

races_select_df = races_df.select(
    F.col('season'),
    F.col('round'),
    F.col('raceName'),
    F.col('date'),
    F.col('circuitId'),
    F.col('ingestion_timestamp'),
    F.col('source_file'),
    F.col('batch_id')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 & 4. Standardize column names

# COMMAND ----------

# circuits_renamed_df = circuits_select_df.withColumnRenamed('circuitId', 'circuit_id') \
#                                         .withColumnRenamed('circuitName', 'circuit_name') \
#                                         .withColumnRenamed('lat', 'latitude') \
#                                         .withColumnRenamed('long', 'longitude')
races_renamed_df = races_select_df.withColumnsRenamed({"circuitId": "circuit_id",
                                                             "circuitName": "circuit_name",
                                                             "raceName": "race_name",
                                                             "date": "race_date"}
)
                                                             
                                        

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5.Filter rows where circuit_id is Null

# COMMAND ----------

# circuits_valid_df = circuits_renamed_df.filter(
#     "circuit_id IS NOT NULL"
# ) SQL Statement
races_valid_df = races_renamed_df.filter(F.col('circuit_id').isNotNull()) #Column expressions
display(races_valid_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 6. Remove duplicate records

# COMMAND ----------

#Distinct method
# circuits_distinct_df = circuits_valid_df.distinct()

#Drop duplicates method
races_distinct_df = races_valid_df.dropDuplicates(["season", "round"])

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 7. Transform values of race_name to TitleCase

# COMMAND ----------

races_final_df = (
    races_distinct_df
    .withColumn('race_name', F.initcap(F.col('race_name')))
    
    
)
display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 8. Write the transformed data to silver races table

# COMMAND ----------

write_to_silver(
    input_df=races_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_id",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))