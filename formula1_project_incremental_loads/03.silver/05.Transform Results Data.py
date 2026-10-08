# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Results Data
# MAGIC 1. Read bronze results table
# MAGIC 2. Keep only columns required for analytics (Drop url column)
# MAGIC 3. Standardize column names using snake_case (constructorId -> constructor_id, raceName -> race_name, driverId -> driver_id, positionText -> finish_position_text)
# MAGIC 4. Rename columns to make them more meaningful (date -> race_date, grid -> grid_position, laps -> completed_laps, number -> car_number, position -> finish_position)
# MAGIC 5. Filter out rows where season, round, counstructor_id or driver_id is NULL (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of column race_name to Title Case
# MAGIC 8. Write the transformed data to silver results table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read bronze drivers table

# COMMAND ----------

# circuits_df = spark.read.option('versionAsOf', 0).table(bronze_table) # reader API allows for chained fucntions like .option
results_df = spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2. Keep only thr required columns (drop url)

# COMMAND ----------

results_select_df = results_df.select("season",
           "round",
            "constructorId",
            "driverId",
            "date",
            "raceName",
            "grid",
            "laps",
            "number",
            "points",
            "position",
            "positionText",
            "status",
            "ingestion_timestamp",
            "source_file",
            "batch_id")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 & 4. Standardize column names

# COMMAND ----------

results_renamed_df = results_select_df.withColumnsRenamed({"driverId": "driver_id",
                                                             "raceName": "race_name",
                                                             "constructorId": "constructor_id",
                                                             "positionText": "finish_position_text",
                                                             "grid": "grid_position",
                                                             "date": "race_date",
                                                             "laps": "completed_laps",
                                                             "number": "car_number",
                                                             "position": "finish_position"}
                                                             
)
                                                             
                                        

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5. Filter out Null values

# COMMAND ----------

results_valid_df = (
    results_renamed_df
        .filter(
            F.col('driver_id').isNotNull() &
            F.col('season').isNotNull() &
            F.col('round').isNotNull() &
            F.col('constructor_id').isNotNull()
        )
)
                    
                      
display(results_renamed_df.count() - results_valid_df.count())                                 


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 6. Remove duplicate records

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season", "round", "constructor_id", "driver_id"])

display(results_valid_df.count() - results_distinct_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 7. Transform values of nationality to TitleCase

# COMMAND ----------

results_final_df = (
    results_distinct_df
    .withColumn('race_name', F.initcap(F.col('race_name')))

    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 8. Write the transformed data to silver constructors table

# COMMAND ----------

write_to_silver(
    input_df=results_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
    columns_to_update=[
        "race_date",
        "race_name",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "finish_position",
        "finish_position_text",
        "status",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))