# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Constructors Data
# MAGIC 1. Read bronze constructors table
# MAGIC 2. Keep only columns required for analytics (Drop url column)
# MAGIC 3. Standardize column names using snake_case (constructorId -> constructor_id)
# MAGIC 4. Rename columns to make them more meaningful (name -> constructor_name)
# MAGIC 5. Filter out rows where circuit_id is NULL (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns nationality to Title Case
# MAGIC 8. Write the transformed data to silver constructors table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read bronze constructors table

# COMMAND ----------

# circuits_df = spark.read.option('versionAsOf', 0).table(bronze_table) # reader API allows for chained fucntions like .option
constructors_df = spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2. Keep only thr required columns (drop url)

# COMMAND ----------

# circuits_select_df = circuits_df.select('circuitId',
#                    'circuitName',
#                    'lat',
#                    'long',
#                    'locality',
#                    'country',
#                    'ingestion_timestamp',
#                    'source_file')

# COMMAND ----------

constructors_select_df = constructors_df.drop(
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
constructors_renamed_df = constructors_select_df.withColumnsRenamed({"constructorId": "constructor_id",
                                                             "name": "constructor_name",}
                                                             
)
                                                             
                                        

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5.Filter rows where constructor_id is Null

# COMMAND ----------

# circuits_valid_df = circuits_renamed_df.filter(
#     "circuit_id IS NOT NULL"
# ) SQL Statement
constructors_valid_df = constructors_renamed_df.filter(F.col('constructor_id').isNotNull()) #Column expressions
display(constructors_valid_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 6. Remove duplicate records

# COMMAND ----------

#Distinct method
# circuits_distinct_df = circuits_valid_df.distinct()

#Drop duplicates method
constructors_distinct_df = constructors_valid_df.dropDuplicates(["constructor_id"])

display(constructors_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 7. Transform values of nationality to TitleCase

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
    .withColumn('nationality', F.initcap(F.col('nationality')))
    
    
)
display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 8. Write the transformed data to silver constructors table

# COMMAND ----------

write_to_silver(
    input_df=constructors_final_df,
    target_table=silver_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))