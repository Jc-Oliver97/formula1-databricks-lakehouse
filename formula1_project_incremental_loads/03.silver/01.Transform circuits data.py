# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Circuits Data
# MAGIC 1. Read bronze circuits table
# MAGIC 2. Keep only columns required for analytics (Drop url column)
# MAGIC 3. Standardize column names using snake_case (circuitID -> circuit_id, circuitName -> circuit_name)
# MAGIC 4. Rename columns to make them more meaningful (lat -> latitude, long -> longitude)
# MAGIC 5. Filter out rows where circuit_id is NULL (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns circuit_name and locality to Title Case
# MAGIC 8. Write the transformed data to silver circuits table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read bronze circuits table

# COMMAND ----------

# circuits_df = spark.read.option('versionAsOf', 0).table(bronze_table) # reader API allows for chained fucntions like .option
circuits_df = (
    spark.table(bronze_table).filter((F.col("batch_id") == v_batch_id))
)

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

circuits_select_df = circuits_df.select(
    F.col('circuitId'),
    F.col('circuitName'),
    F.col('lat'),
    F.col('long'),
    F.col('locality'),
    F.col('country'),
    F.col('ingestion_timestamp'),
    F.col('source_file'),
    F.col('batch_id')
)

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 & 4. Standardize column names

# COMMAND ----------

# circuits_renamed_df = circuits_select_df.withColumnRenamed('circuitId', 'circuit_id') \
#                                         .withColumnRenamed('circuitName', 'circuit_name') \
#                                         .withColumnRenamed('lat', 'latitude') \
#                                         .withColumnRenamed('long', 'longitude')
circuits_renamed_df = circuits_select_df.withColumnsRenamed({"circuitId": "circuit_id",
                                                             "circuitName": "circuit_name",
                                                             "lat": "latitude",
                                                             "long": "longitude"})
                                        

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5.Filter rows where circuit_id is Null

# COMMAND ----------

# circuits_valid_df = circuits_renamed_df.filter(
#     "circuit_id IS NOT NULL"
# ) SQL Statement
circuits_valid_df = circuits_renamed_df.filter(F.col('circuit_id').isNotNull()) #Column expressions
display(circuits_valid_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 6. Remove duplicate records

# COMMAND ----------

#Distinct method
# circuits_distinct_df = circuits_valid_df.distinct()

#Drop duplicates method
circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

display(circuits_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 7. Transform values of circuit_name and locality to TitleCase

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
    .withColumn('circuit_name', F.initcap(F.col('circuit_name')))
    .withColumn('locality', F.initcap(F.col('locality')))
    
)
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 8. Write the transformed data to silver circuits table

# COMMAND ----------

# circuits_final_df = (
#     circuits_final_df
#     .withColumn('created_timestamp', F.current_timestamp())
#     .withColumn('updated_timestamp', F.current_timestamp()) )

# COMMAND ----------

# from delta.tables import DeltaTable

# if not spark.catalog.tableExists(silver_table):
#     (
#         circuits_final_df
#             .write
#             .format('delta')
#             .mode('overwrite')
#             .saveAsTable(silver_table)
#     )
# else:

#     delta_table = DeltaTable.forName(spark, silver_table)
#     (
#         delta_table.alias("t")
#         .merge(
#             circuits_final_df.alias("s"),
#             "t.circuit_id = s.circuit_id"
#         )
#         .whenMatchedUpdate(
#             condition = "s.batch_id" >= "t.batch_id",
#             set = {
#                     "circuit_name": "s.circuit_name",
#                     "latitude": "s.latitude",
#                     "longitude": "s.longitude",
#                     "locality": "s.locality",
#                     "country": "s.country",
#                     "ingestion_timestamp": "s.ingestion_timestamp",
#                     "source_file": "s.source_file",
#                     "updated_timestamp": "s.updated_timestamp"
#             }
#         )
#         .whenNotMatchedInsertAll()
#         .execute()
#     )

# COMMAND ----------

write_to_silver(
    input_df=circuits_final_df,
    target_table=silver_table,
    merge_condition="t.circuit_id = s.circuit_id",
    columns_to_update=[
        "circuit_name",
        "latitude",
        "longitude",
        "locality",
        "country",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))