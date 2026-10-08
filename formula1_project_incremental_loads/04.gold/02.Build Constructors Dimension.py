# Databricks notebook source
# MAGIC %md
# MAGIC #Create Constructors Dimension
# MAGIC 1. Read silver 'constructors' table
# MAGIC 2. Read gold 'ref_nationality_region' table
# MAGIC 3. Join data from tables on nationality
# MAGIC 4. Select required columns
# MAGIC     - constructors.constructor_id
# MAGIC     - constructors.constructor_name
# MAGIC     - constructors.nationality
# MAGIC     - ref_nationality_region.region
# MAGIC 5. Write transformed data to gold 'dim_constructors' table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read the tables

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors").filter(F.col("batch_id") == v_batch_id)
region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Steps 2 - 4. Join and select required columns

# COMMAND ----------

dim_constructors_df = (
    constructors_df
    .join(region_df, constructors_df.nationality == region_df.nationality, 'left')
    .select(constructors_df.constructor_id,
            constructors_df.constructor_name,
            constructors_df.nationality,
            region_df.region.alias("nationality_region"))
)

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5. Write the transformed table

# COMMAND ----------

write_to_gold(
    input_df=dim_constructors_df,
    target_table=target_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "nationality_region"
    ]
)

# COMMAND ----------

display(spark.table(target_table))