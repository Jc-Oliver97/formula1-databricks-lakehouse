# Databricks notebook source
# MAGIC %md
# MAGIC #Create Drivers Dimension
# MAGIC 1. Read silver 'drivers' table
# MAGIC 2. Read gold 'ref_nationality_region' table
# MAGIC 3. Join data from tables on nationality
# MAGIC 4. Select required columns
# MAGIC     - drivers.driver_id
# MAGIC     - drivers.driver_name
# MAGIC     - drivers.date_of_birth
# MAGIC     - drivers.nationality
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

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read the tables

# COMMAND ----------

drivers_df = spark.table(f"{catalog_name}.{silver_schema}.drivers").filter(F.col("batch_id") == v_batch_id)
region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Steps 2 - 4. Join and select required columns

# COMMAND ----------

dim_drivers_df = (
    drivers_df
    .join(region_df, drivers_df.nationality == region_df.nationality, 'left')
    .select(drivers_df.driver_id,
            drivers_df.driver_name,
            drivers_df.date_of_birth,
            drivers_df.nationality,
            region_df.region.alias("nationality_region"))
)

# COMMAND ----------

display(dim_drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5. Write the transformed table

# COMMAND ----------

write_to_gold(
    input_df=dim_drivers_df,
    target_table=target_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_name",
        "date_of_birth",
        "nationality",
        "nationality_region"
    ]
)

# COMMAND ----------

display(spark.table(target_table))