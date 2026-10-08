# Databricks notebook source
# MAGIC %md
# MAGIC #Build Races Dimension
# MAGIC 1. Read silver 'circuits' table
# MAGIC 2. Read silver 'races' table
# MAGIC 3. Join data from both tables with the 'circuit_id'
# MAGIC 4. Select the required columns
# MAGIC     - races.season
# MAGIC     - races.round
# MAGIC     - races.race_name
# MAGIC     - races.race_date
# MAGIC     - circuits.circuits_name
# MAGIC     - circuits.locality
# MAGIC     - circuits.country
# MAGIC 5. Write transformed data to gold 'dim_races' table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read races and circuits silver tables

# COMMAND ----------

circuits_df = spark.table(f"{catalog_name}.{silver_schema}.circuits").filter(F.col("batch_id") == v_batch_id)
races_df = spark.table(f"{catalog_name}.{silver_schema}.races").filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - 4. Join both tables on circuit_id

# COMMAND ----------

dim_races_df = (
    races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id)
    .select(races_df.season,
            races_df.round,
            races_df.race_name,
            races_df.race_date,
            circuits_df.circuit_name,
            circuits_df.locality,
            circuits_df.country)
)


# COMMAND ----------

display(dim_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5. Write to gold schema

# COMMAND ----------

write_to_gold(
    input_df=dim_races_df,
    target_table=target_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_name",
        "locality",
        "country"
    ]
)

# COMMAND ----------

display(spark.table(target_table))