# Databricks notebook source
# MAGIC %md
# MAGIC #Build Races Fact table
# MAGIC 1. Read silver 'races' table
# MAGIC 2. Read silver 'sprints' table
# MAGIC 3. Add a new column session_type with values RACE or SPRINT
# MAGIC 4. UNION 'results' and 'sprints'
# MAGIC 5. Derive additional columns
# MAGIC     - is_win
# MAGIC     - is_podium
# MAGIC     - has_points
# MAGIC 6. Write the table to the gold layer

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read the silver tables

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
        .filter(F.col("batch_id") == v_batch_id)
        .withColumn("session_type", F.lit("RACE"))
        .drop("race_name", "race_date", "ingestion_timestamp", "source_file", "batch_id", "created_timestamp", "updated_timestamp")
)

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
        .filter(F.col("batch_id") == v_batch_id)
        .withColumn("session_type", F.lit("SPRINT"))
        .drop("race_name", "race_date", "ingestion_timestamp", "source_file", "batch_id", "created_timestamp", "updated_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2. UNION the tables

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3. Add derived columns

# COMMAND ----------

fact_session_results_df = (
    results_sprints_df
        .withColumn("is_win", F.col("finish_position") == 1)
        .withColumn("is_podium", F.col("finish_position").between(1,3))
        .withColumn("has_points", F.col("points") > 0)
)

# COMMAND ----------

display(fact_session_results_df.filter("season == 2025"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 6. Write the data

# COMMAND ----------

write_to_gold(
    input_df=fact_session_results_df,
    target_table=target_table,
    merge_condition="""
        t.season = s.season
        AND t.round = s.round
        AND t.constructor_id = s.constructor_id
        AND t.driver_id = s.driver_id
        AND t.session_type = s.session_type
    """,
    columns_to_update=[
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "finish_position",
        "finish_position_text",
        "status",
        "is_win",
        "is_podium",
        "has_points"
    ]
)