# Databricks notebook source
# MAGIC %md
# MAGIC #Setup for project environment for Formula1 Project
# MAGIC 1. Create external location databricks-course-ext-dl-formula1-incr
# MAGIC 2. Create Catalog formula1_incr
# MAGIC 3. Create schema landing, bronze, silver, gold
# MAGIC 4. Create Volumes for landing schema
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Access Storage

# COMMAND ----------

# MAGIC %fs ls 'abfss://formula1-incr@databrickcourseextdl.dfs.core.windows.net'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##External location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl_formula1_incr
# MAGIC URL 'abfss://formula1-incr@databrickcourseextdl.dfs.core.windows.net'
# MAGIC WITH (STORAGE CREDENTIAL `databricks-course-sc`)
# MAGIC COMMENT 'External location for databricks course formula1_incr';

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Unity Catalog - formula1

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS formula1_incr
# MAGIC MANAGED LOCATION 'abfss://formula1-incr@databrickcourseextdl.dfs.core.windows.net'
# MAGIC COMMENT 'This is the main catalog for the formula1 project';

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create the Schemas landing, bronze, silver, gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze
# MAGIC MANAGED LOCATION 'abfss://formula1-incr@databrickcourseextdl.dfs.core.windows.net/bronze';
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
# MAGIC MANAGED LOCATION 'abfss://formula1-incr@databrickcourseextdl.dfs.core.windows.net/silver';
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
# MAGIC MANAGED LOCATION 'abfss://formula1-incr@databrickcourseextdl.dfs.core.windows.net/gold';
# MAGIC
# MAGIC SHOW SCHEMAS IN formula1_incr;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME formula1_incr.landing.files
# MAGIC LOCATION 'abfss://formula1-incr@databrickcourseextdl.dfs.core.windows.net/landing';
# MAGIC

# COMMAND ----------

# MAGIC %fs ls /Volumes/formula1_incr/landing/files