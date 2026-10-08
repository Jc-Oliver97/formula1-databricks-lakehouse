-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Build Driver Standings
-- MAGIC  
-- MAGIC ###Sources
-- MAGIC 1. fact_session_results
-- MAGIC 2. dim_drivers
-- MAGIC
-- MAGIC ### Output Columns
-- MAGIC 1. season
-- MAGIC 2. driver_id
-- MAGIC 3. driver_name
-- MAGIC 4. nationality
-- MAGIC 5. race_starts
-- MAGIC 6. number_of_wins
-- MAGIC 7. total_points
-- MAGIC 8. number_of_podiums
-- MAGIC 9. standing_position 

-- COMMAND ----------

CREATE OR REPLACE VIEW formula1.gold.vw_driver_standings
AS
WITH driver_session_results
AS 
(SELECT r.season, d.driver_id, d.driver_name, d.nationality,
       COUNT(*) AS race_starts,
       SUM(r.points) AS total_points,
       COUNT_IF(r.is_win) AS number_of_wins,
       COUNT_IF(r.is_podium) AS number_of_podiums
FROM formula1_incr.gold.fact_session_results r
JOIN formula1.gold.dim_drivers d
ON r.driver_id = d.driver_id
GROUP BY r.season, d.driver_id, d.driver_name, d.nationality)

SELECT season, driver_id, driver_name, nationality,
       race_starts, total_points, number_of_wins, number_of_podiums,
       RANK() OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing
FROM driver_session_results

-- COMMAND ----------

SELECT *
FROM formula1.gold.vw_driver_standings
WHERE season = 2023;
