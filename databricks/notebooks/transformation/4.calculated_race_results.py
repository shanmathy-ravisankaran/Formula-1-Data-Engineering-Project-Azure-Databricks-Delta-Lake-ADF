# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS f1_processed.drivers_staging
    (
        race_year INT,
        team_name STRING,
        driver_id INT,
        driver_name STRING,
        race_id INT,
        position INT, 
        points INT,
        calculated_points INT,
        created_date TIMESTAMP,
        updated_date TIMESTAMP
    )
    USING DELTA
""")


# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW race_result_updated AS
    SELECT
        races.race_year,
        constructors.name AS team_name,
        drivers.driver_id,
        drivers.name AS driver_name,
        results.race_id,
        results.position,
        results.points,
        11 - results.position AS calculated_points
    FROM f1_processed.results AS results
    JOIN f1_processed.drivers AS drivers ON results.driver_id = drivers.driver_id
    JOIN f1_processed.constructors AS constructors ON results.constructor_id = constructors.constructor_id
    JOIN f1_processed.races AS races ON results.race_id = races.race_id
    WHERE results.position <= 10
    AND results.file_date = '{v_file_date}'
""")


# COMMAND ----------

spark.sql(f""" 
MERGE INTO f1_presentation.calculated_race_results tgt
USING race_result_updated upd
ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
WHEN MATCHED THEN
  UPDATE SET
    tgt.position = upd.position,
    tgt.points = upd.points,
    tgt.calculated_points = upd.calculated_points,
    tgt.update_Date = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    race_year, team_name, driver_id, driver_name, race_id,
    position, points, calculated_points, createdDate
  )
  VALUES (
    upd.race_year, upd.team_name, upd.driver_id, upd.driver_name, upd.race_id,
    upd.position, upd.points, upd.calculated_points, current_timestamp
  )
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM race_results_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results;

# COMMAND ----------

