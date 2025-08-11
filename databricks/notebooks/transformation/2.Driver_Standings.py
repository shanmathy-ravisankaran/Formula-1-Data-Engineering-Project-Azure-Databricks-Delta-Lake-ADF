# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC  %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, sum as _sum, count, when
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc


# COMMAND ----------

presentation_folder_path = "/mnt/formula1dlsgen/presentation"
processed_folder_path = "/mnt/formula1dlsgen/processed"

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter("race_date >= '2021-01-01' AND race_date <= '2021-12-31'") \
    .select("race_year").distinct().collect()

# COMMAND ----------

race_year_list = [row.race_year for row in race_results_list]

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")
results_df_raw = spark.read.format("delta").load(f"{processed_folder_path}/results")
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team") \
    .withColumnRenamed("nationality", "team_nationality")


# COMMAND ----------

results_df = results_df_raw.alias("res") \
    .join(races_df.alias("rac"), col("res.race_id") == col("rac.race_id")) \
    .filter(col("rac.race_year").isin(race_year_list)) \
    .select("res.*", "rac.race_year")

# COMMAND ----------

race_results_df = results_df.alias("res") \
    .join(races_df.alias("rac"), col("res.race_id") == col("rac.race_id"), "inner") \
    .join(circuits_df.alias("cir"), col("rac.circuit_id") == col("cir.circuit_id"), "inner") \
    .join(drivers_df.alias("drv"), col("res.driver_id") == col("drv.driver_id"), "inner") \
    .join(constructors_df.alias("con"), col("res.constructor_id") == col("con.constructor_id"), "inner")

# COMMAND ----------

final_df = race_results_df.select(
    col("res.race_id").alias("race_id"),
    col("rac.race_year"),
    col("rac.name").alias("race_name"),
    col("rac.race_timestamp").alias("race_date"),
    col("cir.location").alias("circuit_location"),
    col("drv.driver_name"),
    col("drv.driver_number"),
    col("drv.driver_nationality"),
    col("con.team"),
    col("res.grid"),
    col("res.fastest_lap"),
    col("res.time"),
    col("res.points"),
    col("res.position"),
    col("res.file_date"),
    current_timestamp().alias("created_date")
)

# COMMAND ----------

driver_standings_df = final_df.groupBy("race_year", "driver_name") \
    .agg(
        _sum("points").alias("total_points"),
        count(when(col("position") == 1, True)).alias("wins")
    )

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_standings_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))


# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year" 

merge_delta_data(
    final_standings_df,
    db_name="f1_presentation",      
    table_name="driver_standings",    
    folder_path=processed_folder_path, 
    merge_condition=merge_condition,
    partition_column="race_year"
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("race_year") \
    .orderBy(desc("total_points"), desc("wins"))

driver_standings_df_with_rank = driver_standings_df.withColumn(
    "rank", rank().over(driver_rank_spec)
)


# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"

merge_delta_data(
    driver_standings_df_with_rank,
    "f1_presentation",
    "driver_standings",
    presentation_folder_path + "/driver_standings",
    merge_condition,
    "race_year"
)


# COMMAND ----------

overwrite_partition(final_standings_df, "f1_presentation", "driver_standings_summary", "race_year")


# COMMAND ----------

