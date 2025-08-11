# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

def df_column_to_list(df, column_name):
    return [row[column_name] for row in df.select(column_name).distinct().collect()]


# COMMAND ----------

processed_folder_path = "/mnt/formula1dlsgen/processed"
from pyspark.sql.functions import col, current_timestamp


# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")
races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")


# COMMAND ----------

drivers_df = drivers_df \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")

constructors_df = constructors_df \
    .withColumnRenamed("name", "team")


# COMMAND ----------

final_df = results_df.alias("res") \
    .join(races_df.alias("rac"), col("res.race_id") == col("rac.race_id"), "inner") \
    .join(circuits_df.alias("cir"), col("rac.circuit_id") == col("cir.circuit_id"), "inner") \
    .join(drivers_df.alias("drv"), col("res.driver_id") == col("drv.driver_id"), "inner") \
    .join(constructors_df.alias("con"), col("res.constructor_id") == col("con.constructor_id"), "inner") \
    .select(
        col("res.race_id").alias("race_id"),
        col("rac.race_year"),
        col("rac.name").alias("race_name"),
        col("rac.race_timestamp").alias("race_date"),
        col("cir.location").alias("circuit_location"),
        col("drv.driver_name").alias("driver_name"),
        col("drv.driver_number").alias("driver_number"),
        col("drv.driver_nationality").alias("driver_nationality"),
        col("con.team").alias("team"),
        col("res.grid"),
        col("res.fastest_lap"),
        col("res.time").alias("time"),
        col("res.points"),
        col("res.position"),
        col("res.file_date").alias("file_date"),
        current_timestamp().alias("created_date")
    )


# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank
from pyspark.sql.window import Window

constructor_standings_df = final_df \
    .groupBy("race_year", "team") \
    .agg(
        sum("points").alias("total_points"),
        count(when(col("position") == 1, True)).alias("wins")
    )

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_constructor_df = constructor_standings_df \
    .withColumn("rank", rank().over(constructor_rank_spec))


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, current_timestamp

race_circuits_df = races_df \
    .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
    .select(
        races_df.race_id,
        races_df.name.alias("race_name"),
        races_df.race_year,
        races_df.race_timestamp.alias("race_date"),
        circuits_df.location.alias("circuit_location")
    )

race_results_df = results_df \
    .join(race_circuits_df, "race_id") \
    .join(drivers_df, "driver_id") \
    .join(constructors_df, "constructor_id")


# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"

merge_delta_data(
    final_constructor_df,
    'f1_presentation',
    'constructor_standings',
    presentation_folder_path,
    merge_condition,
    'race_year'
)


# COMMAND ----------

final_constructor_df.write.mode("overwrite").format("delta") \
    .partitionBy("race_year") \
    .option("mergeSchema", "true") \
    .saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings WHERE race_year = 2021;

# COMMAND ----------

