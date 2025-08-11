# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")


# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

from pyspark.sql.functions import col

race_circuits_df = races_df.alias("races").join(
    circuits_df.alias("circuits"),
    col("races.circuit_id") == col("circuits.circuit_id"),
    "inner"
).select(
    col("races.race_id"),
    col("races.race_year"),
    col("races.race_name"),
    col("races.race_date"),
    col("circuits.location").alias("circuit_location") 
)



# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = results_df.alias("res") \
    .join(race_circuits_df.alias("rac"), col("res.race_id") == col("rac.race_id")) \
    .join(drivers_df.alias("drv"), col("res.driver_id") == col("drv.driver_id")) \
    .join(constructors_df.alias("con"), col("res.constructor_id") == col("con.constructor_id"))

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")

# COMMAND ----------

race_results_df = results_df.alias("res") \
    .join(race_circuits_df.alias("rac"), col("res.race_id") == col("rac.race_id")) \
    .join(drivers_df.alias("drv"), col("res.driver_id") == col("drv.driver_id")) \
    .join(constructors_df.alias("con"), col("res.constructor_id") == col("con.constructor_id"))

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

final_df = race_results_df.select(
    col("res.race_id").alias("race_id"),
    col("rac.race_year"),
    col("rac.race_name"),
    col("rac.race_date"),
    col("rac.circuit_location"),
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

final_df_cleaned = final_df.withColumn("race_id", col("race_id"))

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name"
merge_delta_data(final_df, 'f1_presentation', 'qualifying', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.race_results;