# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])


# COMMAND ----------

v_file_date = "2021-03-21"

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"/mnt/formula1dlsgen/raw/{v_file_date}/lap_times")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_df \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

final_df.write.mode("overwrite").format("delta").save("/mnt/formula1dlsgen/processed/lap_times")


# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"

merge_delta_data(
    input_df=final_df,
    db_name="f1_processed",
    table_name="lap_times",
    folder_path=processed_folder_path,
    merge_condition=merge_condition,
    partition_column="race_id"
)

# COMMAND ----------

dbutils.notebook.exit("Success")