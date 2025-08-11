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
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

v_file_date = "2021-03-21"
pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiLine", True) \
    .json(f"/mnt/formula1dlsgen/raw/{v_file_date}/pit_stops.json")

# COMMAND ----------

final_df = pit_stops_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .select("race_id", "driver_id", "stop", "lap", "time", "duration", "milliseconds", "ingestion_date")


# COMMAND ----------

final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("/mnt/formula1dlsgen/processed/pit_stops")


# COMMAND ----------

final_df = pit_stops_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("stop", "stop") \
    .withColumnRenamed("lap", "lap") \
    .withColumnRenamed("time", "time") \
    .withColumnRenamed("duration", "duration") \
    .withColumnRenamed("milliseconds", "milliseconds") \
    .withColumn("ingestion_date", current_timestamp()) \
    .selectExpr("driver_id as driverId", "duration", "lap", "milliseconds", "race_id as raceId", "stop", "time", "ingestion_date")


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS f1_raw.pit_stops")
spark.sql("""
    CREATE TABLE f1_raw.pit_stops
    USING PARQUET
    LOCATION '/mnt/formula1dlsgen/processed/pit_stops'
""")

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    full_table_name = f"{db_name}.{table_name}"

    if spark._jsparkSession.catalog().tableExists(full_table_name):
        deltaTable = DeltaTable.forName(spark, full_table_name)
        deltaTable.alias("tgt") \
            .merge(input_df.alias("src"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        input_df.write.mode("overwrite") \
            .partitionBy(partition_column) \
            .format("delta") \
            .saveAsTable(full_table_name)

# COMMAND ----------

processed_folder_path = "/mnt/formuladlsgen/processed"
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"

# COMMAND ----------

final_df = final_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id")


# COMMAND ----------

merge_delta_data(
    input_df=final_df,
    db_name="f1_processed",
    table_name="pit_stops",
    folder_path=processed_folder_path,
    merge_condition=merge_condition,
    partition_column="race_id"
)


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

