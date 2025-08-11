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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", StringType(), True)
])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")


# COMMAND ----------

results_with_columns_df = results_df \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    return input_df.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)


# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md METHOD 1

# COMMAND ----------

#race_ids = [row["race_id"] for row in results_final_df.select("race_id").distinct().collect()]

# COMMAND ----------

#race_ids_str = ",".join([str(rid) for rid in race_ids])

# COMMAND ----------

#if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
    #spark.sql(f"DELETE FROM f1_processed.results WHERE race_id IN ({race_ids_str})")

# COMMAND ----------

#results_final_df.write \
    #.mode("append") \
    #.format("delta") \
    #.option("overwriteSchema", "true") \
    #.save("/mnt/formula1dlsgen/processed/results")


# COMMAND ----------

# MAGIC %md METHOD 2

# COMMAND ----------

#overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS f1_processed.results
    (
        result_id INT,
        race_id INT,
        driver_id INT,
        constructor_id INT,
        number INT,
        grid INT,
        position INT,
        position_text STRING,
        position_order INT,
        points FLOAT,
        laps INT,
        time STRING,
        milliseconds INT,
        fastest_lap INT,
        rank INT,
        fastest_lap_time STRING,
        fastest_lap_speed FLOAT,
        data_source STRING,
        file_date STRING,
        ingestion_date TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (race_id)
    LOCATION '/mnt/formula1dlsgen/processed/results'
""")


# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id"


merge_delta_data(
    input_df=results_deduped_df,
    db_name="f1_processed",
    table_name="results",
    folder_path=processed_folder_path,
    merge_condition=merge_condition,
    partition_column="race_id"
)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT file_date
# MAGIC FROM f1_processed.results
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC WHERE file_date = '2021-03-21';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_processed.results WHERE race_id = 540 AND driver_id = 229;