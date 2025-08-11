# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType


# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                   StructField("year", IntegerType(), True),
                   StructField("round", IntegerType(), True),
                   StructField("circuitId", IntegerType(), True),
                   StructField("name", StringType(), True),
                   StructField("date", StringType(), True),
                   StructField("time", StringType(), True),
                   StructField("url", StringType(), True)                
                  ])        

# COMMAND ----------

raw_folder_path = "/mnt/formula1dlsgen/raw"

# COMMAND ----------

races_df = spark.read\
.option("header", True)\
.schema(races_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)


# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('race_timestamp'))

# COMMAND ----------

races_selected_df.write.mode("overwrite").option("overwriteSchema", "true").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")