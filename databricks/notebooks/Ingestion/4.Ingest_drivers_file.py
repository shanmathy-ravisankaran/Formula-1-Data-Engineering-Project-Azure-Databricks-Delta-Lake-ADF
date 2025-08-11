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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,  DataType

# COMMAND ----------

name_schema = StructType(fields=[
  StructField("forename", StringType(), True),
  StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[
  StructField("driverId", IntegerType(), False),
  StructField("driverRef", StringType(), True),
  StructField("number", IntegerType(), True),
  StructField("code", StringType(), True),
  StructField("name", name_schema),
  StructField("dob", StringType(), True),
  StructField("nationality", StringType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("driverRef", "driver_ref") \
        .withColumn("ingestion_date", current_timestamp()) \
            .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/formula1dlsgen/processed/drivers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed.drivers
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dlsgen/processed/drivers';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")