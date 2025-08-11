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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True
                                                  ),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True)])

# COMMAND ----------

circuits_df = spark.read\
.option("header", True)\
.option("inferSchema", True)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")


# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))



# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def add_ingestion_data(df):
    return df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

circuits_final_df = add_ingestion_data(circuits_renamed_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").option("path", "dbfs:/mnt/formula1dlsgen/processed/circuits").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").save(f"{processed_folder_path}/circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

