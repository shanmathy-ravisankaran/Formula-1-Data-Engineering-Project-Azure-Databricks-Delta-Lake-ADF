# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

def re_Arrange_partition_column(input_df, partition_column):
    column_list = []

    for column_name in results_final_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)

    column_list.append(partition_column)

    output_df = results_final_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        input_df.write \
            .mode("overwrite") \
            .insertInto(f"{db_name}.{table_name}")
    else:
        input_df.write \
            .mode("overwrite") \
            .format("delta") \
            .partitionBy(partition_column) \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    full_table_name = f"{db_name}.{table_name}"

    if spark._jsparkSession.catalog().tableExists(full_table_name):
        deltaTable = DeltaTable.forName(spark, full_table_name)
        
        # Dynamically build SET clause from DataFrame columns
        update_set = {col: f"src.{col}" for col in input_df.columns}

        deltaTable.alias("tgt") \
            .merge(input_df.alias("src"), merge_condition) \
            .whenMatchedUpdate(set=update_set) \
            .whenNotMatchedInsert(values=update_set) \
            .execute()
    else:
        input_df.write.mode("overwrite") \
            .partitionBy(partition_column) \
            .format("delta") \
            .saveAsTable(full_table_name)


# COMMAND ----------

