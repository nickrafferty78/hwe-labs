# Databricks notebook source
import os
import shutil
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

'''
Tests environment can run a spark app and load a dataframe
'''
def spark_df_test(spark: SparkSession):
    ## ARRANGE
    # Define the data
    data = [("Congratulations! You have successfully executed a Spark application written in Python!",)]

    # Define the schema for the DataFrame
    schema  = StructType([
        StructField("message", StringType(), True)
    ])

    ## ACT
    # Create a DataFrames
    df: DataFrame = spark.createDataFrame(data, schema)

    # Print the DataFrame
    df.show(truncate=False)
    ## ASSERT w/ visual inspection

'''
Tests environment can load a CSV file and write to a JSON file
'''
def spark_file_io_test(spark: SparkSession):
    ## ARRANGE - if we see evidence of already having been run, delete the contents for a clean test
    src_file_path: str = "file:/[replace with workspace path]"
    dest_folder_path: str = "file:////[replace with workspace path]"

    ## ACT
    df: DataFrame = spark.read.format("csv").option("header", "true").load(src_file_path)
    #df.write.mode("overwrite").format("json").save(dest_folder_path)
    try:
        folder_exists_and_has_files = len(dbutils.fs.ls(dest_folder_path)) > 0
    
        ## ASSERT - if folder exists and has files, calling it good enough
        if folder_exists_and_has_files:
            print("And even better yet, you can read from and write to storage!")
        else:
            # I'm not worthy (or your environment ain't ready yet)
            print("Unfortunately the file IO operations did not work as expected.\r")
    except Exception as e:
        print(f"Exception: {e}")

# COMMAND ----------

spark_df_test(spark)

# COMMAND ----------

spark_file_io_test(spark)

# COMMAND ----------


