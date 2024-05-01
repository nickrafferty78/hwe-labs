# Databricks notebook source
# MAGIC %md
# MAGIC # Welcome to your first Spark Program
# MAGIC
# MAGIC Let's make sure everything works correctly. Please run the cell below and verify it runs as expected. You should see a congratulations message if it is running successfully. 

# COMMAND ----------

import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

# Create a SparkSession
spark = SparkSession.builder.appName("SparkInstallationTest").getOrCreate()

# Define the data
data = [("Congratulations! The second Spark program is executing!",),("(This program will run until you terminate it...)",)]

# Define the schema for the DataFrame
#schema = StringType()
schema  = StructType([
    StructField("message", StringType(), True)
])

# Create a DataFrames
df = spark.createDataFrame(data, schema)

# Print the DataFrame
df.show(truncate=False)

time.sleep(9999)
# Stop the SparkSession
spark.stop()
