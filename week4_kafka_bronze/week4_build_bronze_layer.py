# Databricks notebook source
# MAGIC %md
# MAGIC # Week 4 Lab

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import time
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week4Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use spark.readStream and spark.writeStream to perform a streaming query reading from a Delta Table
# MAGIC
# MAGIC

# COMMAND ----------

# Read data from the delta table using Spark Structured Streaming
df = spark \
    .readStream \
    .format("delta") \
    .table("nick_test_two")

# Kick off the process with a writestream- format as "memory", trigger each batch for 10 seconds
query = df.writeStream \
.format("memory").outputMode("append").queryName("nick_test_spark").trigger(processingTime = "10 seconds").start()

#Use spark.sql to query the structured streaming query and show that to the screen
for x in range(5):
  spark.sql("select * from nick_test_spark").show()
  time.sleep(10)
# Wait for the streaming query to finish
query.awaitTermination()

#Additional Challenge Problems:
#View the spark logs to understand the spark UI while the query is running
#Use sparklistener to add additional logging to the stream
#Perform additional spark.sql queries on top of the streaming data

# COMMAND ----------


