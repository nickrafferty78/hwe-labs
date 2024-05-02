# Databricks notebook source
# MAGIC %md
# MAGIC # Week 4 Lab

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import time
# Create a SparkSession using Spark structured streaming

# COMMAND ----------

# MAGIC %md
# MAGIC Use spark.readStream and spark.writeStream to perform a streaming query reading from a Delta Table

# COMMAND ----------

# Read data from the delta table using Spark Structured Streaming

# Kick off the process with a writestream- format as "memory", trigger each batch for 10 seconds

#Use spark.sql to query the structured streaming query and show that to the screen

# Wait for the streaming query to finish



# COMMAND ----------

#Additional Challenge Problems:
#View the spark logs to understand the spark UI while the query is running

#Use sparklistener to add additional logging to the stream

#Perform additional spark.sql queries on top of the streaming data

