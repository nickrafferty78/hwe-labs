# Databricks notebook source
# MAGIC %md
# MAGIC # Week 5 Lab
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write a streaming pipeline that reads from reviews delta table - similar to week 4

# COMMAND ----------

# Create a SparkSession

#Create a schema

#Read from reviews table with spark structured streaming

#Create a temporary view 

#With another dataframe, use spark to read the customers tsv from AWS S3

#Join the two datasets together using spark.sql and store in a silver_df dataframe

#Use spark structured streaming to kick off a writeStream query and write parquet files to the silver layer location

# COMMAND ----------

# Additional challenge problems:
#How would you transform data during this stream into the silver layer - pick a column and perform a streaming transformation on it
#Do any columns need to be flattened? If they did, what would that look like? It's a very common practice to have a nested map in the raw data that needs to be flattened out into distinct columns in the silver layer. What steps would you take to do that? 
