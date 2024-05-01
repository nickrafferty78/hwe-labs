# Databricks notebook source
# MAGIC %md
# MAGIC # Week 3 Lab

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col
from pyspark.sql.functions import current_timestamp


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week3Lab") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe.
# MAGIC You will use the "reviews" dataframe defined here to answer all the questions below...

# COMMAND ----------

#Question 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 2: Display the schema of the dataframe.

# COMMAND ----------

#Question 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 3: How many records are in the dataframe? 
# MAGIC Store this number in a variable named "reviews_count".

# COMMAND ----------

#Question 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 4: Print the first 5 rows of the dataframe. 
# MAGIC Some of the columns are long - print the entire record, regardless of length.

# COMMAND ----------

#Question 4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 5: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# MAGIC Look at the first 50 rows of that dataframe. 
# MAGIC Which value appears to be the most common?

# COMMAND ----------

#Question 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 6: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# MAGIC What is the product title for that review? How many helpful votes did it have?

# COMMAND ----------

#Question 6

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 7: How many reviews have a 5 star rating?

# COMMAND ----------

#Question 7

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 8: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# MAGIC Create a new dataframe with just those 3 columns, except cast them as "int"s.
# MAGIC Look at 10 rows from this dataframe.

# COMMAND ----------

#Question 8 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 9: Find the date with the most purchases.
# MAGIC Print the date and total count of the date with the most purchases

# COMMAND ----------

#Question 9

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 10: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
# MAGIC Hint: Check the documentation for a function that can help: https://spark.apache.org/docs/3.1.3/api/python/reference/pyspark.sql.html#functions
# MAGIC Print the schema and inspect a few rows of data to make sure the data is correctly populated.

# COMMAND ----------

#Question 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 11: Write the dataframe with load timestamp to s3a://hwe-$CLASS/$HANDLE/bronze/reviews_static in Parquet format.
# MAGIC Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...

# COMMAND ----------

#Question 11

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 12: Read the tab separated file named "resources/customers.tsv.gz" into a dataframe
# MAGIC Write to S3 under s3a://hwe-$CLASS/$HANDLE/bronze/customers
# MAGIC Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
# MAGIC There are no questions to answer about this data set right now, but you will use it in a later lab...

# COMMAND ----------

#Question 12

# COMMAND ----------

# Stop the SparkSession
spark.stop()
