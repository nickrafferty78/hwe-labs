# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup: Create a SparkSession

# COMMAND ----------

from pyspark.sql import SparkSession


# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
# MAGIC Make sure to add file: to the start of your path. Feel free to read more here: https://docs.databricks.com/en/files/index.html Specifically, workspace files section

# COMMAND ----------

#Question 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.

# COMMAND ----------

#Question 2


# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer.

# COMMAND ----------

# Question 3


# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 4: How many records are in the reviews dataframe?
# MAGIC

# COMMAND ----------

#Question 4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 5: Print the first 5 rows of the dataframe.
# MAGIC #### Some of the columns are long - print the entire record, regardless of length.
# MAGIC

# COMMAND ----------

#Question 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# MAGIC
# MAGIC Look at the first 50 rows of that dataframe.
# MAGIC Which value appears to be the most common?

# COMMAND ----------

# Question 6

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# MAGIC
# MAGIC What is the product title for that review? How many helpful votes did it have?

# COMMAND ----------

#Question 7

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 8: How many reviews exist in the dataframe with a 5 star rating?
# MAGIC

# COMMAND ----------

#Question 8 


# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# MAGIC Create a new dataframe with just those 3 columns, except cast them as "int"s.

# COMMAND ----------

# Question 9 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 10: Find the date with the most purchases.
# MAGIC Print the date and total count of the date which had the most purchases.
# MAGIC

# COMMAND ----------

#Question 10 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 11: Write the dataframe from Question 3 to your drive in JSON format.
# MAGIC Feel free to pick any directory on your computer.
# MAGIC Use overwrite mode.
# MAGIC Teardown
# MAGIC Stop the SparkSession

# COMMAND ----------

#Question 11

# COMMAND ----------


