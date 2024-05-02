# Databricks notebook source
# MAGIC %md
# MAGIC # Week 5 Lab

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import time

# COMMAND ----------

# MAGIC %md
# MAGIC Write a streaming pipeline that reads from reviews delta table - similar to week 4

# COMMAND ----------

# Create spark session
spark = SparkSession.builder.appName("Week5").getOrCreate()

#Create schema for reviews
reviews_schema = StructType([
StructField("marketplace", StringType(), nullable=True)
,StructField("customer_id", StringType(), nullable=True)
,StructField("review_id", StringType(), nullable=True)
,StructField("product_id", StringType(), nullable=True)
,StructField("product_parent", StringType(), nullable=True)
,StructField("product_title", StringType(), nullable=True)
,StructField("product_category", StringType(), nullable=True)
,StructField("star_rating", IntegerType(), nullable=True)
,StructField("helpful_votes", IntegerType(), nullable=True)
,StructField("total_votes", IntegerType(), nullable=True)
,StructField("vine", StringType(), nullable=True)
,StructField("verified_purchase", StringType(), nullable=True)
,StructField("review_headline", StringType(), nullable=True)
,StructField("review_body", StringType(), nullable=True)
,StructField("purchase_date", StringType(), nullable=True)
,StructField("review_timestamp", TimestampType(), nullable=True)
])

#Read from reviews table with spark structured streaming
reviews_df = spark.readStream \
    .schema(reviews_schema) \
    .format("delta") \
    .option("catalog", "hive_metastore") \
    .table("default.nick_test_two") \

#Create a temporary view 
reviews_df.createOrReplaceTempView("reviews_tmp")
#With another dataframe, use spark to read the customers tsv from AWS S3
customers_df = spark.read \
    .csv("file:/Workspace/Repos/nrafferty@1904labs.com/hwe-labs/resources/customers.tsv.gz", header="true", sep="\t")
customers_df.createOrReplaceTempView("customers_tmp")

#Join the two datasets together using spark.sql and store in a silver_df dataframe
silver_df = spark.sql("""
   select r.marketplace
         ,r.customer_id
         ,r.review_id
         ,r.product_id
         ,r.product_parent
         ,r.product_title
         ,r.product_category
         ,r.star_rating
         ,r.helpful_votes
         ,r.total_votes
         ,r.vine
         ,r.verified_purchase
         ,r.review_headline
         ,r.review_body
         ,r.purchase_date
         ,c.customer_name
         ,c.gender
         ,c.date_of_birth
         ,c.city
         ,c.state
    from reviews_tmp r
          inner join customers_tmp c
          on r.customer_id = c.customer_id
          where r.verified_purchase = 'Y'
    """)

#Use spark structured streaming to kick off a writeStream query and write parquet files to the silver layer location
query = silver_df.writeStream.format("memory").outputMode("append").queryName("testQuery").trigger(processingTime="10 seconds").start()

for i in range(5):
    spark.sql("SELECT * from testQuery").show(1)
    time.sleep(10)

query.awaitTermination()

# COMMAND ----------

# Additional challenge problems:
#How would you transform data during this stream into the silver layer - pick a column and perform a streaming transformation on it
#Do any columns need to be flattened? If they did, what would that look like? It's a very common practice to have a nested map in the raw data that needs to be flattened out into distinct columns in the silver layer. What steps would you take to do that? 
