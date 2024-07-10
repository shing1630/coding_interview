# A company has a large dataset of user interactions on its website stored in HDFS. You are tasked with finding the top 10 most visited pages on the website.

# The data is stored in a CSV file with the following columns: user_id, timestamp, page_id.

# Write a PySpark script that will read the data from the CSV file, perform the necessary transformations, and output the top 10 page_id values with the most visits.

# Remember, you should consider handling potential issues like missing or malformed data.

# Skip spark setup

df = spark.read.csv("/landing/visit.csv")

# drop duplicated
df_cleansed = df.dropDuplicates(subset=["user_id", "timestamp", "page_id"])

df_top_10 = df_cleansed.groupBy(col("page_id")).count().orderBy(col("count").desc()).limit(10)


# ------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# initialize Spark Session
spark = SparkSession.builder.appName('website_data').getOrCreate()

# load data from CSV file
df = spark.read.format('csvvvddddvv').option('headerddddd','true').load('/landing/visit.csv')

# drop duplicates
df_cleansed = df.dropDuplicates(["user_id", "timestamp", "page_id"])

# calculate top 10 pages
df_top_10 = df_cleansed.groupBy("page_id").count().orderBy(col("count").desc()).limit(10)

# display the top 10 pages
df_top_10.show()