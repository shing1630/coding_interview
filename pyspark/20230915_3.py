# Task: User Log Analysis

# Given a dataset of user logs, your task is to analyze the data to find the most active users. The dataset consists of two columns: user_id and timestamp. The user_id represents the unique identifier for each user and timestamp is the time of the user's activity in the format yyyy-mm-dd HH:MM:SS.

# Your task is divided into three parts:

# Data Loading: Load the data from a CSV file into a PySpark DataFrame.

# Data Processing: Process the data to find the top 10 most active users in terms of total activities.

# Data Output: Write the output (top 10 most active users and their total activities) to a new CSV file.

# Regarding potential enhancements:

# Schema Definition: While loading the CSV file, it's good practice to define the schema, especially when working with big data. This can improve reading performance and ensure correct data types.

# Error Handling: Your script assumes that the file /landing/logs.csv exists and can be read without errors. In a production environment, you'd want to add error handling code to manage potential issues (like file not found, permission errors, etc).

# Timestamp Parsing: You didn't perform any explicit timestamp parsing. While PySpark may automatically infer the correct type, for production code, it's a good idea to explicitly parse or validate the timestamp format.

# Data Persistence: Consider persisting the cleansed DataFrame if it's reused multiple times. This can improve performance by reducing the number of times data is read from disk.

# Unit Tests: Consider writing unit tests to ensure the correctness of your code. You can use PySpark's DataFrameSuiteBase to write tests for Spark applications.

# Logging: Incorporate logging so that the status of data processing can be monitored and potential issues can be debugged.

# Documentation: While your code is straightforward, remember to add comments and document your functions in a production environment, especially when the logic is complex.

# Modularize Your Code: Try to make your code more modular by creating separate functions for different aspects of the data processing. This would make your code more maintainable and easier to test.

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# setup logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

logger.info("------ Spark session created --------")

# Define schema
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Data loading
logger.info("Starting data loading")

df = spark.read.format("csv").option("header", "true").schema(schema).load("/landing/logs.csv")

logger.info("Data loaded successfully")


# parse timestamp
logger.info("Starting timestamp parsing")

df = df.withColumn("timestamp", F.to_timestamp(F.col("timestamp", "yyyy-MM-dd HH:mm:ss")))

logger.info("Timestamp parsing completed")


# Data processing

logger.info("Starting data cleansing")

# cleansing
# 1. remove null record. It is better to remove null record rather than assign default value because it will not affect the result.
df_drop_na = df.na.drop(subset=["user_id", "timestamp"])

# 2. remove duplicated record because it will increase count the result.
df_cleansed = df_drop_na.dropDuplicates(subset=["user_id", "timestamp"])

logger.info("Data cleansing completed")


# find top 10
logger.info("Starting finding top 10 users")

df_top_10 = df_cleansed.groupBy("user_id") \
                       .count().alias("total_activity") \
                       .orderBy(F.col("total_activity").desc()) \
                       .limit(10)

logger.info("Top 10 users found")


# Data Output
logger.info("Starting data output")

df_top_10.write.option("header", "true").csv("/refined/top10.csv")

logger.info("Data output completed")


