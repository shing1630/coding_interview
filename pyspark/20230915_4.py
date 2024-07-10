# In a PySpark environment, you're given a DataFrame df with the following columns: 'id', 'first_name', 'last_name', and 'email'. Can you write a PySpark code snippet to achieve the following transformations?

# Remove any rows where 'email' is null.
# Create a new column 'full_name' that is a concatenation of 'first_name' and 'last_name', separated by a space.
# Rename the column 'email' to 'contact_email'.

import logging
from pyspark.sql import SparkSession
from pyspark.sql import Function as F

# setup logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("id", StringType, false),
    StructField("first_name", StringType, false),
    StructField("last_name", StringType, false),
    StructField("email", StringType, false),
])

df = spark.read.format("csv").option("header", "true").schema(schema).load("/landing/users.csv")

logger.info("Removing any rows where email is null")
# Remove any rows where 'email' is null.
df_drop_email_na = df.na.drop(subset=["email"])

logger.info("full name concatenation")
# Create a new column 'full_name' that is a concatenation of 'first_name' and 'last_name', separated by a space.
df_drop_email_na_with_full_name = df_drop_email_na.withColumn("full_name", concat_ws(" ", F.col("first_name"), F.col("last_name")))

logger.info("contact_email rename")
# Rename the column 'email' to 'contact_email'.
df_drop_email_na_with_full_name_contract_email = df_drop_email_na_with_full_name.withColumnRenamed("email", "contact_email")

df_drop_email_na_with_full_name_contract_email.write.option("header", "true").csv.("/cleansed/users.csv")


# What is the different between option 1 and option 2?
# Which option is better and why?

# option 1:
# df_drop_email_na = df.na.drop(subset=["email"])
# df_drop_email_na_with_full_name = df_drop_email_na.withColumn("full_name", concat_ws(" ", F.col("first_name"), F.col("last_name")))
# df_drop_email_na_with_full_name_contract_email = df_drop_email_na_with_full_name.withColumnRenamed("email", "contact_email")
# end of option 1

# option 2:
# df = df.na.drop(subset=["email"])
# df = df.withColumn("full_name", concat_ws(" ", F.col("first_name"), F.col("last_name")))
# df = df.withColumnRenamed("email", "contact_email")
# end of option 2

