# You are given a large dataset in the form of a PySpark DataFrame called df. The DataFrame has millions of rows and several columns including 'UserId' and '"TransactionAmount"'. Our goal is to find the total transaction amount for each user.

# Here's a sample of the DataFrame:

# UserId	"TransactionAmount"
# 1	100
# 2	200
# 1	150
# 3	300
# 2	250
# 3	350
# Please write a PySpark code snippet to solve this problem.

from pyspark.sql import functions as F
# skip spart setup

df = spark.read.format("csv").option("header", "true").load("/landing/transaction.csv")

df_result = df.groupBy("UserId") \
              .agg(F.sum("TransactionAmount").alias("SumTransactionAmount"))

df_result.show()