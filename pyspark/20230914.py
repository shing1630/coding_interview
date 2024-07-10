# You have been given a large dataset of customer transactions in a PySpark DataFrame with the following columns: transaction_id, customer_id, transaction_date, product_id, and transaction_amount.

# Write a PySpark script that achieves the following:

# 1. Calculate the total transaction amount for each customer.
# 2. Find the top 10 customers who have the highest total transaction amount.
# 3. For these top 10 customers, find the product they bought most frequently.

# skip spark setup 

df = spark.read.csv("/landing/transction.csv")

# 1. Calculate the total transaction amount for each customer.
df_total_amount_per_customer = df.groupBy("customer_id") \
                                 .agg(sum("transaction_amount").alias("sum_transaction_amount"))

# 2. Find the top 10 customers who have the highest total transaction amount.
df_top_10 = df_total_amount_per_customer.sort(desc("sum_transaction_amount")) \
                                        .limit(10)

# 3. For these top 10 customers, find the product they bought most frequently.
df_top_10_product = df.join(df_top_10, df.customer_id == df_top_10.customer_id, "inner") \
                      .groupBy("customer_id", "product_id") \
                      .count() \
                      .max("count")

# anwser
from pyspark.sql import Window
from pyspark.sql.functions import row_number

# 3. For these top 10 customers, find the product they bought most frequently.
df_top_10_product_frequency = df.join(df_top_10, df.customer_id == df_top_10.customer_id, "iner") \
                                .groupBy("customer_id", "product_id") \
                                .count() 

window = Window.partitionBy(df_top_10_product_frequency['customer_id']) \
               .orderBy(df_top_10_product_frequency['count'].desc())

df_top_product_per_customer = df_top_10_product_frequency.select('*', row_number().over(window).alias('rank')) \
                                                         .filter(col('rank') <= 1)
