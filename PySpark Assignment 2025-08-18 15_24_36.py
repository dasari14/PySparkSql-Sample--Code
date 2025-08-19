# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Bank_Transaction").master("local").getOrCreate()
sc = spark.sparkContext
cust_Df = spark.read.format("csv")\
         .options(inferSchema='True',delimiter=',')\
        .load("/FileStore/tables/Banking_transcations_data.csv")\
        .toDF("txnid", "txndate", "custid", "amount", "txn_type", "channel", "branch", "city", "state", "payment_method")
cust_Df.show()
# high_value_txns = cust_Df.filter(F.col("amount") > 10000).select("txnid", "amount", "payment_method")
# high_value_txns.show()
upi_df= cust_Df.filter("payment_method='upi' and (amount > 10000)").select("*")
upi_df.display()


# COMMAND ----------

# 2) Calculate the total cash withdrawals per city.

# Find the most frequently used channel in Chennai.


cash_withdrawl= cust_Df.groupBy("city","state").agg(F.sum("amount").alias("total_cash_withdrawl"))
cash_withdrawl.show()

most_freq=  cust_Df.filter(F.col("city") == "Chennai").groupBy("channel").count().orderBy(F.desc("count")).limit(1)
most_freq.show()

# COMMAND ----------

# 3) Grouping & Summarization
# Group by txn_type and get the sum of amount for each type.
# For each customer, find their total number of transactions and maximum transaction value.

txn_type= cust_Df.groupby("txn_type").agg(F.sum("amount").alias("txn_type_amt"))
txn_type.show()

# COMMAND ----------

# 4) Trend Analysis
# Get transactions per month for each branch (by extracting the month from txndate).
# Show which city had the largest total deposits during January 2023.
from pyspark.sql.functions import month,year
txn_month= cust_Df.withColumn("month", month("txndate"))
txn_month.show()

jan_2023_deposits = txn_month.filter((year("txndate") == 2023) & (month("txndate") == 1))
jan_2023_deposits.show()
total_deposits_DF= jan_2023_deposits.groupby("city").agg(F.sum("amount").alias("total_deposit"))
total_Deposits_orderBy= total_deposits_DF.orderBy(total_deposits_DF.total_deposit.desc()).limit(1)
total_Deposits_orderBy.show()

# COMMAND ----------

# 5) Advanced Analysis
# Find all branches where the average amount for Cash Withdrawal exceeds ₹5,000.

all_branch= cust_Df.filter(F.col("txn_type") == "Cash Withdrawal").groupby("branch").agg(F.avg("amount").alias("Avg_amt")).filter(F.col("Avg_amt") > 5000)
all_branch.show()

# COMMAND ----------

# Detect customers who did consecutive transactions (by date) on different channels.

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, datediff
window_spec = Window.partitionBy("custid").orderBy("txndate")
df_with_prev = cust_Df.withColumn("prev_txndate", lag("txndate").over(window_spec)) \
                     .withColumn("prev_channel", lag("channel").over(window_spec))
consecutive_diff_channel = df_with_prev.withColumn(
    "days_diff", datediff(col("txndate"), col("prev_txndate"))
).filter(
    (col("days_diff") == 1) & (col("channel") != col("prev_channel"))
)

consecutive_diff_channel.show()


# COMMAND ----------

