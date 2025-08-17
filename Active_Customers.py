from sys import stdin

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_sub, current_date, to_date, lower, desc

seq = [
    [1, "alia", "Active", "2025-08-11"],
    [2, "bob", "Active", "2025-08-01"],
    [3, "charlie", "Inactive", "2025-07-25"],
    [4, "david", "Active", "2025-07-28"],
    [5, "emma", "Active", "2025-07-15"],
    [6, "farhan", "Active", "2025-08-05"],
    [7, "grace", "Active", "2025-08-02"],
    [8, "hannah", "Inactive", "2025-07-20"],
    [9, "ian", "Active", "2025-07-31"],
    [10, "jack", "Inactive", "2025-06-30"],
    [11, "kate", "Active", "2025-08-04"],
    [12, "leo", "Inactive", "2025-07-05"],
    [13, "maya", "Active", "2025-08-03"],
    [14, "nina", "Active", "2025-07-10"],
    [15, "oliver", "Active", "2025-08-07"],
    [16, "paul", "Active", "2025-07-29"],
    [17, "quinn", "Inactive", "2025-07-01"],
    [18, "ryan", "Active", "2025-08-06"],
    [19, "sophia", "Inactive", "2025-07-08"],
    [20, "tom", "Active", "2025-07-30"],
    [21, "ursula", "Active", "2025-08-09"],
    [22, "victor", "Inactive", "2025-06-28"],
    [23, "wade", "Active", "2025-08-08"],
    [24, "xena", "Active", "2025-08-10"],
    [25, "yara", "Inactive", "2025-07-18"],
    [26, "zane", "Active", "2025-08-01"],
    [27, "arjun", "Inactive", "2025-07-21"],
    [28, "bella", "Active", "2025-08-02"],
    [29, "carl", "Inactive", "2025-07-14"],
    [30, "dan", "Active", "2025-07-27"],
    [31, "elena", "Active", "2025-07-12"],
    [32, "fiona", "Inactive", "2025-08-07"],
    [33, "george", "Inactive", "2025-07-09"],
    [34, "harry", "Active", "2025-08-05"],
    [35, "isla", "Inactive", "2025-07-13"],
    [36, "james", "InActive", "2025-08-04"],
    [37, "kiran", "Inactive", "2025-07-16"],
    [38, "lara", "Active", "2025-08-03"],
    [39, "mohit", "Active", "2025-07-11"],
    [40, "nora", "Active", "2025-08-06"]
]

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "Active Customers")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf)\
    .getOrCreate()

schema = "id int, name string, status string, date string"

df = spark.createDataFrame(data=seq,schema=schema)

df = df.withColumn("date", to_date(col("date"),'yyyy-MM-dd' ))

active_customer_df = df.filter(
    (lower(col("status")) == 'active') &
    (col("date") >= date_sub(current_date(), 30)))\
    .orderBy(desc(col("date")))

# df show
df.orderBy(desc("date")).show(50)

# active customers df show
active_customer_df.show(50)

#input("Enter here :")
stdin.readline()
spark.stop()

