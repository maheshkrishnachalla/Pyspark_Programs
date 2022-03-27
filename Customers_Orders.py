import logging
from sys import stdin

from pyspark import StorageLevel
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Customer Orders").enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel("INFO")

spark_web_URL = spark.sparkContext.uiWebUrl

print(spark_web_URL)

base_rdd = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\Spark Data\\spark-data\\customer-orders.csv")

mapped_input = base_rdd.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))

total_by_customer = mapped_input.reduceByKey(lambda x, y: x + y)

prime_customers = total_by_customer.filter(lambda x: x[1] > 5000)

doubled_amount = prime_customers.map(lambda x: (x[0], round(x[1] * 2, 2))).persist(StorageLevel.MEMORY_ONLY)

result = doubled_amount.collect()

for res in result:
    print(res)

print(prime_customers.count())

stdin.readline()
spark.stop()
