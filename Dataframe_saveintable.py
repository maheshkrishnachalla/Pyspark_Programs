from sys import stdin

from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "dataframe read and write")
spark_conf.set("spark.master", "local[*]")
spark_conf.set("spark.jars", "C:\\Spark\\spark-hive_2.11-2.4.4.jar")

spark = SparkSession.builder\
    .config(conf=spark_conf)\
    .enableHiveSupport().getOrCreate()

orders_df = spark.read\
    .format("csv").option("header", True)\
    .option("inferSchema", True)\
    .option("path", "D:\\BIGDATA\\Spark\\Spark Data\\orders.csv")\
    .load()

orders_df.write\
    .format("csv").mode("overwrite")\
    .bucketBy(4, "order_customer_id")\
    .sortBy("order_customer_id")\
    .saveAsTable("retail.orders3")

print("table is created")

stdin.readline()
spark.stop()
