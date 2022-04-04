from sys import stdin

from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf = SparkConf()
spark_conf.set("spark.ap.name", "dataframe read and write")
spark_conf.set("spark.master", "local[*]")
spark_conf.set("spark.jars", "C:\\Spark\\spark-avro_2.11-2.4.4.jar")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

orders_df = spark.read\
    .format("csv").option("header", True)\
    .option("inferSchema", True)\
    .option("path", "D:\\BIGDATA\\Spark\\Spark Data\\orders.csv")\
    .load()

print("number of partitions ", orders_df.rdd.getNumPartitions())

orders_new_df = orders_df.repartition(4)

orders_new_df.write\
    .format("avro")\
    .partitionBy("order_status")\
    .mode("overwrite")\
    .option("path", "D:\\BIGDATA\\Spark\\Spark Data\\orders_pyspark").save()

stdin.readline()
spark.stop()
