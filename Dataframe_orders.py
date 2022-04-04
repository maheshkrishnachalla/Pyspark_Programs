from sys import stdin
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "Dataframe Orders")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

orders_schema = StructType([StructField("orderid", IntegerType()),
                                StructField("orderdate", TimestampType()),
                                StructField("customerid", IntegerType()),
                                StructField("status", StringType())])

order_schema_ddl = """orderid integer, orderdate timestamp, customerid integer, status string"""

orders_df = spark.read\
    .format("csv")\
    .option("header", True)\
    .schema(order_schema_ddl)\
    .option("path", "D:\\BIGDATA\\Spark\\Spark Data\\orders.csv")\
    .load()

"""group_df = orders_df.repartition(4)\
    .where("order_customer_id > 10000")\
    .select("order_id", "order_customer_id")\
    .groupBy("order_customer_id")\
    .count()

group_df.show(truncate=False)
"""
orders_df.printSchema()

orders_df.show(truncate=False)


stdin.readline()

spark.stop()

