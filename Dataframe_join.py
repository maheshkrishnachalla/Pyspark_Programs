from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "dataframe grouping")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

orders_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "D:\\BIGDATA\\Spark\Dataframe and Dataset\\orders.csv") \
    .load()

customers_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "D:\\BIGDATA\\Spark\\Dataframe and Dataset\\customers.csv") \
    .load()

# orders_df.printSchema()
# customers_df.printSchema()

join_condition = orders_df.customer_id == customers_df.customer_id

join_df = orders_df.join(customers_df, join_condition, "inner")

join_df.show()

spark.stop()
