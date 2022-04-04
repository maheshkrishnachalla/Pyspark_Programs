from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "dataframe grouping")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

orders_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "D:\\BIGDATA\\Spark\\Spark Data\\orders.csv") \
    .load()

orders_df.select("order_id","order_status").show()

orders_df.select(col("order_id")).show()

spark.stop()
