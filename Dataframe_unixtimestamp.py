from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, monotonically_increasing_id

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

new_df = orders_df.withColumn("date1", unix_timestamp(col("order_date")))\
    .withColumn("newid", monotonically_increasing_id())\
    .dropDuplicates(["order_date", "order_customer_id"])\
    .drop("order_id")\
    .sort("order_date")

new_df.printSchema()

new_df.show(truncate=False)

spark.stop()