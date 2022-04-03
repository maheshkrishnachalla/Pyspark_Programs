from sys import stdin

from pyspark import SparkConf
from pyspark.sql import SparkSession

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

orders_df.createOrReplaceTempView("orders")

# result_df = spark.sql("select order_status, count(*) as total_orders from orders group by order_status")

result_df = spark.sql("""select order_customer_id, count(*) as total_orders
 from orders where order_status = 'CLOSED' group by order_customer_id order by total_orders desc """)

result_df.show(100)

stdin.readline()
spark.stop()
