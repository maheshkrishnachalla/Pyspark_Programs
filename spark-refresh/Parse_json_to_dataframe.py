from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "Json Parser to dataframe")
spark_conf.set("spark.master", "local[2]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

df = spark.read.format("json").option("multiline",True)\
    .option("path", "/Users/maheshkrishnachalla/PycharmProjects/dsa/DATASTR-N-ALGOR/Pyspark_Programs/input/customers.json")\
    .load()

df.show()

# Explode orders array to get one row per order
orders_df = df.select(
    "customer_id",
    "name",
    "email",
    "address.*",  # explode address struct into columns (street, city, state, zip)
    explode("orders").alias("order")
)

orders_df = orders_df.select(
    "customer_id",
    "name",
    "email",
    "street",
    "city",
    "state",
    "zip",
    "order.*",
    explode("order.items").alias("item")
)

orders_df = orders_df.select(
"customer_id",
    "name",
    "email",
    "street",
    "city",
    "state",
    "zip",
    "order_date",
    "order_id",
    "total_amount",
    "item.*"
)

orders_df = orders_df.select(
"customer_id",
    "name",
    "email",
    "street",
    "city",
    "state",
    "zip",
    "order_date",
    "order_id",
    "total_amount",
    "item_id",
    "price",
    "product_name",
    "quantity"
)

orders_df.explain(extended=True)

orders_df = orders_df.groupby("customer_id").agg(sum("total_amount").alias("tot_amount")).orderBy(desc("tot_amount"))

orders_df.show()

spark.stop()