from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark_conf = SparkConf()
spark_conf.set("spark.app.name", "dataframe grouping")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

invoice_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "D:\\BIGDATA\\Spark\\Dataframe and Dataset\\order_data.csv") \
    .load()

# column expression
summary_df = invoice_df\
    .groupby("Country", "InvoiceNo")\
    .agg(sum("Quantity").alias("TotalQuantity"),
         sum(expr("Quantity * UnitPrice")).alias("InvoiceValue"))

summary_df.show()

# string expression

summary_df1 = invoice_df.groupby("Country", "InvoiceNo")\
    .agg(expr("sum(Quantity) as TotalQuantity"),
         expr("sum(Quantity * UnitPrice) as InvoiceValue"))

summary_df1.show()


# sql
invoice_df.createOrReplaceTempView("sales")

spark.sql("select Country, InvoiceNo, "
          "sum(Quantity) as TotalQuantity, "
          "sum(Quantity * UnitPrice) as InvoiceValue from "
          "sales group by "
          "Country, InvoiceNo ").show()

spark.stop()
