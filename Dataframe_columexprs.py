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

invoice_df.select(count("*").alias("Rowcount"),
                  sum("Quantity").alias("TotalQuantity"),
                  avg("unitPrice").alias("AvgPrice"),
                  countDistinct("InvoiceNo").alias("countDistinct")).show()

invoice_df.selectExpr("count(*) as Rowcount",
                      "sum(Quantity) as TotalQuantity",
                      "avg(UnitPrice) as AvgPrice",
                      "count(Distinct(InvoiceNo)) as countDistinct").show()

invoice_df.createOrReplaceTempView("sales")

spark.sql("select count(*) as Rowcount, "
          "sum(Quantity) as TotalQuantity, "
          "avg(UnitPrice) as AvgPrice, "
          "count(Distinct(InvoiceNo)) as countDistinct"
          " from sales").show()

spark.stop()
