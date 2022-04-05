from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


spark_conf = SparkConf()
spark_conf.set("spark.app.name", "dataframe grouping")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

invoice_schema_ddl = "country string, weeknum int, InvoiceNo int, Quantity int, invoicevalue double"

invoice_df = spark.read \
    .format("csv") \
    .option("header", False) \
    .schema(invoice_schema_ddl)\
    .option("path", "D:\\BIGDATA\\Spark\\Dataframe and Dataset\\windowdata2.csv") \
    .load()

#invoice_df.printSchema()

my_window = Window.partitionBy("country")\
    .orderBy("weeknum")\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

my_df = invoice_df.withColumn("RunningTotal", sum("invoicevalue").over(my_window))

my_df.show()

spark.stop()
