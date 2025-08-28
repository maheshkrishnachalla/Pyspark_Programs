from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf = SparkConf()
spark_conf.set("spark.app.name", 'wide dataframe (large of colums)')
spark_conf.set("spark.master","local[2]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

df = spark.createDataFrame([('row1', *range(500))],["id"] + [f"sensor_{i}" for i in range(500)])

df.printSchema()

df.explain()

df.show()

spark.stop()