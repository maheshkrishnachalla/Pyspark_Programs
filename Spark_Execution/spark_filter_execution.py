import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, StructField, IntegerType

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "spark filter execution")
spark_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

data = [(1, 'Ganesh', 100, 500),
        (2, 'Narayana', 200, 450),
        (3, 'Sujatha', 300, 450),
        (4, 'Hari', 400, 500)]

"""schema = StructType([StructField("id", IntegerType()),
                     StructField("name", StringType()),
                     StructField("block", IntegerType()),
                     StructField("zip_code", IntegerType())])
"""
schema = "id int, name string, block int, zip_code int"
df = spark.createDataFrame(data=data, schema=schema)

df2 = df.groupBy("zip_code").count()

df3 = df.filter("block < 300 ")

df3.explain()

df3.show()

sys.stdin.readline()
spark.stop()