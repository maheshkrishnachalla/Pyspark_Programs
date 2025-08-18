import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "hudi_example_1")
spark_conf.set("spark.master", "local[2]")
spark_conf.set("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0")
spark_conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

data = [
    {"id": 1, "name": "John", "ts": 1},
    {"id": 2, "name": "Jane", "ts": 2}
]

df = spark.createDataFrame(data=data)

df.write.format("hudi")\
    .option("hoodie.table.name", "test_hudi_table")\
    .option("hoodie.datasource.write.recordkey.field","id")\
    .option("hoodie.datasource.write.partitionpath.field","ts")\
    .mode("overwrite")\
    .save("./tmp/hudi/test_hudi_table")

result_df = spark.read.format("hudi").load("./tmp/hudi/test_hudi_table")
result_df.show()

sys.stdin.readline()
spark.stop()