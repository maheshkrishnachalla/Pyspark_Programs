from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functools import reduce

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "Manual Pivot")
spark_conf.set("spark.master", "local[*]")
spark_conf.set("spark.sql.crossJoin.enabled", "true")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

data = [(1,10), (1,10), (3,20), (4,50), (2,20), (4,20), (5,100), (2,50)]

schema = "month int, cost int"

df = spark.createDataFrame(data=data, schema=schema)

months = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
          7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
          }

df.show()

sums = []
for k, v in months.items():
    sum_df = df.filter(df.month == k).agg(sum("cost").alias("v"))
    sums.append(sum_df)

print(sum_df)
print(sums)

result = reduce(lambda a, b: a.crossjoin(b), sums)

result.show()


spark.stop()