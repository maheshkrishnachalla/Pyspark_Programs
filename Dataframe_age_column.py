from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StringType

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "dataframe grouping")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

df = spark.read \
    .format("csv") \
    .option("inferSchema", True) \
    .option("path", "D:\\BIGDATA\\Spark\\dataset1") \
    .load()

df1 = df.toDF("name", "age", "city")


def age_checker(age):
    if age > 18:
        return "Y"
    else:
        return "N"


# parse_age_function = udf(age_checker, StringType())
# df2 = df1.withColumn("adult", parse_age_function("age"))

spark.udf.register("parse_age_function", age_checker, StringType())

for x in spark.catalog.listFunctions():
    print(x)

df2 = df1.withColumn("age", expr("parse_age_function(age)"))

df2.printSchema()

df2.show()
