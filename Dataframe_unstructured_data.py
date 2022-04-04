from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "dataframe unstructured data")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

my_regex = r'^(\S+) (\S+)\t(\S+)\,(\S+)'

lines_df = spark.read.text("D:\\BIGDATA\\Spark\\Spark Data\\orders_new.csv")

final_df = lines_df\
    .select(regexp_extract("value", my_regex, 1).alias("order_id"),
            regexp_extract("value", my_regex, 2).alias("order_date"),
            regexp_extract("value", my_regex, 3).alias("order_customer_id"),
            regexp_extract("value", my_regex, 4).alias("status"))

final_df.printSchema()

final_df.show()

final_df.select("order_id").show()

final_df.groupby("status").count().show()

spark.stop()


