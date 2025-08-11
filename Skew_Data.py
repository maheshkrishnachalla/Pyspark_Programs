from sys import stdin

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf = SparkConf()
spark_conf.set("spark.app.name","Skewed Data")
spark_conf.set("spark.master", "local[2]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# Sample data for demonstration
data_left = [("A", 1), ("A", 2), ("B", 3), ("B", 4), ("C", 5), ("C", 6)]
data_right = [("A", "x"), ("B", "y"), ("C", "z")]

#create dataframes
df_left = spark.createDataFrame(data_left, ["key", "valued"])
df_right = spark.createDataFrame(data_right, ["key", "description"])

#Number of salts to distribute skewed keys
num_salts = 3

#Identify skewed keys manually (for Illustration)
skewed_keys = ["A"]

#Separate skewed and non-skewed data on left df
skewed_left = df_left.filter(col("key").isin(skewed_keys))
non_skewed_left = df_left.filter(~col("key").isin(skewed_keys))

#salt the skewed left data by adding a random number to distribute
skewed_left_salted = skewed_left.withColumn("salt",(ceil(rand()) * num_salts) % num_salts)

skewed_left_salted  = skewed_left_salted.withColumn("salted_key", concat(col("key"),lit("_"),col("salt")))


# Replicate right DataFrame for each salt value
salt_values = spark.createDataFrame([(i, ) for i in range(num_salts)], ["salt"])
right_replicated = df_right.crossJoin(salt_values)\
    .withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

#Join salted skewed data on salted_key
join_skewed = skewed_left_salted.join(right_replicated, "salted_key").drop("salt")

join_skewed = join_skewed.select(skewed_left_salted["key"], col("valued"), col("description"))

# join non-skewed data normally
join_non_skewed = non_skewed_left.join(df_right, "key")

# union both results
final_df = join_skewed.select(col("key"), col("valued"), col("description")).union(join_non_skewed)

final_df.show()

stdin.readline()

spark.stop()





