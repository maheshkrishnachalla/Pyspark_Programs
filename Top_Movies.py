from sys import stdin

from pyspark.sql import SparkSession


def get_ratings_data(input_data, delimiter):
    result_data = input_data.split(delimiter)
    return result_data[1], result_data[2]


def get_movies_data(input_data, delimiter):
    result_data = input_data.split(delimiter)
    return result_data[0], (result_data[1], result_data[2])


spark = SparkSession.builder \
    .master("local") \
    .appName("Top Movies") \
    .enableHiveSupport() \
    .getOrCreate()

rating_rdd = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\Moviesdata\\ratings.dat")

mapped_rdd = rating_rdd.map(lambda x: get_ratings_data(input_data=x, delimiter="::"))

map_value_rdd = mapped_rdd.mapValues(lambda x: (float(x), 1.0))

reduced_rdd = map_value_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

filtered_rdd = reduced_rdd.filter(lambda x: x[1][0] > 1000)

final_rdd = filtered_rdd.mapValues(lambda x: x[0] / x[1]).filter(lambda x: x[1] > 4.5)

movies_rdd = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\Moviesdata\\movies.dat")

movies_mapped_rdd = movies_rdd.map(lambda x: get_movies_data(input_data=x, delimiter="::"))

joined_rdd = movies_mapped_rdd.join(final_rdd)

top_movies_rdd = joined_rdd.map(lambda x: x[1][0])

result = top_movies_rdd.collect()

for res in result:
    print(res)

stdin.readline()

spark.stop()
