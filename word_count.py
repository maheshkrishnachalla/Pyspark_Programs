from pyspark.sql import  SparkSession

spark = SparkSession.builder.master("local").appName("Word Count").enableHiveSupport().getOrCreate()

input_data = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\InputFile.txt")

flatmap_data = input_data.flatMap(lambda x: x.split(" "))

map_data = flatmap_data.map(lambda x: (x, 1))

aggregated_data = map_data.reduceByKey(lambda x, y: x+y)

result_data = aggregated_data.collect()

for res in result_data:
    print(res)