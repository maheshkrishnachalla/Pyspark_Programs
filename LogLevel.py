from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Log Level").enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel("INFO")

if __name__ == "__main__":
    my_list = list(["WARN: Tuesday 4 September 0405",
                    "ERROR: Tuesday 4 September 0408",
                    "ERROR: Tuesday 4 September 0408",
                    "ERROR: Tuesday 4 September 0408",
                    "ERROR: Tuesday 4 September 0408",
                    "ERROR: Tuesday 4 September 0408"])

    original_logs_rdd = spark.sparkContext.parallelize(my_list)

else:
    original_logs_rdd = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\Spark Data\\bigLog.txt")
    print("else block")


pair_rdd = original_logs_rdd.map(lambda x: (x.split(":")[0], 1))

count_rdd = pair_rdd.reduceByKey(lambda x, y: x+y)

result = count_rdd.collect()

for i in result:
    print(i)
