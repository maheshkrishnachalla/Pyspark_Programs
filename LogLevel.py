from pyspark.sql import SparkSession

my_list = list(["WARN: Tuesday 4 September 0405",
                "ERROR: Tuesday 4 September 0408",
                "ERROR: Tuesday 4 September 0408",
                "ERROR: Tuesday 4 September 0408",
                "ERROR: Tuesday 4 September 0408",
                "ERROR: Tuesday 4 September 0408"])

spark = SparkSession.builder.master("local").appName("Log Level").enableHiveSupport().getOrCreate()

original_logs_rdd = spark.sparkContext.parallelize(my_list)

pair_rdd = original_logs_rdd.map(lambda x: (x.split(":")[0], 1))

count_rdd = pair_rdd.reduceByKey(lambda x, y: x+y)

result = count_rdd.collect()

for i in result:
    print(i)
