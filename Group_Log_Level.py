from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Group Log Level Count").enableHiveSupport().getOrCreate()

load_data = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\Spark Data\\bigLog.txt")

mapped_data = load_data.map(lambda x: (x.split(":")[0], x.split(":")[1]))

grouped_data = mapped_data.groupByKey()

grouped_mapped_data = grouped_data.map(lambda x:(x[0], len(x[1])))

result = grouped_mapped_data..collect()

for res in result:
    print(res)
