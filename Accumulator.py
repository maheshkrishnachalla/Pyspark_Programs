from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Accumulator").enableHiveSupport().getOrCreate()

load_input = spark.sparkContext.textFile("D:\\BIGDATA\Spark\\samplefile.txt")

accumulator = spark.sparkContext.accumulator(0)

result = load_input.collect()
for i in result:
    if i == "":
        accumulator.add(1)

print(accumulator)
