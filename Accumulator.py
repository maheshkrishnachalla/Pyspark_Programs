from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Accumulator").enableHiveSupport().getOrCreate()

load_input = spark.sparkContext.textFile("D:\\BIGDATA\Spark\\samplefile.txt")

my_accum = spark.sparkContext.accumulator(0)

result  = load_input.collect()
for i in result:
    if i == "":
        my_accum.add(1)

print(my_accum)
