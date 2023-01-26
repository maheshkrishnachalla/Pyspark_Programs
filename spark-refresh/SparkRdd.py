from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SparkRdd").getOrCreate()

# create RDD from parallelize data
dataSeq = [("Java", 20000), ("Python", 30000), ("Scala", 10000)]
rdd = spark.sparkContext.parallelize(dataSeq)
items = rdd.collect()
for i in items:
    print(i)
print("Number of partitions =" , rdd.getNumPartitions())

# create RDD from reading files

rdd = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\files")
#rdd.foreach(print)
items_in_files = rdd.collect()

for item in items_in_files:
    print(item)


#create empty RDD by using sparkContext.parallelize
rdd = spark.sparkContext.parallelize([])
lt = rdd.collect()
for i in lt:
    print(i)

# read whole text files from the directory
rdd_whole = spark.sparkContext.wholeTextFiles("D:\\BIGDATA\\Spark\\files")

items_in_wholefiles = rdd_whole.collect()

for item in items_in_wholefiles:
    print(item[0], item[1])



# rdd read multiple text files from the directory

rdd_multi = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\files\\text01.txt,D:\\BIGDATA\\Spark\\files\\text02.txt,"
                                        "D:\\BIGDATA\\Spark\\files\\text03.txt,D:\\BIGDATA\\Spark\\files\\text04.txt,"
                                        "D:\\BIGDATA\\Spark\\files\\Invalid.txt")

items_in_multiFiles = rdd_multi.collect()

for item in items_in_multiFiles:
    print(item)

#Reading multiple CSV files into RDD

rdd_multiple_csv = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\files")

rdd_map = rdd_multiple_csv.map(lambda x:x.split(","))

rdd_csv_collect = rdd_map.collect()

for i in rdd_csv_collect:
    print(i[0], "|", i[1])

spark.stop()