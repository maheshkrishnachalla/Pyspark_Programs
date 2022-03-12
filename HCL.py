from pyspark.sql import SparkSession

# spark session creation
spark = SparkSession.builder.master("local").appName("HCL").enableHiveSupport().getOrCreate()


def age_parser(row):
    # age parser to filter based on the age
    row = row.split(",")
    if int(row[1]) > 18:
        return 'Y'
    else:
        return 'N'


# loading the file in input RDD
input = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\dataset1")

# passing age_parser function to map to create tuple with new column has Y/N
mapped = input.map(lambda x: (x, age_parser(x)))

# collecting the results in result array collection
result = mapped.collect()

for i in result:
    print(i)
