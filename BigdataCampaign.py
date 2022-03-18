from pyspark.sql import SparkSession


def load_boring_words():
    boring_words = set()
    with open("D:\\BIGDATA\\Spark\\Spark Data\\boringwords.txt") as f:
        lines = f.readlines()
    for line in lines:
        boring_words.add(line.rstrip())
    return boring_words


def convert(data):
    return name_set.value


def get_amount_and_words(lines):
    line = lines.split(",")
    words = line[0]
    amount = float(line[10])
    return amount, words


spark = SparkSession.builder.master("local").appName("Big data Campaign").enableHiveSupport().getOrCreate()

name_set = spark.sparkContext.broadcast(load_boring_words())

input_data = spark.sparkContext.textFile("D:\\BIGDATA\Spark\\bigdata-campaign-data.csv")

mapped_data = input_data.map(lambda x: get_amount_and_words(x)) #map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))

flatMapped_data = mapped_data.flatMapValues(lambda x: x.split(" ")).map(lambda x: (x[1].lower(), x[0]))

finalMapped_data = flatMapped_data.filter(lambda x: x[0] not in convert(x[0]))

aggregated_data = finalMapped_data.reduceByKey(lambda x, y: x + y)

sorted_data = aggregated_data.sortBy(lambda x: x[1], False).map(lambda x: (x[0], round(x[1], 2)))

result = sorted_data.take(50)

for i in result:
    print(i[0], ":", i[1])

"""boring_words = load_boring_words()
print("Number of boring words =", len(boring_words))
for word in boring_words:
    print(word, end=", ")
"""
