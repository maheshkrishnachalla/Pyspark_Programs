from pyspark.sql import SparkSession


def recordings(records):
    record_points = records.split(",")
    station_id = record_points[0]
    reading_type = record_points[2]
    temp_recorded = int(record_points[3])
    return station_id, reading_type, temp_recorded


spark = SparkSession.builder.master("local").appName("Station Recordings").enableHiveSupport().getOrCreate()

input_data = spark.sparkContext.textFile("D:\\BIGDATA\\Spark\\temp-data.csv")

filtered_data = input_data.filter(lambda x: recordings(x)[1].lower() == "tmin")

mapped_data = filtered_data.map(lambda x: (recordings(x)[0], recordings(x)[2]))

result = mapped_data.groupByKey().collect()

for i in result:
    print(i[0], print([x for x in i[1]]))
    print(i[0], min(i[1]))
