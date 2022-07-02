from sys import stdin
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, col, date_format

spark_conf = SparkConf()
spark_conf.set("spark.app.name","Dataframe Loglevel Grouping")
spark_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# this is how the data present in the log file
smpl_data =[("DEBUG,2015-2-6 16:24:07"),
         ("WARN,2016-7-26 18:54:43"),
         ("DEBUG,2012-4-26 14:26:50"),
         ("DEBUG,2013-9-28 20:27:13"),
         ("INFO,2017-8-20 13:17:27"),
         ("INFO,2015-4-13 09:28:17"),
         ("DEBUG,2015-7-17 00:49:27"),
         ("DEBUG,2014-7-26 02:33:09"),
         ("INFO,2012-10-18 14:35:19")]

# explicit schema creation
log_schema = "level string, datetime timestamp"

# loading the data from file into the dataframe by creating dataframe
big_log= spark.read.option("header",True)\
    .option("delimiter",",")\
    .schema(schema=log_schema)\
    .csv("D:\\BIGDATA\\Spark\\Dataframe and Dataset\\bigLog.txt")


month_num_df = big_log.withColumn("month",date_format(col("datetime"),'MMMM')).drop(col("datetime"))

month_names = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]

# grouping and pivot the month names as column names and error logs as rows
aggregate_df = month_num_df.groupby("level").pivot("month",month_names).count()

aggregate_df.show(truncate=False)

# to hold the commandline and the spark session
stdin.readline()
spark.stop()
