import datetime
import sys
from dateutil.relativedelta import relativedelta
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "scd target init")
spark_conf.set("spark.master", "local[2]")


def get_df_cols_list(df):
    return [field.name for field in df.schema]


spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

source_emp_df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "./input/employee.csv") \
    .load()

"""
for col, dtype in emp_df.dtypes:
    print(f"{col} : {dtype}")

for field in emp_df.schema:
    print(f"{field.name} : {field.dataType}")
"""
print(get_df_cols_list(df=source_emp_df))

old_cols = source_emp_df.columns
new_cols = [col.strip().replace("\'", "") for col in old_cols]
# print(type(i) for i in new_cols )


source_emp_df = source_emp_df.toDF(*new_cols)

for i in new_cols:
    source_emp_df = source_emp_df.withColumn(i, regexp_replace(i, "\'", ""))

dt = datetime.datetime.now().date()
dt = dt - relativedelta(days=0)

source_emp_df = source_emp_df.withColumn("start_date", lit(dt)) \
    .withColumn("end_date", to_date(lit("2999-12-31"), 'yyyy-MM-dd'))\
    .withColumn("active_flag", lit('Y'))


source_emp_df.write.mode("overwrite") \
    .partitionBy("department", "active_flag") \
    .option("path", "./output/parquet/hist_employee") \
    .format("parquet") \
    .save()


sys.stdin.readline()
spark.stop()


