import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "scd types")
spark_conf.set("spark.master", "local[2]")

def get_df_cols_list(df):
    return [field.name for field in df.schema]


spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

emp_df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path", "./input/employee.csv")\
    .load()

"""
for col, dtype in emp_df.dtypes:
    print(f"{col} : {dtype}")
    
for field in emp_df.schema:
    print(f"{field.name} : {field.dataType}")
"""
print(get_df_cols_list(df=emp_df))

old_cols = emp_df.columns
new_cols = [col.strip().replace("\'","") for col in old_cols]
#print(type(i) for i in new_cols )


emp_df = emp_df.toDF(*new_cols)

for i in new_cols:
    emp_df = emp_df.withColumn(i, regexp_replace(i,"\'",""))

emp_df.printSchema()

emp_df.write.mode("overwrite").format("csv")\
    .option("header",True)\
    .save("./output/hist_employee.csv")

emp_df.show()
#sys.stdin.readline()
spark.stop()