import datetime
import sys
from dateutil.relativedelta import relativedelta
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "scd types")
spark_conf.set("spark.master", "local[2]")

def get_df_cols_list(df):
    return [field.name for field in df.schema]


spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

source_emp_df = spark.read.format("csv")\
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
print(get_df_cols_list(df=source_emp_df))

old_cols = source_emp_df.columns
new_cols = [col.strip().replace("\'","") for col in old_cols]
#print(type(i) for i in new_cols )


source_emp_df = source_emp_df.toDF(*new_cols)

for i in new_cols:
    source_emp_df = source_emp_df.withColumn(i, regexp_replace(i,"\'",""))

dt = datetime.datetime.now().date()
dt =  dt - relativedelta(days=0)

source_emp_df = source_emp_df.withColumn("start_date", lit(dt) )\
    .withColumn("end_date", to_date(lit("2999-12-31"), 'yyyy-MM-dd'))

hist_emp_df = spark.read.format("parquet")\
    .option("path","./output/parquet/hist_employee")\
    .load()


hist_emp_df = hist_emp_df.filter("active_flag = 'Y'")

source_emp_df.printSchema()
hist_emp_df.printSchema()
#source_emp_df.show()
#hist_emp_df.show()

target_cols = source_emp_df.columns
primary_cols = ['employeeid']

# 1. Identify New Rows in the Source dataframe

new_records_df = source_emp_df.alias('s')\
    .join(hist_emp_df.alias('t'), on=primary_cols, how= "leftanti")\
    .withColumn("active_flag", lit('Y'))\
    .withColumn("start_date", lit(dt) )\
    .withColumn("end_date", to_date(lit("2999-12-31"), 'yyyy-MM-dd'))\
    .select("s.EmployeeID","s.FirstName", "s.LastName",
            "s.Department","s.Salary","start_date", "end_date", "active_flag")

cols_to_hash = ["FirstName", "LastName", "Department","Salary"]

def create_hash_cols(df, cols_to_hash):
    return df.withColumn("hash_value", md5(concat_ws('||',*[col(c) for c in cols_to_hash])))

hashed_source_df = create_hash_cols(df=source_emp_df, cols_to_hash=cols_to_hash)
hashed_target_df = create_hash_cols(df=hist_emp_df, cols_to_hash=cols_to_hash)

#hashed_source_df.printSchema()
#hashed_target_df.printSchema()


# Find the records with the changes
updated_recs_df = hashed_source_df.join(hashed_target_df, on=primary_cols, how="inner")\
    .filter(hashed_source_df["hash_value"] != hashed_target_df["hash_value"])\
    .select(hashed_source_df["EmployeeID"],
            hashed_source_df["FirstName"],
            hashed_source_df["lastName"],
            hashed_source_df["Department"],
            hashed_source_df["Salary"])


update_expired_recs_df = hashed_target_df.alias('t')\
    .join(updated_recs_df.alias('up'), on=primary_cols, how="inner")\
    .withColumn("end_date", lit(dt))\
    .withColumn("active_flag", lit('N'))\
    .select("t.EmployeeID", "t.FirstName","t.lastName",
            "t.Department","t.Salary","t.start_date","end_date", "active_flag")

updated_records_from_src_df = hashed_source_df.alias('s')\
    .join(updated_recs_df.alias('up'), on=primary_cols, how='inner')\
    .withColumn("start_date", lit(dt))\
    .withColumn("end_date", to_date(lit("2999-12-31"), 'yyyy-MM-dd'))\
    .withColumn("active_flag", lit('Y'))            \
    .select("s.EmployeeId","s.FirstName","s.LastName", "s.Department","s.Salary","start_date",
            "end_date",
            "active_flag")


# No Change records

no_changes_df = hashed_source_df.join(hashed_target_df, on=primary_cols, how="inner")\
    .filter(hashed_source_df["hash_value"] == hashed_target_df["hash_value"])\
    .select(hashed_target_df["EmployeeID"],
            hashed_target_df["FirstName"],
            hashed_target_df["LastName"],
            hashed_target_df["Department"],
            hashed_target_df["Salary"],
            hashed_target_df["start_date"],
            hashed_target_df["end_date"],
            hashed_target_df["active_flag"]
            )

#Union all the dfs
# new insert df
# updated_records_from_src df
# updated expired records df
# unchanged records df
final_df = new_records_df.union(updated_records_from_src_df)\
    .union(update_expired_recs_df)\
    .union(no_changes_df)

final_df.orderBy(col("EmployeeID").cast("Int")).show(100)


final_df.write.mode("overwrite")\
    .partitionBy("department","active_flag")\
    .option("path","./output/parquet/tmp/hst_employee")\
    .format("parquet")\
    .save()


tmp_df = spark.read.format("parquet")\
    .option("path","./output/parquet/tmp/hst_employee")\
    .load()

tmp_df.write.mode("overwrite").partitionBy("department","active_flag")\
    .option("path", "./output/parquet/hst_employee")\
    .format("parquet")\
    .save()

sys.stdin.readline()
spark.stop()

