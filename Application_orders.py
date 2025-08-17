import os
import sys
from os import truncate

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct

from Dataframe_grouping import orders_df


class Application_orders:

    def __int__(self):
        pass

    def spark_session(self):
        spark  = SparkSession\
        .builder\
        .appName("orders")\
        .getOrCreate()
        return  spark

    def orders_df(self, spark):
        if len(sys.argv) > 1:
            input_path = sys.argv[1]
            output_path = sys.argv[2]
        else:
            print("S3 output location not specified")

        region = os.getenv("AWS_REGION")
        orders_df = spark.read.format("csv")\
        .option("inferSchema", True)\
        .option("header", True)\
        .load(input_path)

        #Aggregate data
        status_counts = orders_df.groupBy("order_status").count()

        #show aggregated data
        status_counts.show(truncate=False)

        if output_path:
            status_counts.write.mode("overwrite").parquet(output_path)
            print("orders count job completed successfully, refer S3 output path: ",output_path)
        else:
            status_counts.show(10, False)
            print("orders count job completed successfully")

        spark.stop()


app = Application_orders()
app.orders_df(spark=app.spark_session())
