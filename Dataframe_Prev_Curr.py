from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf = SparkConf()
spark_conf.set("spark.app.name","dataframe_prev_curr")
spark_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

data = [(1,'James','1981-04-01'),
        (1,'Michael','1991-05-19'),
        (2,'Robert','1978-09-05'),
        (2,'Maria','1979-12-01'),
        (3,'Jen','1990-02-17'),
        (4,'Mary','1981-05-17'),
        (4,'Brown','1990-02-17'),
        (1,'Rose','2000-05-19'),
        (5,'Robert','2004-09-05'),
        (5,'Anne','1996-12-01'),
        (6,'Mary','1980-11-17',),
        (6,'Williams','1991-02-17',),
        (7,'Jones','1967-01-01')
        ]

columns = ["cmp_id","director","date"]

df = spark.createDataFrame(data=data,schema=columns)
df.createOrReplaceTempView("tmp_tbl")

df2 = spark.sql("select cmp_id,director, date,"
                " row_number() over (partition by cmp_id order by date desc) as rnk "
                "from tmp_tbl order by cmp_id")
df2.show()

df2.createOrReplaceTempView("tmp_tbl_2")
df3 = spark.sql("select cmp_id, case when rnk = 1 then director end as cur_director,"
          "case when rnk = 2 then director end as prev_director,"
          " date from tmp_tbl_2")
df3.show()

df3.createOrReplaceTempView("tmp_tbl_3")
df4 = spark.sql("select cmp_id, max(cur_director) as cur_director,"
                "max(prev_director) as prev_director "
                "from tmp_tbl_3 group by cmp_id")

df4.show()