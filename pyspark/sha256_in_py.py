#!/usr/bin/env python3

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

print("PySpark Python")

spark = SparkSession.builder \
    .master("local") \
    .appName('learn_spark') \
    .getOrCreate()

csv_file = "strings.csv"
df = spark.read.csv(csv_file)
df = df.withColumn('sha2_val', F.sha2(df['_c0'].cast(T.StringType()), 256))
df.show(truncate=False, vertical=True);


spark.stop()
