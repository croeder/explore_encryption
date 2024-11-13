#!/usr/bin/env python3


from pyspark.sql import SparkSession

print("PySpark SQL")

spark = SparkSession.builder \
    .master("local") \
    .appName('learn_spark') \
    .getOrCreate()

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
#spark.sql("DROP TABLE if exists learn_spark_db.strings")
csv_file = "strings.csv"
schema=" test_string STRING, foo STRING"
strings_df = spark.read.csv(csv_file, schema=schema)
strings_df.write.mode("overwrite").saveAsTable("strings")

strings_df = spark.sql("""
select test_string,
       sha2(test_string, 256) as sha2_val, 
       sha2(encode(test_string, 'UTF-8'), 256) as utf8_sha2_val
FROM strings
""") 
strings_df.show(truncate=False, vertical=True);


spark.stop()
