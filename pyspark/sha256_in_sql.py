#!/usr/bin/env python3


from pyspark.sql import SparkSession


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

print("Databases:")
print(spark.catalog.listDatabases())
print("Tables:")
print(spark.catalog.listTables("learn_spark_db"))
print("Columns:")
print(spark.catalog.listColumns("strings"))


# plain
strings_df = spark.sql("SELECT * from strings ") 
print(strings_df.count())
strings_df.show(truncate=False, vertical=True);

# create the hash
strings_df = spark.sql("""
select test_string,
       encode(test_string, 'UTF-8'),
       sha2(test_string, 256) as sha2_val, 
       sha2(encode(test_string, 'UTF-8'), 256) as sha2_val, 
        substr(
            sha2(test_string, 256),
            length(sha2(test_string, 256)) -13 , 13) right_substr,
        cast(
           conv(
               substr(
                   sha2(test_string, 256),
                   length(sha2(test_string, 256)) -13 , 13),
            16, 10) 
        as bigint) as right_hashed_value
FROM strings
""") 
print(strings_df.count())
print(type(strings_df))
strings_df.show(truncate=False, vertical=True);


spark.stop()
