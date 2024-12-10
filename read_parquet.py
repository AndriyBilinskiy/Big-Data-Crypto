from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Parquet Data") \
    .getOrCreate()

parquet_df = spark.read.parquet("data/part-*.parquet")
parquet_df.show()
print(parquet_df.count())