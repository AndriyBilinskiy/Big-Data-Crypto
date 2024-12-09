from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read Parquet Data") \
    .getOrCreate()

# Read the Parquet data
parquet_df = spark.read.parquet("data")

# Show the data
parquet_df.show()
print(parquet_df.count())