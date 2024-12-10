from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder \
    .appName("Read Parquet Data") \
    .getOrCreate()

parquet_df = spark.read.parquet("data/part-*.parquet")

pdf = parquet_df.toPandas()

pdf['event_time'] = pd.to_datetime(pdf['event_time'], unit='ms')
pdf['quantity'] = pd.to_numeric(pdf['quantity'], errors='coerce')
pdf['price'] = pd.to_numeric(pdf['price'], errors='coerce')
print(pdf['event_time'].head())
pdf = pdf.dropna(subset=['quantity', 'price'])

#Change to preferred cryptocurrency
crypto_symbol = "BTCUSDT"
filtered_pdf = pdf[pdf['symbol'] == crypto_symbol]

plt.figure(figsize=(12, 6))
plt.scatter(filtered_pdf['event_time'], filtered_pdf['price'], alpha=0.7, label=f"{crypto_symbol} Price")
plt.title(f"Price Change of {crypto_symbol} Over Time")
plt.xlabel("Event Time")
plt.ylabel("Price")
plt.grid(True)
plt.xticks(rotation=45)
plt.legend()
plt.show()