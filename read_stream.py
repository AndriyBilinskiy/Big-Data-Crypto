import json
import ssl
import threading
import websocket
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
from config import pairs, ws_url

producer = KafkaProducer(bootstrap_servers='localhost:9092')


def on_message(ws, message):
    data = json.loads(message)

    # Rename the fields
    renamed_data = {
        "event_type": data.get("e"),
        "event_time": data.get("E"),
        "symbol": data.get("s"),
        "trade_id": data.get("t"),
        "price": data.get("p"),
        "quantity": data.get("q"),
        "trade_time": data.get("T"),
        "is_market_maker": data.get("m"),
        "ignore": data.get("M")
    }
    renamed_message = json.dumps(renamed_data)
    producer.send('websocket_topic', value=renamed_message.encode('utf-8'))
    producer.flush()


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed with code: {close_status_code}, message: {close_msg}")


def on_open(ws):
    print("WebSocket connection opened")
    subscribe_message = json.dumps({
        "method": "SUBSCRIBE",
        "params": pairs,
        "id": 1
    })
    ws.send(subscribe_message)


def start_websocket():
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})


def run_ws_to_kafka():
    # Run WebSocket in a separate thread to keep it running alongside Spark
    threading.Thread(target=start_websocket).start()


# Start the WebSocket consumer
run_ws_to_kafka()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WebSocket Stream Example") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.1.0") \
    .getOrCreate()

# Read data from Kafka topic
# Spark reads data from Kafka in a streaming context
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "websocket_topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# The Kafka messages are in byte format; decode them into a string and parse JSON
json_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as message")

schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_time", LongType(), True),
    StructField("symbol", StringType(), True),
    StructField("trade_id", LongType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("trade_time", LongType(), True),
    StructField("is_market_maker", BooleanType(), True),
    StructField("ignore", BooleanType(), True)
])

# Parse the JSON message using the schema
parsed_df = json_df.withColumn("jsonData", from_json(col("message"), schema)) \
    .select("jsonData.*")

# Display the streaming DataFrame
# change to delta to write to Delta Lake
query = parsed_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data") \
    .option("checkpointLocation", "data/_checkpoints") \
    .start()

# Await termination of the stream
query.awaitTermination()
