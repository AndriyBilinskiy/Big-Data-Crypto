import json
import ssl
import threading
import websocket
from kafka import KafkaProducer
from pyspark.sql import SparkSession

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')


# Function to handle WebSocket messages and send them to Kafka
def on_message(ws, message):
    # Assuming the message is a JSON string containing timestamp, price, and quantity
    data = json.loads(message)
    producer.send('websocket_topic', value=message.encode('utf-8'))
    producer.flush()


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed with code: {close_status_code}, message: {close_msg}")


def on_open(ws):
    print("WebSocket connection opened")


def start_websocket():
    ws_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
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
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Read data from Kafka topic
# Spark reads data from Kafka in a streaming context
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "websocket_topic") \
    .load()

# The Kafka messages are in byte format; decode them into a string and parse JSON
json_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as message")

# Parse the JSON message (assuming message has 'timestamp', 'price', 'quantity')
# parsed_df = json_df.select(
#     col("message"),
#     json.loads(col("message"))["timestamp"].alias("timestamp"),
#     json.loads(col("message"))["price"].alias("price"),
#     json.loads(col("message"))["quantity"].alias("quantity")
# )
# parsed_df.show()

# Display the streaming DataFrame
query = json_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination of the stream
query.awaitTermination()
