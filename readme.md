
This project streams real-time cryptocurrency trade data from Binance WebSocket API, processes it using Apache Spark, and stores it in Parquet format.

## Features

- **WebSocket Streaming**: Connects to Binance WebSocket API to receive real-time trade data.
- **Kafka Integration**: Streams data to Kafka for reliable message delivery.
- **Spark Processing**: Uses Apache Spark for real-time data processing.
- **Parquet Storage**: Stores processed data in Parquet format for efficient querying.

## Getting Started

### Prerequisites

- Python 3.11
- Apache Spark 3.4.0
- Kafka
- PySpark
- WebSocket-client
- Kafka-Python

### Installation

1. Create docker container for Kafka and Zookeeper:
    ```sh
    docker-compose up -d
    ```

2. Install dependencies:
    ```sh
    pip install -r requirements.txt
    ```

### Running the Application

1. Start the WebSocket to Parquet streamer:
    ```sh
    python read_stream.py
    ```

2. Read and display the Parquet data:
    ```sh
    python read_parquet.py
    ```

### Telegram Bot:

1. First of all, you need to start bot:
   ```shell
   python tg_crypto_bot.py
   ```
   
2. Start saving data to file:
   ```shell
   python read_stream.py
   ```
   
3. After this use this [bot](https://t.me/cryptoproject_news_bot)


## Configuration

Edit `config.py` to change the WebSocket URL or the list of trading pairs.
Raw trade response example
{

}# Real-Time Cryptocurrency Trade Streamer

## Example Trade Data

```json
{
  "e": "trade",       // Event type
  "E": 1672515782136, // Event time
  "s": "BNBBTC",      // Symbol
  "t": 12345,         // Trade ID
  "p": "0.001",       // Price
  "q": "100",         // Quantity
  "T": 1672515782136, // Trade time
  "m": true,          // Is the buyer the market maker?
  "M": true           // Ignore
}