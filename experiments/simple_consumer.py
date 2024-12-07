from kafka import KafkaProducer, KafkaConsumer

# Connect to Kafka broker
producer = KafkaProducer(bootstrap_servers='localhost:9092')
print('Connected to Kafka broker')
# Produce a message
producer.send('test-topic', value=b'Hello Kafka')
print('Message sent to Kafka')
# Connect to Kafka consumer
consumer = KafkaConsumer('test-topic', bootstrap_servers='localhost:9092')
print('Connected to Kafka consumer')
# Consume messages
producer.send('my_topic', value=b'Hello Kafka')

for msg in consumer:
    print(msg.value)