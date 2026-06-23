import logging
from kafka import KafkaProducer, KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

print("1. Sending message to test-fetch-topic...")
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
)

producer.send('test-fetch-topic', b'hello from consumer test!')
producer.flush()
print("Message sent.\n")

print("2. Starting consumer...")
consumer = KafkaConsumer(
    'test-fetch-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000,
)

print("Waiting for messages...")
for msg in consumer:
    print(f"\n✓ Received message!")
    print(f"  topic={msg.topic} partition={msg.partition} offset={msg.offset}")
    print(f"  value={msg.value.decode('utf-8')}")
    break
