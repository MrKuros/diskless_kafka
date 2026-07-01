from kafka import KafkaProducer
import time

BROKERS = ["localhost:9092", "localhost:9093"]
TOPIC = "test-2part"

# Initialize producer. It will fetch metadata and learn about the 2 brokers and 2 partitions.
producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    api_version=(1, 0, 0),
    client_id="test-producer"
)

# Send to partition 0 (should route to broker 1)
future_0 = producer.send(TOPIC, b"Message for partition 0", partition=0)
print(f"Sent to partition 0: {future_0.get()}")

# Send to partition 1 (should route to broker 2)
future_1 = producer.send(TOPIC, b"Message for partition 1", partition=1)
print(f"Sent to partition 1: {future_1.get()}")

producer.flush()
producer.close()
