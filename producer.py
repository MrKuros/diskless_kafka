#!/usr/bin/env python3
"""
producer.py
───────────
A simple example script demonstrating how to produce messages
to the diskless Kafka broker using the standard kafka-python client.
"""
import time
from kafka import KafkaProducer

def main():
    print("Connecting to diskless Kafka broker on localhost:9092...")
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        api_version=(1, 0, 0),
        # Optimize for S3 latency with aggressive batching
        linger_ms=100,
        batch_size=1048576,
    )
    
    topic = "demo-topic"
    count = 1000
    
    print(f"Producing {count} messages to '{topic}'...")
    start_time = time.time()
    
    for i in range(count):
        producer.send(topic, f"Hello diskless Kafka! msg_id={i}".encode())
        if i > 0 and i % 250 == 0:
            print(f"  Sent {i} messages...")
            
    producer.flush()
    elapsed = time.time() - start_time
    
    print(f"Successfully produced {count} messages in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    main()
