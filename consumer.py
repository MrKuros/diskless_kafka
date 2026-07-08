#!/usr/bin/env python3
"""
consumer.py
───────────
A simple example script demonstrating how to consume messages
from the diskless Kafka broker using the standard kafka-python client.
"""
from kafka import KafkaConsumer

def main():
    topic = "demo-topic"
    print(f"Connecting to diskless Kafka broker on localhost:9092, consuming '{topic}'...")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        api_version=(1, 0, 0),
        auto_offset_reset="earliest",
        group_id="demo-consumer-group",
        # Optimize for large S3 fetches
        fetch_max_bytes=52428800,
        max_partition_fetch_bytes=10485760,
        consumer_timeout_ms=5000,
    )
    
    count = 0
    for msg in consumer:
        count += 1
        if count % 250 == 0 or count == 1:
            print(f"Received msg: {msg.value.decode()} (offset={msg.offset}, partition={msg.partition})")
            
    print(f"Consumer finished reading. Total messages consumed: {count}")

if __name__ == "__main__":
    main()
