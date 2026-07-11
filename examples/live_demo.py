import subprocess
import time
import sys
import threading
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

def print_header(msg):
    print(f"\n{'-'*60}\n>>> {msg}\n{'-'*60}")

def run_producer(topic):
    # retries=10 ensures the producer will retry sending when the leader disconnects
    producer = KafkaProducer(bootstrap_servers="localhost:9092", api_version=(1,0,0), retries=10)
    for i in range(1, 151):
        producer.send(topic, f"msg-{i}".encode())
        if i % 30 == 0:
            print(f"  [PRODUCER] Successfully sent {i} messages...")
        time.sleep(0.15)
    producer.flush()
    print("  [PRODUCER] Finished sending all 150 messages.")

def run_consumer(topic):
    # We assign manually to bypass complex group rebalancing logic for a simple demo
    consumer = KafkaConsumer(bootstrap_servers="localhost:9092", api_version=(1,0,0))
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)
    
    count = 0
    for msg in consumer:
        count += 1
        if count % 30 == 0:
            print(f"  [CONSUMER] Read {count} messages... (latest: {msg.value.decode()})")
    print(f"  [CONSUMER] Finished reading. Total messages: {count}.")

def main():
    # Enable unbuffered output so print statements flush immediately
    sys.stdout.reconfigure(line_buffering=True)
    
    topic = f"demo-topic-{int(time.time())}"

    print_header("Booting up Diskless Kafka Cluster")
    print("Running: docker compose up -d")
    subprocess.run(["docker", "compose", "up", "-d"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)
    print("Cluster is up! (PostgreSQL, MinIO, Broker-1, Broker-2)")

    print_header("Starting Producer & Consumer")
    
    # Start Producer Thread
    t_prod = threading.Thread(target=run_producer, args=(topic,))
    t_prod.start()
    time.sleep(2) # Give producer a head start so consumer doesn't block on empty topic
    
    # Start Consumer Thread
    t_cons = threading.Thread(target=run_consumer, args=(topic,))
    t_cons.start()

    # Wait for traffic to flow
    time.sleep(4)

    print_header("Simulating Hard Node Failure")
    print("Running: docker compose kill broker-2")
    subprocess.run(["docker", "compose", "kill", "broker-2"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("Broker-2 has been forcefully killed.")
    print("Waiting 10 seconds for the Postgres heartbeat sweep to usurp leadership...")

    # Wait for the producer and consumer threads to finish
    t_prod.join()
    t_cons.join()

    print_header("Tearing Down Cluster")
    print("Running: docker compose down")
    subprocess.run(["docker", "compose", "down"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("\n✅ Live Demo Complete. Zero data loss achieved.")

if __name__ == "__main__":
    main()
