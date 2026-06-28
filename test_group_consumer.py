"""
test_group_consumer.py
─────────────────────
Verify that a kafka-python consumer with a group_id can connect
without a CoordinatorNotAvailable error.

We don't need to read any messages — we just want to confirm the
FindCoordinator handshake succeeds and the consumer sits idle
waiting for records (instead of crashing).
"""
import logging
import sys
from kafka import KafkaProducer, KafkaConsumer

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(message)s")

BOOTSTRAP = "localhost:9092"
TOPIC     = "test-fetch-topic"
GROUP_ID  = "my-test-group"

# 1. Ensure there's at least one message in the topic so the consumer
#    has something to seek to.
print("1. Sending a test message …")
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
producer.send(TOPIC, value=b"hello from group consumer test!")
producer.flush()
producer.close()
print("   Message sent.\n")

# 2. Create a consumer WITH group_id (triggers FindCoordinator flow).
print(f"2. Starting consumer with group_id={GROUP_ID!r} …")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000,   # stop after 5 s if nothing arrives
)
print("   Consumer created. Polling for up to 5 seconds …\n")

received = []
for msg in consumer:
    received.append(msg)
    print(f"   ✓ Received: topic={msg.topic} partition={msg.partition} "
          f"offset={msg.offset} value={msg.value!r}")
    break   # just need one

consumer.close()

if received:
    print("\n✓ End-to-end with consumer group PASSED!")
else:
    print("\n✗ No message received (timeout) — check broker logs.")
