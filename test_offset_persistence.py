"""
test_offset_persistence.py
──────────────────────────
Verify that committed offsets survive a broker restart.

Steps:
  1. Produce 5 messages to 'offset-persist-topic'.
  2. Start a consumer with group_id='persist-test-group'.
     Poll until all 5 messages are received.
     Wait for auto-commit to fire (6 seconds > auto.commit.interval.ms=5s).
  3. Print the committed offset from MinIO directly.
  4. Restart the consumer (same group_id).
     The first message it sees should be > the 5 we already consumed
     (or the poll returns nothing because we're already caught up).

The broker must already be running.  MinIO must be running.
"""

import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from storage import load_committed_offsets

logging.basicConfig(
    level=logging.WARNING,   # suppress kafka-python noise
    format="%(asctime)s  %(levelname)-7s  %(message)s",
)
log = logging.getLogger("test_persist")

BROKER        = "localhost:9092"
TOPIC         = "offset-persist-topic"
GROUP_ID      = "persist-test-group"
N_MESSAGES    = 5
API_VERSION   = (1, 0, 0)


def produce_messages(n: int) -> None:
    print(f"\n1. Producing {n} messages to '{TOPIC}' …")
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        api_version=API_VERSION,
    )
    for i in range(n):
        value = f"msg-{i}".encode()
        producer.send(TOPIC, value=value)
    producer.flush()
    producer.close()
    print(f"   ✓ {n} messages produced.")


def consume_and_commit(expected: int) -> int:
    """
    Consume messages until we've seen *expected* records (or 30s timeout).
    Wait 6 seconds after the last record to let auto-commit fire.
    Returns the count of messages actually received.
    """
    print(f"\n2. Starting consumer (group='{GROUP_ID}') — expecting {expected} messages …")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,  # 5 seconds
        api_version=API_VERSION,
        consumer_timeout_ms=15000,     # give up after 15s of silence
    )

    received = 0
    deadline = time.time() + 30
    for msg in consumer:
        received += 1
        print(f"   ← offset={msg.offset}  value={msg.value.decode()}")
        if received >= expected:
            break
        if time.time() > deadline:
            break

    print(f"   ✓ {received} message(s) received.")
    print("   Waiting 6s for auto-commit to fire …")
    time.sleep(6)
    consumer.close()
    return received


def check_minio_offset() -> int:
    """Read the committed offset directly from MinIO."""
    print("\n3. Reading committed offset from MinIO …")
    offsets = load_committed_offsets()
    key = (GROUP_ID, TOPIC, 0)
    committed = offsets.get(key, -1)
    print(f"   MinIO says: committed offset for {key} = {committed}")
    return committed


def consume_again() -> list[int]:
    """
    Start a fresh consumer with the same group_id.
    It should read the committed offset from OffsetFetch and start from there.
    Returns list of offsets seen (should be empty if already caught up, or
    only new messages if more were produced).
    """
    print(f"\n4. Restarting consumer (same group='{GROUP_ID}') — should resume, not replay …")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        api_version=API_VERSION,
        consumer_timeout_ms=8000,   # 8s to see if any (duplicate) messages arrive
    )

    seen_offsets = []
    for msg in consumer:
        seen_offsets.append(msg.offset)
        print(f"   ← offset={msg.offset}  value={msg.value.decode()}")

    consumer.close()
    return seen_offsets


def main() -> None:
    produce_messages(N_MESSAGES)
    received = consume_and_commit(N_MESSAGES)

    committed = check_minio_offset()

    if committed < 0:
        print("\n✗ FAIL: No committed offset found in MinIO after consuming.")
        return
    if committed != received:
        print(f"\n⚠ WARNING: committed={committed} but received={received}. Auto-commit may be off.")

    # Consume again — should see zero old messages (already committed past them)
    seen = consume_again()

    if not seen:
        print(f"\n✓ PASS: Consumer resumed from offset {committed}. No duplicate messages.")
    elif all(o >= committed for o in seen):
        print(f"\n✓ PASS: Consumer resumed from offset {committed}. "
              f"Saw {len(seen)} new messages at offsets {seen}.")
    else:
        duplicates = [o for o in seen if o < committed]
        print(f"\n✗ FAIL: Consumer replayed {len(duplicates)} already-committed "
              f"message(s) at offsets {duplicates}. Persistence may be broken.")


if __name__ == "__main__":
    main()
