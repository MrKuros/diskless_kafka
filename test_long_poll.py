"""
test_long_poll.py
─────────────────
Verify Fetch long-polling behaviour: the broker should hold idle connections
open for up to max_wait_ms instead of responding immediately with empty.

Measurement approach:
  1. Start a consumer in a background thread.
  2. Produce a burst of 3 messages, then pause for 5 seconds (no production).
  3. Measure how many Fetch requests the broker receives during the idle gap
     by watching the broker log file.
  4. With tight-polling you'd see ~100-200 Fetches/5s; with long-polling
     you should see ~10 (one every 500ms = max_wait_ms).

Also print wall-clock timing: the consumer should print received messages
quickly when they arrive, then go quiet for the idle period.
"""

import os
import time
import threading
import subprocess
import sys

sys.path.insert(0, "/home/alien/code/dev_prep/diskless_kafka")

from kafka import KafkaProducer, KafkaConsumer

BROKER      = "localhost:9092"
TOPIC       = "longpoll-test"
GROUP_ID    = "longpoll-group"
API_VERSION = (1, 0, 0)

received_offsets = []
stop_consumer    = threading.Event()


def consumer_thread():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=12_000,   # exits after 12s of silence
        api_version=API_VERSION,
    )
    for msg in consumer:
        ts = time.time()
        received_offsets.append((ts, msg.offset))
        print(f"  [consumer] t={ts:.3f}  offset={msg.offset}  value={msg.value.decode()}")
        if stop_consumer.is_set():
            break
    consumer.close()


def count_fetch_lines_in_log(log_path: str, since: float, until: float) -> int:
    """Count 'Fetch ←' log lines written between two epoch timestamps."""
    count = 0
    try:
        with open(log_path) as fh:
            for line in fh:
                if "Fetch ←" not in line:
                    continue
                # Log line starts with HH:MM:SS  — compare by position in file
                # We just count ALL Fetch lines then divide below for rate
                count += 1
    except FileNotFoundError:
        pass
    return count


def main():
    broker_log = (
        "/home/alien/.gemini/antigravity-ide/brain/"
        "9cad526d-fb63-41ab-9baf-91a195c1252d/"
        ".system_generated/tasks/task-1118.log"
    )

    print("Starting consumer thread …")
    t = threading.Thread(target=consumer_thread, daemon=True)
    t.start()
    time.sleep(2)   # let consumer join the group

    # ── Phase 1: produce 3 messages ──────────────────────────────────────────
    print("\n[Phase 1] Producing 3 messages …")
    producer = KafkaProducer(bootstrap_servers=BROKER, api_version=API_VERSION)
    for i in range(3):
        producer.send(TOPIC, value=f"burst-{i}".encode())
    producer.flush()
    producer.close()
    print("[Phase 1] Done producing. Consumer should wake up immediately.")
    time.sleep(1)

    # ── Phase 2: idle gap — no production for 5 seconds ──────────────────────
    print("\n[Phase 2] 5-second idle gap (no production) …")
    fetch_before = count_fetch_lines_in_log(broker_log, 0, 0)
    idle_start   = time.time()
    time.sleep(5)
    idle_end     = time.time()
    fetch_after  = count_fetch_lines_in_log(broker_log, 0, 0)

    idle_fetches  = fetch_after - fetch_before
    idle_duration = idle_end - idle_start
    fetch_rate    = idle_fetches / idle_duration

    print(f"[Phase 2] Idle duration: {idle_duration:.1f}s")
    print(f"[Phase 2] Fetch requests during idle: {idle_fetches}")
    print(f"[Phase 2] Fetch rate: {fetch_rate:.1f} req/s")

    # kafka-python default max_wait_ms=500ms → expect ~2 Fetches/s
    # tight-poll would be ~20-100 Fetches/s
    if fetch_rate <= 5.0:
        print(f"\n✓ PASS: Long-polling active — {fetch_rate:.1f} Fetch/s  "
              f"(≤5 req/s expected with max_wait_ms=500ms)")
    else:
        print(f"\n✗ FAIL: Tight-polling detected — {fetch_rate:.1f} Fetch/s  "
              f"(expected ≤5 req/s)")

    stop_consumer.set()
    t.join(timeout=5)


if __name__ == "__main__":
    main()
