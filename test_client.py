"""
diskless_kafka/test_client.py
──────────────────────────────
Day 6: Confirms that:
  - The produce request is written to MinIO as a .batch object.
  - The server sends back a proper Produce response (error_code=0).
  - kafka-python receives the ack and future.get() returns record metadata.

Expected outcome:
  Step 1: Producer created   →  ApiVersions + Metadata OK
  Step 2: send() + flush()   →  ack received, offset printed
  Step 3: close

Run MinIO first, then server.py, then this script.
"""

import logging
import sys

# Show only WARNING+ from kafka internals so we can see the important errors.
# Change to logging.DEBUG to see every kafka-python internal step.
logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)-7s %(name)s: %(message)s",
    stream=sys.stdout,
)

try:
    from kafka import KafkaProducer
    from kafka.errors import (
        KafkaError,
        NoBrokersAvailable,
        LeaderNotAvailableError,
    )
except ImportError:
    sys.exit(
        "kafka-python is not installed.\n"
        "Run:  pip install kafka-python-ng\n"
        "Then retry this script."
    )


TOPIC   = "test-topic"
MESSAGE = b"hello from diskless-kafka day 6 -- stored in MinIO!"


def main() -> None:
    print("─" * 60)
    print("Step 1: creating KafkaProducer (triggers ApiVersions + Metadata) …")

    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            request_timeout_ms=3_000,
            connections_max_idle_ms=10_000,
            reconnect_backoff_ms=200,
            reconnect_backoff_max_ms=1_000,
            retries=0,
        )
    except NoBrokersAvailable:
        print("FAIL — NoBrokersAvailable: ApiVersions or Metadata not handled yet.")
        return
    except Exception as exc:
        print(f"FAIL — unexpected error during connect: {exc!r}")
        return

    print("  ✓  Producer created — ApiVersions + Metadata both answered.")
    print()

    # ── Step 2: try to send a message ────────────────────────────────────────
    # This causes kafka-python to send a topic-specific Metadata request
    # (with the topic name) so it can find the partition leader, then send
    # a Produce request to that leader.
    #
    # Expected outcome after Day 4:
    #   - No NoBrokersAvailable      (fixed in Day 3 — ApiVersions)
    #   - No LeaderNotAvailableError (fixed today  — Metadata)
    #   - RequestTimedOutError on Produce (expected — Day 5 work)
    print(f"Step 2: sending one message to topic '{TOPIC}' …")

    try:
        future = producer.send(TOPIC, value=MESSAGE)
        # flush() blocks until the Produce response is received.
        # With Day 6, the server acks immediately after MinIO write.
        producer.flush(timeout=10)
        record_metadata = future.get(timeout=10)
        print(f"  ✓  Message sent!  partition={record_metadata.partition}  "
              f"offset={record_metadata.offset}")
        print(f"  ✓  Check MinIO console (localhost:9001) for object:")
        print(f"      diskless-kafka / {TOPIC}/{record_metadata.partition}/"
              f"{record_metadata.offset:020d}.batch")
    except Exception as exc:
        # RequestTimedOutError here means MinIO write or response failed.
        print(f"  FAIL  Produce raised: {type(exc).__name__}: {exc}")

    print()
    print("Step 3: closing producer …")
    try:
        producer.close(timeout=1)
    except Exception:
        pass
    print("  done.")
    print("─" * 60)


if __name__ == "__main__":
    main()
