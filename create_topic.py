#!/usr/bin/env python3
"""
create_topic.py
───────────────
CLI tool to register a topic in the broker's MinIO-backed topic config.

Usage:
    python3 create_topic.py <topic_name> <num_partitions> [replication_factor]

Examples:
    python3 create_topic.py orders 3
    python3 create_topic.py events 6 1
    python3 create_topic.py --list

Why this exists
───────────────
In real Kafka you would run:
    kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 1

That command hits the ZooKeeper/KRaft controller which stores the topic
config and broadcasts it to all brokers via metadata replication.

We do the equivalent by writing a single JSON object to MinIO at:
    __topic_config/topics.json

Every broker reads this file on startup (and caches it in memory), so any
topic registered here will appear correctly in Metadata responses.
"""

import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-7s  %(message)s",
)


def main() -> None:
    # Inject the project path so we can import storage directly.
    import os
    sys.path.insert(0, os.path.dirname(__file__))

    from storage import get_topic_config, create_topic

    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    if args[0] == "--list":
        config = get_topic_config()
        if not config:
            print("No topics registered yet.")
        else:
            print(f"{'Topic':<30}  {'Partitions':>10}  {'Repl.Factor':>12}")
            print("-" * 56)
            for name, cfg in sorted(config.items()):
                print(f"{name:<30}  {cfg['partitions']:>10}  "
                      f"{cfg.get('replication_factor', 1):>12}")
        sys.exit(0)

    if len(args) < 2:
        print("Error: topic name and partition count required.", file=sys.stderr)
        print("Usage: python3 create_topic.py <name> <partitions> [repl_factor]",
              file=sys.stderr)
        sys.exit(1)

    topic_name       = args[0]
    num_partitions   = int(args[1])
    replication_factor = int(args[2]) if len(args) > 2 else 1

    if num_partitions < 1:
        print("Error: partition count must be >= 1", file=sys.stderr)
        sys.exit(1)

    try:
        create_topic(topic_name, num_partitions, replication_factor)
        print(f"✓ Topic {topic_name!r} created: "
              f"{num_partitions} partition(s), replication_factor={replication_factor}")
    except ValueError as exc:
        print(f"✗ {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
