#!/usr/bin/env python3
"""CLI to register a topic in the broker's object-store topic config.

Usage:
    python3 create_topic.py <topic_name> <num_partitions> [replication_factor]
    python3 create_topic.py --list

In real Kafka this is `kafka-topics.sh --create`, which writes to the
ZooKeeper/KRaft controller.  Here it writes a single JSON object to the object
store (`__topic_config/topics.json`); every broker reads it on startup and
refreshes on a short cache.
"""

import logging
import os
import sys

# Run from anywhere: put the repo root (parent of examples/) on the import path.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Settings
from storage import ObjectStore

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s  %(message)s")


def main() -> None:
    args = sys.argv[1:]
    store = ObjectStore(Settings.from_env())

    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        return

    if args[0] == "--list":
        config = store.get_topic_config()
        if not config:
            print("No topics registered yet.")
            return
        print(f"{'Topic':<30}  {'Partitions':>10}  {'Repl.Factor':>12}")
        print("-" * 56)
        for name, cfg in sorted(config.items()):
            print(f"{name:<30}  {cfg['partitions']:>10}  {cfg.get('replication_factor', 1):>12}")
        return

    if len(args) < 2:
        sys.exit("Usage: create_topic.py <name> <partitions> [repl_factor]")

    topic_name = args[0]
    num_partitions = int(args[1])
    replication_factor = int(args[2]) if len(args) > 2 else 1
    if num_partitions < 1:
        sys.exit("Error: partition count must be >= 1")

    try:
        store.create_topic(topic_name, num_partitions, replication_factor)
        print(f"✓ Topic {topic_name!r} created: {num_partitions} partition(s), "
              f"replication_factor={replication_factor}")
    except ValueError as exc:
        sys.exit(f"✗ {exc}")


if __name__ == "__main__":
    main()
