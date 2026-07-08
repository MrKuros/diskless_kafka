# Diskless Kafka

A Kafka-protocol-compatible message broker where brokers write nothing to local disk. Every message segment is flushed directly to object storage (MinIO/S3), and cluster coordination is managed via PostgreSQL. Standard `kafka-python` producers and consumers connect without modification.

### Why this is interesting
* **Standard Kafka clients work unmodified**: You don't need to adopt a new SDK; existing Kafka integrations work out of the box.
* **Kill any broker, restart anywhere, zero data loss**: Because brokers are entirely stateless compute nodes, they can crash or be rescheduled instantly without rebalancing terabytes of local disk data.
* **The future of Apache Kafka**: This architecture cleanly separates compute from storage, proving out the design that [KIP-1150](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150%3A+Write+Kafka+Data+Directly+to+S3) and vendors like WarpStream are bringing to the Kafka ecosystem.

## Quickstart

Start the infrastructure (MinIO, Postgres, and two stateless brokers):
```bash
docker compose up -d
```

Produce 1000 messages using the standard Kafka client:
```bash
python producer.py
# Connecting to diskless Kafka broker on localhost:9092...
# Producing 1000 messages to 'demo-topic'...
# Successfully produced 1000 messages in 1.42 seconds.
```

Read them back:
```bash
python consumer.py
# Connecting to diskless Kafka broker on localhost:9092, consuming 'demo-topic'...
# Received msg: Hello diskless Kafka! msg_id=0 (offset=0, partition=0)
# Received msg: Hello diskless Kafka! msg_id=250 (offset=250, partition=0)
# Consumer finished reading. Total messages consumed: 1000
```

Because brokers are stateless, you can kill one mid-stream:
```bash
docker compose kill broker-2
```
Producers will transparently retry, the surviving broker will instantly claim the orphaned partitions via PostgreSQL, and consumers will keep reading without dropping a single message.

### See it in action

We have recorded this exact failover sequence. Watch the clients seamlessly recover from a broker crash in real time:

![Failover Demo](demo.gif)

*(You can also replay the terminal session directly in your console by running `asciinema play demo.cast`)*

## Performance Benchmarks

Since brokers stream data directly to/from S3, performance characteristics are fundamentally different from traditional disk-based Kafka.

**Throughput (1 Broker)**
| Message Size | Produce Throughput | Consume Throughput | S3 Cost Amortization |
|--------------|--------------------|--------------------|----------------------|
| **100 B**    | 12,875 msgs/s      | 70,527 msgs/s      | High (many PUTs)     |
| **1 KB**     | 2,238 msgs/s       | 66,367 msgs/s      | Moderate             |
| **10 KB**    | 244 msgs/s         | 7,759 msgs/s (75 MB/s) | Low (S3 fetch is fast) |

*For deep analysis of these numbers, see the architecture document.*

## Architecture & Internals

For a deep dive into how the stateless broker design handles partition leadership, storage layer prefixing, and fetch long-polling, see [ARCHITECTURE.md](ARCHITECTURE.md).

## What is NOT Implemented
* **Replication / ISRs**: S3 handles replication natively, but if the S3 bucket is lost, data is lost.
* **Log Compaction**: Requires a background job to deduplicate S3 objects.
* **Transactions / Exactly-Once Semantics**.
* **SASL / TLS Authentication**.
* **Specific APIs**: `CreateTopics`, `DeleteTopics`, `DescribeGroups`. Topics are auto-created on first produce.
