# Diskless Kafka

A Kafka-protocol-compatible message broker where brokers write nothing to local disk. Every message segment is flushed directly to object storage (MinIO/S3), and cluster coordination is managed via PostgreSQL. Standard `kafka-python` producers and consumers connect without modification.

### The Core Insight

Kafka's most operationally painful problems — broker failover, rebalancing, storage scaling — all exist because **brokers own their data**. They hold it on local disk. Kill a broker and you need to move that disk data to someone else, and that is expensive. This project asks: what if brokers owned nothing? What if they were just routing boxes?

Brokers here are stateless networking proxies. Every message batch is flushed directly to MinIO (S3-compatible object storage). Every coordination decision goes through PostgreSQL. Standard Kafka clients connect to port 9092, think they're talking to a real Kafka cluster, and never know the difference.

### Why this is interesting
* **Standard Kafka clients work unmodified**: You don't need to adopt a new SDK; existing Kafka integrations work out of the box.
* **Kill any broker, restart anywhere, zero data loss**: Because brokers are entirely stateless, they can crash or be rescheduled without rebalancing a single byte. On restart, a broker issues one `ListObjects` call to MinIO and fully recovers its high-watermark.
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
Producers will transparently retry, the surviving broker will claim the orphaned partitions, and consumers will keep reading without dropping a single message.

**The exact failover sequence:**
1. The dead broker stops writing to `broker_health` in PostgreSQL.
2. After **10 seconds**, `usurp_dead_brokers` nulls out that broker's partition leadership rows.
3. Surviving brokers race to claim orphaned partitions with an atomic `UPDATE ... WHERE leader_id IS NULL`. PostgreSQL's ACID semantics ensure only one wins.
4. The winning broker runs `ListObjects` on MinIO to recover the high-watermark — no log replay needed.
5. Clients get `LEADER_NOT_AVAILABLE`, retry metadata, and resume against the new leader.

Total failover window: ~10–12 seconds, driven entirely by the heartbeat timeout.

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

## Consumer Group Protocol

The broker implements the full Kafka consumer group protocol so that unmodified `kafka-python` consumers work correctly:

1. **JoinGroup** — Consumers register with a group ID. The first member becomes the *leader* and receives the full member list. Followers receive an empty list.
2. **SyncGroup** — The leader runs the assignment strategy locally (e.g., RoundRobin) and sends the completed partition map back to the broker. The broker distributes each member's slice in the SyncGroup response.
3. **Heartbeat loop** — Consumers heartbeat every 3s. Heartbeats carry a `generation_id`; if it doesn't match, the broker returns `ILLEGAL_GENERATION` and the consumer rejoins. This is how rebalances are detected.

The leader does assignment rather than the broker because the broker doesn't need to know about assignment strategies. New strategies can be added client-side without any broker changes.

## Comparison to WarpStream

WarpStream and this project start from the same insight: write messages directly to S3, make brokers stateless. The data path is identical.

The difference is the metadata layer. WarpStream runs a purpose-built distributed metadata service (backed by something like etcd or DynamoDB) that's designed for multi-region, high-availability coordination. This project uses PostgreSQL — a single node, but one whose ACID semantics prevent split-brain, and whose logic you can read in eight lines of SQL.

PostgreSQL is a single point of failure; a distributed metadata store is not. That's the honest trade-off.

## Architecture & Internals

For a deep dive into how the stateless broker design handles partition leadership, storage layer prefixing, and fetch long-polling, see [ARCHITECTURE.md](ARCHITECTURE.md).

## What is NOT Implemented

| Feature | Notes |
|---|---|
| **Replication / ISRs** | Only one copy of data exists. If the MinIO bucket is lost, data is lost. Real fix: write to multiple buckets or implement ISR tracking in PostgreSQL. |
| **Log Compaction** | Requires a background job that reads `.batch` files, deduplicates by key, and rewrites survivors to MinIO. |
| **Transactions / Exactly-Once Semantics** | No two-phase commit, no producer ID tracking. |
| **SSL / SASL** | All communication is plaintext. Requires TLS termination in the asyncio server and SCRAM-SHA-256 credential handling. |
| **Distributed Metadata Store** | PostgreSQL is a single point of failure. Production equivalent: replace `db.py` with etcd or DynamoDB for multi-region coordination. |
| **CreateTopics / DeleteTopics / DescribeGroups** | Topics are auto-created on first produce. |
