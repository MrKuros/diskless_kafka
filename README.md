# Diskless Kafka

Kafka-protocol broker that writes **nothing** to local disk:

- Message batches → object storage (MinIO/S3).
- Cluster coordination → PostgreSQL.
- Brokers are stateless routing boxes.
- Standard `kafka-python` producers/consumers connect unmodified on port 9092.

## Contents

- [The Core Insight](#the-core-insight)
- [Why this is interesting](#why-this-is-interesting)
- [Quickstart](#quickstart)
- [Failover](#failover)
- [Performance Benchmarks](#performance-benchmarks)
- [Consumer Group Protocol](#consumer-group-protocol)
- [Code Layout](#code-layout)
- [Architecture & Internals](#architecture--internals)
- [What is NOT Implemented](#what-is-not-implemented)

## The Core Insight

- Kafka's pain — failover, rebalancing, storage scaling — comes from one thing: **brokers own their data** (local disk).
- Kill a broker → move its disk data to someone else → expensive.
- This project: brokers own **nothing**.
  - Every batch → MinIO. Every coordination decision → Postgres.
  - Clients think they hit real Kafka. They can't tell.

## Why this is interesting

- **Standard clients work unmodified** — no new SDK; existing integrations just work.
- **Kill any broker, restart anywhere, zero data loss** — nothing to rebalance; restart recovers high-watermark with one `ListObjects` call.
- **Where Kafka is heading** — compute/storage split, same design as [KIP-1150](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150%3A+Write+Kafka+Data+Directly+to+S3).

## Quickstart

One command — reset, bring up MinIO + Postgres + two brokers, register topic, produce, consume:
```bash
./run.sh
```

By hand:
```bash
docker compose up -d                                             # infra
MINIO_ENDPOINT=localhost:9010 python examples/create_topic.py demo-topic 2  # register topic (host → MinIO published port)
python examples/producer.py                                               # produce 1000 msgs
python examples/consumer.py                                               # read them back
```
- Topics must exist before producing (no auto-create).
- `examples/create_topic.py` runs on the host → point it at MinIO's published port `9010`.

## Failover

Kill a broker mid-stream — producers retry, survivor claims the partitions, consumers lose nothing:
```bash
docker compose kill broker-2
```

Sequence:
1. Dead broker stops writing `broker_health`.
2. After **10s**, `reap_dead_brokers` nulls its `partition_leaders` rows.
3. Survivors race to claim via atomic `UPDATE ... WHERE leader_id IS NULL` — Postgres ACID picks one winner.
4. Winner runs `ListObjects` on MinIO → recovers high-watermark. No log replay.
5. Clients get `LEADER_NOT_AVAILABLE`, retry metadata, resume on new leader.

- **Total window: ~10–12s**, bounded by the heartbeat timeout.
- Recorded proof:

![Failover Demo](docs/demo.gif)

*(Replay in your console: `asciinema play docs/demo.cast`)*

## Performance Benchmarks

Brokers stream to/from S3, so the profile differs from disk Kafka.

**Throughput (1 broker)**
| Message Size | Produce | Consume | S3 Cost Amortization |
|---|---|---|---|
| **100 B** | 12,875 msgs/s | 70,527 msgs/s | High (many PUTs) |
| **1 KB** | 2,238 msgs/s | 66,367 msgs/s | Moderate |
| **10 KB** | 244 msgs/s | 7,759 msgs/s (75 MB/s) | Low (S3 fetch is fast) |

- Small messages → bottlenecked by per-PUT S3 latency.
- Large fetches → S3 streams big sequential reads fast (75 MB/s+).
- Deep dive: [ARCHITECTURE.md](docs/ARCHITECTURE.md#8-benchmark-results-with-analysis).

## Consumer Group Protocol

Full group protocol so unmodified `kafka-python` consumers work:

- **JoinGroup** — members register; first member = *leader*, gets full member list; followers get empty.
- **SyncGroup** — leader runs assignment locally (e.g. RoundRobin), sends partition map; broker fans each slice back.
- **Heartbeat** — every 3s, carries `generation_id`; mismatch → `ILLEGAL_GENERATION` → consumer rejoins (how rebalances are detected).

Why leader-side assignment: broker stays ignorant of strategies → new strategies are client-only, zero broker change.

## Code Layout

Flat modules, layered top → bottom (each depends only on those above):

| Module | Responsibility |
|---|---|
| `codec.py` | Wire primitives — `BinaryReader` / `BinaryWriter`: ints, length-prefixed strings/bytes, varints. |
| `errors.py` | `ApiKey` / `ErrorCode` enums + supported API-version table. |
| `config.py` | `Settings.from_env()` — env knobs in one immutable object. |
| `protocol.py` | Request parsers + response builders + record-batch decode. Pure functions over `codec`. |
| `storage.py` | `ObjectStore` — MinIO data plane: batches, offsets, topic config. |
| `control_plane.py` | `ControlPlane` — Postgres coordination: heartbeat, claim, reap. |
| `coordinator.py` | In-memory state: `GroupCoordinator`, `OffsetStore`, `FetchPurgatory`. |
| `broker.py` | `Broker` — wires the above into one object (no globals). |
| `handlers.py` | `@handler(ApiKey.X)` registry — one function per API. |
| `server.py` | asyncio server: frame → dispatch → respond, + failover loop. |

- **Flow:** `server` reads frame → `protocol.parse_*` → `handlers` → `broker` (`storage` / `control_plane` / `coordinator`) → `protocol.build_*` → response.
- **Tests:** `python tests/test_broker.py` — codec, parsers, coordinator, dispatch. No MinIO/Postgres needed.

## Architecture & Internals

Partition leadership, storage prefixing, fetch long-polling → [ARCHITECTURE.md](docs/ARCHITECTURE.md).

## What is NOT Implemented

| Feature | Notes |
|---|---|
| **Replication / ISRs** | Single copy. Lose the bucket, lose the data. Fix: multi-bucket writes or ISR tracking in Postgres. |
| **Log Compaction** | Needs a background job: read `.batch` files, dedup by key, rewrite survivors. |
| **Transactions / EOS** | No two-phase commit, no producer-ID tracking. |
| **SSL / SASL** | Plaintext only. Needs TLS termination + SCRAM-SHA-256. |
| **Distributed Metadata Store** | Postgres is a SPOF. Prod: swap `control_plane.py` for etcd/DynamoDB. |
| **Auto CreateTopics / DeleteTopics / DescribeGroups** | Topics registered out-of-band via `examples/create_topic.py`. |
