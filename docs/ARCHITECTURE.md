# Diskless Kafka Architecture

## Contents

1. [The Core Insight](#1-the-core-insight)
2. [Architecture Diagram](#2-architecture-diagram)
   - [Code Layout & Design](#code-layout--design)
3. [The Storage Design](#3-the-storage-design)
4. [The Coordination Design](#4-the-coordination-design)
5. [The Kafka Protocol Subset](#5-the-kafka-protocol-subset)
6. [Consumer Group Protocol](#6-consumer-group-protocol)
7. [Known Limitations vs Real Kafka](#7-known-limitations-vs-real-kafka)
8. [Benchmark Results with Analysis](#8-benchmark-results-with-analysis)
9. [References](#9-references)

## 1. The Core Insight

- Traditional Kafka couples compute + local disk → painful, expensive scaling and rebalancing.
- Decouple: storage → object store (MinIO/S3), brokers → stateless proxies.
- Gain: instant elasticity, infinite storage.
- Cost: S3 PUT/GET latency inline with produce/fetch → bottleneck shifts from local I/O to network + batching.

## 2. Architecture Diagram

```text
                                 ┌──────────────┐
                                 │  Producer    │
                                 └──────┬───────┘
                                        │ (Produce Request)
                                        │
           ┌────────────────────────────┼────────────────────────────┐
           ▼                            ▼                            ▼
  ┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
  │                 │          │                 │          │                 │
  │   Broker 1      │          │   Broker 2      │          │   Broker N      │
  │   (Stateless)   │          │   (Stateless)   │          │   (Stateless)   │
  │                 │          │                 │          │                 │
  └────┬───────┬────┘          └────┬───────┬────┘          └────┬───────┬────┘
       │       │                    │       │                    │       │
 (PUT) │       │ (SQL: Heartbeat    │       │ (SQL: Claim        │       │
       │       │  & Claim Leader)   │       │  Leader)           │       │
       │       ▼                    │       ▼                    │       ▼
       │ ┌─────────────────────────────────────────────────────────────────┐
       │ │                           PostgreSQL                            │
       │ │  (Cluster Coordination: partition_leaders, broker_health)       │
       │ └─────────────────────────────────────────────────────────────────┘
       │
       │ (PUT/GET Batch)
       │
       ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │                              MinIO                              │
 │            (Object Storage: s3://<topic>/<partition>/)          │
 └──────────────┬──────────────────────────────────────────────────┘
                │
                │ (Fetch Request / GET Object)
                │
         ┌──────▼───────┐
         │  Consumer    │
         └──────────────┘
```

## Code Layout & Design

### Module map

Arrows = "calls / depends on". Dependencies flow downward — no cycles.

```text
                         ┌───────────────────────┐
                         │   kafka-python client  │
                         └───────────┬────────────┘
                                     │ TCP :9092
                                     ▼
                         ┌───────────────────────┐
                         │      server.py         │
                         │  frame → dispatch      │
                         └───────────┬────────────┘
                                     │ dispatch(header, payload)
                                     ▼
                         ┌───────────────────────┐
                         │     handlers.py        │
                         │  @handler(ApiKey.X)    │
                         └───┬───────────────┬────┘
                             │               │
                  parse/build│               │drive
                             ▼               ▼
                  ┌──────────────┐  ┌──────────────────┐
                  │ protocol.py  │  │    broker.py      │
                  │ parsers +    │  │ aggregates        │
                  │ builders     │  │ subsystems        │
                  └──────┬───────┘  └──┬─────┬──────┬──┘
                         │             │     │      │
                         ▼             ▼     ▼      ▼
                  ┌──────────────┐  ┌──────┐┌────┐┌─────────────┐
                  │  codec.py    │  │store ││ctrl││coordinator  │
                  │  BinaryReader│  │.py   ││.py ││.py          │
                  │  / Writer    │  └──┬───┘└──┬─┘└─────────────┘
                  └──────────────┘     │       │
                                      │PUT/GET│SQL
                                      ▼       ▼
                                  ┌──────┐ ┌──────────┐
                                  │MinIO │ │ Postgres │
                                  └──────┘ └──────────┘
```

**Foundation** (imported by every layer): `config.py` (`Settings`) · `errors.py` (`ApiKey` / `ErrorCode`).

### Modules

- `codec.py` — `BinaryReader` / `BinaryWriter`: only place that touches raw bytes (ints, length-prefixed strings/bytes, varints).
- `protocol.py` — request parsers + response builders + record-batch decode; pure functions over `codec`.
- `errors.py` — `ApiKey` / `ErrorCode` enums, supported API-version table.
- `config.py` — `Settings.from_env()`, one immutable config object.
- `storage.py` — `ObjectStore`: MinIO data plane (batches, offsets, topic config).
- `control_plane.py` — `ControlPlane`: Postgres coordination (heartbeat, claim, reap).
- `coordinator.py` — in-memory state: `GroupCoordinator`, `OffsetStore`, `FetchPurgatory`.
- `broker.py` — `Broker`: aggregates the subsystems into one object.
- `handlers.py` — per-API handlers behind a registry.
- `server.py` — asyncio server: frame → dispatch → respond, + failover loop.

### Design choices

- **Registry dispatch** — `@handler(ApiKey.X)` maps each API to one function; no `if api_key == …` ladder.
- **One codec** — every parser/builder shares `BinaryReader`/`BinaryWriter`; Kafka's null/length rules live in exactly one place.
- **No globals** — group/offset/fetch state held on objects, passed via `Broker`; handlers unit-testable with fakes.
- **Byte relay** — batches stored verbatim, never re-parsed on the hot path.

### Data flow example

**Produce** — a client writes one batch:

1. `server.py` — reads the 4-byte length + payload; `protocol.parse_request_header` → `RequestHeader(api_key=0)`.
2. `server.py` → `dispatch` → `handlers._produce` (registered for `ApiKey.PRODUCE`).
3. `handlers._produce` → `protocol.parse_produce` → `ProduceRequest` holding the **raw** `record_set` bytes.
4. → `broker.store.write_batch(...)` → `ObjectStore` PUTs those bytes verbatim to `s3://.../<topic>/<part>/<offset:020d>.batch`, returns `base_offset`.
5. → `broker.purgatory.notify(topic, part)` — wakes any Fetch parked on this partition.
6. → `protocol.build_produce_response` → framed bytes → `server.py` writes to socket.

**Fetch** — a consumer reads it back:

1. `server.py` → `handlers._fetch` (`ApiKey.FETCH`); `protocol.parse_fetch` → targets `(topic, part, fetch_offset)`.
2. → `broker.store.read_batch(...)` → `ObjectStore` lists the prefix, picks the batch, patches `base_offset` (CRC-safe), returns bytes + high-watermark.
3. No data yet + within `max_wait_ms` → `await broker.purgatory.waiter(...)` (long-poll); a Produce `notify` wakes it early.
4. → `protocol.build_fetch_response` → framed bytes → socket.

## 3. The Storage Design

**Key format & lexicographic order**
- Key: `s3://diskless-kafka/<topic>/<partition>/<base_offset:020d>.batch` (e.g. `.../00000000000000001024.batch`).
- Object stores have no range/integer lookup → zero-pad offset to 20 digits so alphabetical sort == numeric sort.
- Broker: list by prefix → sort → binary-search to the exact batch holding a requested offset.

**Raw RecordBatch storage**
- Store the producer's raw `RecordBatch` bytes verbatim — no parse/deserialize/reserialize.
- Keeps the compute tier lightweight (no CPU-bound parsing).

**Header patching (CRC-safe)**
- Producers emit `base_offset = 0`; broker patches it to the real cluster offset on fetch.
- CRC32C covers bytes 21+ only → `base_offset` (bytes 0–7) sits outside it.
- Overwrite those 8 bytes, serve — no decompress, no rebuild, no CRC recalc.

**High-watermark recovery**
- No local state → on restart/claim: `ListObjects` the partition prefix.
- Read highest `base_offset` from the last `.batch` filename → one `GET` for its header → add `records_count`.
- Exact high-watermark recovered instantly.

## 4. The Coordination Design

No ZooKeeper/KRaft — Postgres enforces linearizability and holds cluster state.

**Claiming partition leadership**
- `partition_leaders` table; one leader per `(topic, partition)`.
- Claim orphan: atomic `UPDATE partition_leaders SET leader_id = %s WHERE leader_id IS NULL` → DB guarantees a single winner.

**Heartbeat & failover**
- Brokers write `broker_health` every 3s.
- Background sweep: heartbeat > 10s old → partitions usurped (`leader_id = NULL`).
- Survivors race to re-claim via the atomic `UPDATE`.

**Preventing split-brain**
- ACID + primary-key constraints serialize the state transition → two brokers can't co-lead a partition.
- Trade-off: single Postgres = SPOF; prod → multi-region store (etcd, DynamoDB).

## 5. The Kafka Protocol Subset

| API Key | API Name | Support | Notes |
|---------|----------|---------|-------|
| 0 | Produce | Partial | Writes `RecordBatch` directly to S3. |
| 1 | Fetch | Partial | asyncio long-poll (blocks until data). |
| 2 | ListOffsets | Partial | `earliest` and `latest`. |
| 3 | Metadata | Partial | Resolves partitions + current Postgres leaders. |
| 11 | JoinGroup | Full | Member subscription + leader election. |
| 14 | SyncGroup | Full | Distributes partition assignments. |
| 8 | OffsetCommit | Full | Stores consumer offsets in memory. |
| 9 | OffsetFetch | Full | Retrieves consumer offsets. |
| 18 | ApiVersions | Partial | Fakes support for Kafka v2.4.0+. |

**Version-probing hack**
- Clients send `ApiVersions` at connect to learn supported versions.
- Broker replies with a hardcoded set claiming Kafka v2.4.0+ → clients (e.g. `kafka-python`) don't bail, even though only v0/v1 are fully implemented.

**Missing APIs**
- No `CreateTopics` → topics registered out-of-band via `examples/create_topic.py` (writes topic config to MinIO).
- No `DeleteTopics`, `DescribeGroups`.
- No `SaslHandshake` → security = network boundaries only.

## 6. Consumer Group Protocol

Group coordinator runs entirely in broker memory.

- **JoinGroup** — members send subscriptions; coordinator waits up to `session_timeout`, picks first as Leader, returns full member+subscription list to it.
- **SyncGroup** — coordinator does *not* assign; Leader computes the partition map (e.g. RoundRobin) and sends it back; coordinator fans slices to followers.
- **Heartbeat & OffsetCommit** — members heartbeat to stay alive + commit offsets.
- **Rebalancing** — missed heartbeat / new member → `PreparingRebalance`, all members forced to rejoin. Coordinator state is in-memory → if that broker dies, the group rebalances against a survivor.

## 7. Known Limitations vs Real Kafka

- **No replication** — single copy; bucket lost = data lost. Fix: multi-bucket writes or S3 erasure coding.
- **No log compaction** — would need a background job to download `.batch` files, dedup keys, rewrite.
- **Higher small-message latency** — S3 HTTP adds a flat 5–15ms per Produce; disk Kafka hits page cache in µs.
- **No SSL/SASL** — plaintext; needs TLS termination in the asyncio server.

## 8. Benchmark Results with Analysis

| Message Size | Produce | Consume | S3 Cost Amortization |
|--------------|---------|---------|----------------------|
| **100 B** | 12,875 msgs/s | 70,527 msgs/s | High (many PUTs) |
| **1 KB** | 2,238 msgs/s | 66,367 msgs/s | Moderate |
| **10 KB** | 244 msgs/s | 7,759 msgs/s (75 MB/s) | Low (S3 fetch is fast) |

- **Small-message underperformance** — 100B produce bottlenecked by per-`PUT` S3 round-trips; frequent client flushes → thousands of tiny requests; latency bounds throughput.
- **Large-message competitiveness** — consumer grabs big contiguous blocks in one `GET`; S3 loves sequential reads → 75 MB/s+.
- **Failover time** — ~10–12s, driven entirely by `reap_dead_brokers` sweeping `broker_health` for heartbeats > 10s old.

## 9. References

- [KIP-1150: Write Kafka Data Directly to S3](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150%3A+Write+Kafka+Data+Directly+to+S3)
- [Apache Kafka Protocol Guide](https://kafka.apache.org/protocol.html)
