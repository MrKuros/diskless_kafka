# Diskless Kafka Architecture

## 1. The Core Insight

Traditional Kafka tightly couples compute and local disk storage, which makes scaling and rebalancing painful and operationally expensive. By decoupling storage entirely into an object store (MinIO/S3) and leaving brokers as pure, stateless networking proxies, you gain instantaneous elasticity and infinite storage scaling. The unavoidable trade-off is the addition of S3 PUT/GET latency inline with produce and fetch requests, fundamentally shifting the bottleneck from local I/O to network bandwidth and batching efficiency.

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

## 3. The Storage Design

**Key Format and Lexicographic Order**  
All batches are written directly to S3 using the key format `s3://diskless-kafka/<topic>/<partition>/<base_offset:020d>.batch` (e.g., `.../00000000000000001024.batch`). Because object stores do not support range queries or integer-based lookups, zero-padding the base offset to 20 digits ensures that lexicographic (alphabetical) sorting perfectly aligns with numeric sorting. This allows the broker to list objects by prefix, sort them alphabetically, and perform a binary search to instantly locate the exact batch containing a consumer's requested offset.

**Raw RecordBatch Storage**  
Instead of parsing, deserializing, and reserializing individual Kafka messages, the broker stores the exact raw `RecordBatch` byte string it receives from the producer. This prevents the broker from doing expensive CPU-bound parsing work, ensuring the compute tier remains as lightweight as possible.

**Header Patching & CRC32C Validation**  
Producers generate batches starting at a local `base_offset` of 0. Before writing to S3, the broker must patch the 8-byte `base_offset` in the binary header to reflect the actual cluster-wide offset. Because the offset is included in the Kafka batch CRC checksum, simply patching the offset invalidates the payload. Rather than fully decompressing and rebuilding the batch, the broker safely updates the offset bytes and immediately recalculates the CRC32C checksum of the patched header, injecting it back into the payload before writing to MinIO.

**High-Watermark Recovery**  
Because brokers hold no local state, when a broker restarts or claims a new partition, it simply issues a `ListObjects` request to MinIO for that partition's prefix. It reads the highest `base_offset` from the final `.batch` filename, performs a single `GET` to read its header, adds the `records_count`, and instantly recovers the partition's exact high-watermark.

## 4. The Coordination Design

Without ZooKeeper or KRaft, this architecture relies on PostgreSQL to enforce linearizability and manage cluster state.

**Claiming Partition Leadership**  
Brokers maintain an active `partition_leaders` table. Only one broker can be the leader for a given `(topic, partition)` pair. To claim an orphaned partition, a broker executes an atomic `UPDATE partition_leaders SET leader_id = %s WHERE leader_id IS NULL`. The database's concurrency controls strictly guarantee that only one broker succeeds.

**Heartbeat Mechanism & Failover**  
Brokers write a timestamp to `broker_health` every 3 seconds. A background thread constantly sweeps this table; if a broker's heartbeat is older than 10 seconds, its partitions are usurped (updated to `leader_id = NULL`). The surviving brokers then immediately race to claim the newly orphaned partitions via the atomic `UPDATE` described above.

**Preventing Split-Brain**  
PostgreSQL prevents split-brain scenarios inherently through ACID transactions and primary key constraints. Two brokers cannot simultaneously hold leadership of a partition because the state transition is strictly serialized by the SQL engine.

**WarpStream's Approach**  
In production, WarpStream uses DynamoDB or a custom consensus layer (Consul/etcd) instead of PostgreSQL for this control plane. Those solutions offer multi-region replication and higher availability guarantees compared to a single PostgreSQL instance, which is a single point of failure in this PoC.

## 5. The Kafka Protocol Subset

| API Key | API Name      | Support | Notes |
|---------|---------------|---------|-------|
| 0       | Produce       | Partial | Writes `RecordBatch` directly to S3. |
| 1       | Fetch         | Partial | Implements asyncio long-polling (blocks until data arrives). |
| 2       | ListOffsets   | Partial | Supports `earliest` and `latest`. |
| 3       | Metadata      | Partial | Resolves partitions and current Postgres leaders. |
| 11      | JoinGroup     | Full    | Handles member subscription and leader election. |
| 14      | SyncGroup     | Full    | Distributes partition assignments. |
| 8       | OffsetCommit  | Full    | Stores consumer offsets in memory. |
| 9       | OffsetFetch   | Full    | Retrieves consumer offsets. |
| 18      | ApiVersions   | Partial | Fakes support for Kafka v2.4.0+. |

**The Version Probing Hack**  
Modern Kafka clients often send an `ApiVersions` request at connection time to determine what protocol versions the broker supports. To prevent clients (like `kafka-python`) from failing immediately, the broker statically replies with a hardcoded `ApiVersions` response claiming support for Kafka v2.4.0+, even though it only fully implements v0/v1 APIs.

**Missing APIs**  
We do not implement `CreateTopics`, `DeleteTopics`, `DescribeGroups`, or `SaslHandshake`. Without `CreateTopics`, topics are implicitly created on first produce. Without `SaslHandshake`, security relies entirely on network boundaries.

## 6. Consumer Group Protocol

The Kafka Consumer Group protocol is entirely managed in-memory by the broker acting as the group coordinator.

1. **JoinGroup**: Consumers join by sending their subscribed topics. The coordinator waits up to `session_timeout` for all known members to join. It selects the first consumer as the group "Leader" and returns the full list of members and subscriptions to it.
2. **SyncGroup**: The coordinator does *not* assign partitions. The consumer Leader runs the assignment strategy locally (e.g., RoundRobin) and sends the final partition mapping back to the coordinator in the `SyncGroup` request. The coordinator then distributes these assignments to the rest of the followers.
3. **Heartbeat & OffsetCommit**: Consumers periodically heartbeat to keep their session alive and commit their offsets to the coordinator.

4. **Rebalancing**: If a consumer misses a heartbeat or a new consumer joins, the coordinator drops the group state to `PreparingRebalance` and forces all members to rejoin on their next request. Because state is kept in-memory, if the broker acting as coordinator crashes, the entire group is forced to rebalance against a surviving broker.

## 7. Known Limitations vs Real Kafka

- **No Replication**: There is only a single copy of the data. If the S3 bucket is deleted or corrupted, data is permanently lost. Implementing this would require writing to multiple MinIO buckets or relying entirely on S3's native erasure coding.
- **No Log Compaction**: Kafka supports retaining only the latest value for a specific key (useful for event sourcing). This would require a background job that downloads `.batch` files, deduplicates keys, and rewrites the files to S3.
- **Higher Latency for Small Messages**: S3 HTTP overhead adds a flat 5-15ms penalty to every `Produce` request. Disk-based Kafka can `mmap` to a page cache in microseconds.
- **No SSL/SASL**: All communication is plaintext. Adding this would require integrating TLS termination into the `asyncio` socket server.

## 8. Benchmark Results with Analysis

| Message Size | Produce Throughput | Consume Throughput | S3 Cost Amortization |
|--------------|--------------------|--------------------|----------------------|
| **100 B**    | 12,875 msgs/s      | 70,527 msgs/s      | High (many PUTs)     |
| **1 KB**     | 2,238 msgs/s       | 66,367 msgs/s      | Moderate             |
| **10 KB**    | 244 msgs/s         | 7,759 msgs/s (75 MB/s) | Low (S3 fetch is fast) |

**Analysis**
- **Small Message Underperformance**: The 100B produce throughput is completely bottlenecked by S3 HTTP `PUT` requests. Because the client flushes frequently, the broker is forced to make thousands of tiny network requests to MinIO. The latency of the HTTP round-trip bounds the throughput.
- **Large Message Competitiveness**: When the consumer requests data, the broker grabs massive contiguous blocks from S3 in a single `GET` request. S3 is highly optimized for streaming large sequential reads, allowing the consumer to easily exceed 75 MB/s bandwidth.
- **Failover Time**: When a broker is killed mid-produce, failover completes in ~10-12 seconds. This is driven entirely by the `usurp_dead_brokers` background loop, which sweeps the `broker_health` table for heartbeats older than 10 seconds.

## 9. References

- [WarpStream Blog: Kafka is Dead, Long Live Kafka](https://www.warpstream.com/blog/kafka-is-dead-long-live-kafka)
- [KIP-1150: Write Kafka Data Directly to S3](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150%3A+Write+Kafka+Data+Directly+to+S3)
- [Apache Kafka Protocol Guide](https://kafka.apache.org/protocol.html)
