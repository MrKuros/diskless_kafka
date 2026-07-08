"""
diskless_kafka/benchmark.py
───────────────────────────
Comprehensive benchmark for the diskless Kafka broker. Covers:

  1. Producer throughput   — msgs/s and MB/s for 100B, 1KB, 10KB payloads
  2. Consumer throughput   — msgs/s, MB/s, and end-to-end latency (p50/p95/p99)
  3. Multi-broker          — compare 1-broker vs 2-broker throughput side-by-side
  4. Failover time         — kill broker-2 mid-produce and measure leader-claim latency
  5. Consumer group rebalance — kill one consumer; measure partition takeover time

Run with both brokers up:
    docker compose up -d
    source .venv/bin/activate
    MINIO_ENDPOINT=localhost:9010 python benchmark.py
"""

from __future__ import annotations

import os
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka import TopicPartition
from kafka.errors import NoBrokersAvailable, KafkaError

# ─── Configuration ────────────────────────────────────────────────────────────

ONE_BROKER  = ["localhost:9092"]
TWO_BROKERS = ["localhost:9092", "localhost:9093"]

MSG_COUNTS: dict[int, int] = {
    100:   100_000,   # 100-byte messages  (~10 MB total)
    1024:  100_000,   # 1 KB messages      (~100 MB total)
    10240:  10_000,   # 10 KB messages     (~100 MB total)
}

FAILOVER_MSG_COUNT  = 10_000
REBALANCE_MSG_COUNT = 5_000   # per partition

HEARTBEAT_TIMEOUT_SEC = 10

# ─── Topic helpers ─────────────────────────────────────────────────────────────

def _ensure_topic(topic: str, partitions: int = 2) -> None:
    """Register a topic in MinIO's topic config (no-op if already exists)."""
    from storage import get_topic_config, create_topic as _create
    cfg = get_topic_config()
    if topic not in cfg:
        _create(topic, partitions, replication_factor=1)
        print(f"    ✓ created topic {topic!r} ({partitions} partitions)")


def _clear_topic_data(topic: str) -> None:
    """Delete all RecordBatch objects for a topic from MinIO."""
    from storage import get_client, MINIO_BUCKET, _next_offset
    client = get_client()
    try:
        objs = list(client.list_objects(MINIO_BUCKET, prefix=f"{topic}/", recursive=True))
        for obj in objs:
            client.remove_object(MINIO_BUCKET, obj.object_name)
        # Also wipe the in-process offset cache so the broker starts fresh
        keys_to_del = [k for k in _next_offset if k[0] == topic]
        for k in keys_to_del:
            del _next_offset[k]
    except Exception as exc:
        print(f"    [warn] MinIO clear failed for {topic!r}: {exc}")


def _delete_pg_leaders(topic: str) -> None:
    """Remove partition_leaders rows for a topic from Postgres."""
    try:
        import psycopg2
        dsn = os.getenv("POSTGRES_DSN",
                        "postgresql://kafka:kafka@localhost:5432/diskless_kafka")
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM partition_leaders WHERE topic = %s", (topic,))
            conn.commit()
    except Exception as exc:
        print(f"    [warn] Postgres clear failed for {topic!r}: {exc}")


def _reset_topic(topic: str, partitions: int = 2) -> None:
    """Wipe topic data and ensure it's registered in MinIO config."""
    _clear_topic_data(topic)
    _delete_pg_leaders(topic)
    _ensure_topic(topic, partitions)


# ─── Producer / Consumer factories ────────────────────────────────────────────

def _make_producer(brokers: list[str]) -> KafkaProducer:
    for attempt in range(10):
        try:
            return KafkaProducer(
                bootstrap_servers=brokers,
                api_version=(1, 0, 0),
                acks=1,
                linger_ms=50,           # accumulate more messages per batch
                batch_size=1024 * 1024, # 1 MB — amortises S3 PUT cost
                compression_type=None,
            )
        except NoBrokersAvailable:
            time.sleep(1)
    raise RuntimeError(f"Could not connect to {brokers}")


def _make_consumer(
    brokers: list[str],
    topic: str,
    group: str,
    partitions: Optional[list[int]] = None,
) -> KafkaConsumer:
    parts = partitions if partitions is not None else [0, 1]
    c = KafkaConsumer(
        bootstrap_servers=brokers,
        api_version=(1, 0, 0),
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=30_000,  # 30 s — generous for slow brokers
        fetch_max_bytes=52_428_800,
        max_partition_fetch_bytes=10_485_760,
    )
    tps = [TopicPartition(topic, p) for p in parts]
    c.assign(tps)
    # Seek to beginning explicitly so we don't depend on committed offsets
    c.seek_to_beginning(*tps)
    return c


# ─── Utility ──────────────────────────────────────────────────────────────────

def _pct(data: list[float], pct: int) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _docker_kill(service: str) -> None:
    subprocess.run(
        ["docker", "compose", "kill", service],
        capture_output=True,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )


def _docker_start(service: str) -> None:
    subprocess.run(
        ["docker", "compose", "start", service],
        capture_output=True,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )


# ─── Section 1 & 2: Throughput ────────────────────────────────────────────────

@dataclass
class ThroughputResult:
    size:       int
    brokers:    int
    prod_tps:   float = 0.0
    prod_mbs:   float = 0.0
    cons_tps:   float = 0.0
    cons_mbs:   float = 0.0
    lat_p50_ms: float = 0.0
    lat_p95_ms: float = 0.0
    lat_p99_ms: float = 0.0


def _produce(topic: str, brokers: list[str], size: int, count: int, out: dict) -> None:
    producer = _make_producer(brokers)
    # Pre-build a message template: first 8 bytes = big-endian ms timestamp
    pad = b"x" * max(0, size - 8)
    start = time.perf_counter()
    for _ in range(count):
        ts = int(time.time() * 1000).to_bytes(8, "big")
        producer.send(topic, ts + pad)
    producer.flush()
    elapsed = time.perf_counter() - start
    producer.close()
    out["tps"] = count / elapsed
    out["mbs"] = (count * size) / (1024 * 1024) / elapsed


def _consume(topic: str, brokers: list[str], size: int, count: int, out: dict) -> None:
    consumer = _make_consumer(
        brokers, topic,
        group=f"bench-{topic}-{int(time.time())}",
    )
    latencies_ms: list[float] = []
    consumed = 0
    start = time.perf_counter()
    for msg in consumer:
        if len(msg.value) >= 8:
            ts_ms = int.from_bytes(msg.value[:8], "big")
            lat = int(time.time() * 1000) - ts_ms
            if 0 < lat < 300_000:
                latencies_ms.append(float(lat))
        consumed += 1
        if consumed >= count:
            break
    elapsed = time.perf_counter() - start
    consumer.close()
    out["tps"]    = consumed / elapsed if elapsed > 0 else 0.0
    out["mbs"]    = (consumed * size) / (1024 * 1024) / elapsed if elapsed > 0 else 0.0
    out["lat_p50"] = _pct(latencies_ms, 50)
    out["lat_p95"] = _pct(latencies_ms, 95)
    out["lat_p99"] = _pct(latencies_ms, 99)


def run_throughput_benchmark(
    brokers: list[str],
    sizes_counts: dict[int, int],
) -> list[ThroughputResult]:
    """
    For each message size:
      1. Produce all messages (blocking flush), record produce throughput.
      2. Immediately start consumer from offset 0 and drain all messages.
      3. End-to-end latency = time.now() - timestamp_in_message, measured per
         message. This is the "time-to-available" metric: how long after a
         producer called send() does the data become readable by a consumer.
         The p50/p95/p99 reflect batch upload latency + fetch round-trip.
    """
    results: list[ThroughputResult] = []
    nb = len(brokers)
    for size, count in sizes_counts.items():
        topic = f"bench-{size}-{nb}b-{int(time.time())}"
        total_mb = count * size / (1024 * 1024)
        print(f"\n  [{nb} broker(s)]  {size:>6} B × {count:,} msgs ({total_mb:.0f} MB)  topic={topic!r}")
        _reset_topic(topic, partitions=2)
        time.sleep(2)  # wait for broker topic-config cache TTL

        p_out: dict = {}
        c_out: dict = {}

        # ── Produce all messages first ────────────────────────────────────────
        _produce(topic, brokers, size, count, p_out)
        print(f"    produce: {p_out.get('tps', 0):>8,.0f} msgs/s   {p_out.get('mbs', 0):.2f} MB/s")

        # ── Consume immediately after flush ──────────────────────────────────
        _consume(topic, brokers, size, count, c_out)
        print(
            f"    consume: {c_out.get('tps', 0):>8,.0f} msgs/s   {c_out.get('mbs', 0):.2f} MB/s   "
            f"lat p50={c_out.get('lat_p50', 0):.0f}ms  "
            f"p95={c_out.get('lat_p95', 0):.0f}ms  "
            f"p99={c_out.get('lat_p99', 0):.0f}ms"
        )

        results.append(ThroughputResult(
            size=size, brokers=nb,
            prod_tps=p_out.get("tps", 0), prod_mbs=p_out.get("mbs", 0),
            cons_tps=c_out.get("tps", 0), cons_mbs=c_out.get("mbs", 0),
            lat_p50_ms=c_out.get("lat_p50", 0),
            lat_p95_ms=c_out.get("lat_p95", 0),
            lat_p99_ms=c_out.get("lat_p99", 0),
        ))
    return results


# ─── Section 4: Failover ──────────────────────────────────────────────────────

@dataclass
class FailoverResult:
    msgs_before_kill:  int   = 0
    msgs_after_kill:   int   = 0
    failover_time_sec: float = 0.0
    total_produced:    int   = 0
    total_consumed:    int   = 0
    zero_loss:         bool  = False


def run_failover_benchmark() -> FailoverResult:
    print("\n  Setting up failover test …")
    topic = f"bench-failover-{int(time.time())}"
    _reset_topic(topic, partitions=2)

    _docker_start("broker-2")
    time.sleep(5)

    producer = _make_producer(TWO_BROKERS)
    result = FailoverResult()
    halfway = FAILOVER_MSG_COUNT // 2

    print(f"  Producing first {halfway} msgs (both brokers up) …")
    for i in range(halfway):
        producer.send(topic, f"msg-{i}".encode())
    producer.flush()
    result.msgs_before_kill = halfway

    print("  Killing broker-2 …")
    kill_ts = time.perf_counter()
    _docker_kill("broker-2")

    print("  Producing remaining msgs; retrying until broker-1 claims leadership …")
    failover_done = False
    retries = 0
    claim_ts = kill_ts

    for i in range(halfway, FAILOVER_MSG_COUNT):
        while True:
            try:
                fut = producer.send(topic, f"msg-{i}".encode())
                if not failover_done:
                    fut.get(timeout=5)
                    claim_ts = time.perf_counter()
                    failover_done = True
                break
            except Exception:
                retries += 1
                time.sleep(0.1)

    producer.flush()
    producer.close()

    result.msgs_after_kill   = FAILOVER_MSG_COUNT - halfway
    result.failover_time_sec = claim_ts - kill_ts
    result.total_produced    = FAILOVER_MSG_COUNT

    print(f"  Failover in {result.failover_time_sec:.2f}s  ({retries} retried sends)")

    print("  Verifying zero message loss via consumer …")
    time.sleep(2)
    verifier = _make_consumer(
        ONE_BROKER, topic,
        group=f"bench-failover-verify-{int(time.time())}",
    )
    consumed = sum(1 for _ in verifier)
    verifier.close()
    result.total_consumed = consumed
    result.zero_loss      = (consumed == FAILOVER_MSG_COUNT)
    print(
        f"  Produced {FAILOVER_MSG_COUNT:,}  Consumed {consumed:,}  → "
        f"{'✓ ZERO LOSS' if result.zero_loss else '✗ LOSS DETECTED'}"
    )
    return result


# ─── Section 5: Consumer group rebalance ──────────────────────────────────────

@dataclass
class RebalanceResult:
    rebalance_time_sec: float = 0.0
    detected: bool = False


def run_rebalance_benchmark() -> RebalanceResult:
    """
    Manual-assign two consumers to partitions 0 and 1.
    Stop consumer-A after consuming half its messages.
    Reassign consumer-B to both partitions and time how long until it
    receives the first message from the partition consumer-A abandoned.
    """
    print("\n  Setting up rebalance test …")
    topic = f"bench-rebalance-{int(time.time())}"
    _reset_topic(topic, partitions=2)

    _docker_start("broker-2")
    time.sleep(4)

    total_per_partition = REBALANCE_MSG_COUNT
    print(f"  Pre-producing {total_per_partition * 2:,} msgs …")
    producer = _make_producer(TWO_BROKERS)
    for i in range(total_per_partition * 2):
        producer.send(topic, f"msg-{i}".encode())
    producer.flush()
    producer.close()
    time.sleep(1)

    result = RebalanceResult()
    kill_ts: list[float] = []
    first_p0_ts: list[float] = []
    stop_a = threading.Event()

    def consumer_a() -> None:
        """Consume half of partition 0, then stop."""
        c = _make_consumer(TWO_BROKERS, topic,
                           group=f"rbal-{int(time.time())}", partitions=[0])
        n = 0
        for _ in c:
            n += 1
            if n >= total_per_partition // 2:
                break
        c.close()
        kill_ts.append(time.perf_counter())
        stop_a.set()

    def consumer_b() -> None:
        """Start on partition 1; after A stops, get reassigned to both."""
        c = _make_consumer(TWO_BROKERS, topic,
                           group=f"rbal2-{int(time.time())}", partitions=[1])
        # Consume partition 1 until A dies, then switch to both
        for _ in c:
            if stop_a.is_set():
                break
        c.close()

        # Reassign to both partitions and time first message from P0
        c2 = _make_consumer(ONE_BROKER, topic,
                             group=f"rbal2-final-{int(time.time())}",
                             partitions=[0, 1])
        for msg in c2:
            if msg.partition == 0:
                first_p0_ts.append(time.perf_counter())
                break
        c2.close()

    ta = threading.Thread(target=consumer_a, daemon=True)
    tb = threading.Thread(target=consumer_b, daemon=True)
    ta.start()
    tb.start()

    ta.join(timeout=30)
    stop_a.set()  # in case tb needs nudging
    tb.join(timeout=30)

    if kill_ts and first_p0_ts:
        result.rebalance_time_sec = first_p0_ts[0] - kill_ts[0]
        result.detected = True
    return result


# ─── Idle CPU ─────────────────────────────────────────────────────────────────

def get_idle_cpu() -> str:
    print("  Waiting 10 s for brokers to idle …")
    time.sleep(10)
    samples: list[float] = []
    for _ in range(3):
        try:
            r = subprocess.run(
                ["docker", "stats", "--no-stream", "--format",
                 "{{.CPUPerc}}", "diskless_kafka-broker-1-1"],
                capture_output=True, text=True, check=True,
            )
            val = r.stdout.strip().replace("%", "")
            if val:
                samples.append(float(val))
        except Exception:
            pass
        time.sleep(1)
    return f"{sum(samples)/len(samples):.2f}%" if samples else "N/A"


# ─── Pretty-print tables ──────────────────────────────────────────────────────

W = 110
SEP  = "─" * W
SEP2 = "═" * W


def _hr(size: int) -> str:
    return f"{size // 1024} KB" if size >= 1024 else f"{size} B"


def print_throughput_table(rows: list[ThroughputResult]) -> None:
    print(f"\n{SEP2}")
    print("  THROUGHPUT & END-TO-END LATENCY")
    print(SEP2)
    hdr = (
        f"  {'Msg Size':<9} {'Brokers':<8}"
        f"{'Prod msgs/s':>13} {'Prod MB/s':>11}"
        f"{'Cons msgs/s':>13} {'Cons MB/s':>11}"
        f"{'Lat p50':>9} {'Lat p95':>9} {'Lat p99':>9}"
    )
    print(hdr)
    print(f"  {SEP}")
    for r in rows:
        print(
            f"  {_hr(r.size):<9} {r.brokers:<8}"
            f"{r.prod_tps:>13,.0f} {r.prod_mbs:>11.2f}"
            f"{r.cons_tps:>13,.0f} {r.cons_mbs:>11.2f}"
            f"{r.lat_p50_ms:>8.0f}ms {r.lat_p95_ms:>8.0f}ms {r.lat_p99_ms:>8.0f}ms"
        )
    print(f"  {SEP2}\n")


def print_multi_broker_table(
    single: list[ThroughputResult],
    dual:   list[ThroughputResult],
) -> None:
    print(f"\n{SEP2}")
    print("  MULTI-BROKER SCALING  (1 broker vs 2 brokers, same workload)")
    print(SEP2)
    hdr = (
        f"  {'Msg Size':<9}"
        f"{'1B Prod/s':>13} {'1B Cons/s':>13}"
        f"{'2B Prod/s':>13} {'2B Cons/s':>13}"
        f"{'Prod scale':>12} {'Cons scale':>12}"
    )
    print(hdr)
    print(f"  {SEP}")
    by_size: dict[int, dict] = {}
    for r in single:
        by_size.setdefault(r.size, {})["s"] = r
    for r in dual:
        by_size.setdefault(r.size, {})["d"] = r
    for size in sorted(by_size):
        g  = by_size[size]
        r1 = g.get("s")
        r2 = g.get("d")
        if r1 and r2:
            ps = r2.prod_tps / r1.prod_tps if r1.prod_tps else 0
            cs = r2.cons_tps / r1.cons_tps if r1.cons_tps else 0
            print(
                f"  {_hr(size):<9}"
                f"{r1.prod_tps:>13,.0f} {r1.cons_tps:>13,.0f}"
                f"{r2.prod_tps:>13,.0f} {r2.cons_tps:>13,.0f}"
                f"{ps:>11.2f}×  {cs:>11.2f}×"
            )
    print(f"  {SEP2}\n")


def print_failover_table(r: FailoverResult) -> None:
    print(f"\n{SEP2}")
    print("  FAILOVER BENCHMARK")
    print(SEP2)
    rows = [
        ("Messages before broker-2 killed",  f"{r.msgs_before_kill:,}"),
        ("Messages after kill (via broker-1)", f"{r.msgs_after_kill:,}"),
        ("Failover detection time",           f"{r.failover_time_sec:.2f} s"),
        ("Total produced",                    f"{r.total_produced:,}"),
        ("Total consumed (post-failover)",    f"{r.total_consumed:,}"),
        ("Zero message loss",                 "✓ YES" if r.zero_loss else "✗ NO"),
    ]
    for k, v in rows:
        print(f"  {k:<42}  {v}")
    print(f"  {SEP2}\n")


def print_rebalance_table(r: RebalanceResult) -> None:
    print(f"\n{SEP2}")
    print("  CONSUMER GROUP REBALANCE BENCHMARK")
    print(SEP2)
    if r.detected:
        print(f"  Time from consumer-A stop → consumer-B first P0 msg:  {r.rebalance_time_sec:.2f} s")
        print(f"  (includes seek-to-beginning + first Fetch round-trip)")
    else:
        print("  Rebalance event not detected within timeout.")
    print(f"  Note: manual assign has no JoinGroup rebalance; this measures")
    print(f"  raw seek + Fetch latency when a consumer takes over a partition.")
    print(f"  {SEP2}\n")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print(SEP2)
    print("  DISKLESS KAFKA BENCHMARK SUITE")
    print(SEP2)

    # ── 1: 1-broker throughput ────────────────────────────────────────────────
    print("\n[1/5] Throughput — 1 broker")
    single_results = run_throughput_benchmark(ONE_BROKER, MSG_COUNTS)

    # ── 2: 2-broker throughput ────────────────────────────────────────────────
    print("\n[2/5] Throughput — 2 brokers")
    dual_results = run_throughput_benchmark(TWO_BROKERS, MSG_COUNTS)

    # ── 3: Failover ───────────────────────────────────────────────────────────
    print("\n[3/5] Failover Test")
    failover_result: Optional[FailoverResult] = None
    try:
        failover_result = run_failover_benchmark()
    except Exception as exc:
        print(f"  [error] {exc}")
    finally:
        print("  Restarting broker-2 …")
        _docker_start("broker-2")
        time.sleep(4)

    # ── 4: Rebalance ──────────────────────────────────────────────────────────
    print("\n[4/5] Consumer Rebalance Test")
    rebalance_result: Optional[RebalanceResult] = None
    try:
        rebalance_result = run_rebalance_benchmark()
    except Exception as exc:
        print(f"  [error] {exc}")

    # ── 5: Idle CPU ───────────────────────────────────────────────────────────
    print("\n[5/5] Fetch Long-Poll Efficiency (Idle Broker CPU)")
    idle_cpu = get_idle_cpu()

    # ── Tables ────────────────────────────────────────────────────────────────
    print_throughput_table(single_results + dual_results)
    print_multi_broker_table(single_results, dual_results)
    if failover_result:
        print_failover_table(failover_result)
    if rebalance_result:
        print_rebalance_table(rebalance_result)

    print(SEP2)
    print(f"  Fetch long-poll idle CPU (broker-1):  {idle_cpu}")
    print(f"  Heartbeat timeout (failover ceiling):  {HEARTBEAT_TIMEOUT_SEC} s")
    print(SEP2 + "\n")


if __name__ == "__main__":
    main()
