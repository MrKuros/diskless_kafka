"""
diskless_kafka/storage.py
─────────────────────────
Day 7: MinIO-backed batch store — write (Day 6) + read (Day 7).

Every RecordBatch that arrives in a Produce request is written verbatim to
MinIO under the object key:

    {topic}/{partition}/{base_offset:020d}.batch

The 20-digit zero-padded base_offset ensures that lexicographic sort of object
keys equals numeric (log) order — crucial when the Fetch handler lists the
bucket to reconstruct the partition log.

Why the entire RecordBatch, not individual records?
  • CRC32C integrity is preserved end-to-end (producer → MinIO → consumer).
  • Compression is batch-scoped; individual records aren't separately decodable.
  • The broker never needs to parse message contents — it's a pure byte relay.
  • Replication and retention can operate on whole batch objects atomically.

Configuration (change these to match your MinIO instance):
  MINIO_ENDPOINT   — host:port of the MinIO API (default: localhost:9000)
  MINIO_ACCESS_KEY — access key   (default MinIO standalone: minioadmin)
  MINIO_SECRET_KEY — secret key   (default MinIO standalone: minioadmin)
  MINIO_BUCKET     — target bucket (created automatically on first write)
"""

from __future__ import annotations

import io
import json
import struct
import logging
from minio import Minio
from minio.error import S3Error

log = logging.getLogger("kafka.storage")

# ---------------------------------------------------------------------------
# Connection settings — edit these to match your MinIO instance
# ---------------------------------------------------------------------------
MINIO_ENDPOINT   = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET     = "diskless-kafka"
MINIO_SECURE     = False      # False = plain HTTP; True = HTTPS

# ---------------------------------------------------------------------------
# Lazy-initialised MinIO client
# ---------------------------------------------------------------------------
_client: Minio | None = None


def get_client() -> Minio:
    """Return the (singleton) MinIO client, initialising it on first call."""
    global _client
    if _client is None:
        _client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        # Create the bucket if it does not exist yet.
        try:
            if not _client.bucket_exists(MINIO_BUCKET):
                _client.make_bucket(MINIO_BUCKET)
                log.info("MinIO: created bucket %r", MINIO_BUCKET)
            else:
                log.info("MinIO: bucket %r already exists", MINIO_BUCKET)
        except S3Error as exc:
            log.error("MinIO: bucket check/create failed: %s", exc)
            raise
    return _client


# ---------------------------------------------------------------------------
# Per-partition offset tracking (in-memory cache)
# ---------------------------------------------------------------------------
# Tracks the next available offset for each (topic, partition).
# Increments by records_count after every successful write.
#
# If the broker restarts, read_batch() reconstructs the high_watermark by
# scanning the MinIO objects and reading the records_count from the last batch
# header, so consumers can continue without data loss.

_next_offset: dict[tuple[str, int], int] = {}


def write_batch(topic: str, partition: int, record_set: bytes, records_count: int) -> int:
    """
    Write a raw RecordBatch to MinIO and advance the partition offset.

    Parameters
    ----------
    topic          : Kafka topic name.
    partition      : Partition index (0-based).
    record_set     : The raw RecordBatch bytes exactly as received from the
                     producer — NOT parsed, NOT modified.
    records_count  : Number of records inside the batch (from the RecordBatch
                     header field at bytes 57-60).  Used to advance the offset
                     counter by the right amount.

    Returns
    -------
    base_offset : The offset assigned to the first record in this batch.
                  Sent back to the producer in the Produce response.

    Object key format
    -----------------
    {topic}/{partition}/{base_offset:020d}.batch

    Example:  test-topic/0/00000000000000000000.batch
    """
    key = (topic, partition)
    base_offset = _next_offset.get(key, 0)

    # Zero-padded offset → lexicographic sort == log order
    object_key = f"{topic}/{partition}/{base_offset:020d}.batch"

    client = get_client()
    data = io.BytesIO(record_set)

    client.put_object(
        MINIO_BUCKET,
        object_key,
        data,
        length=len(record_set),
        content_type="application/octet-stream",
    )

    # Advance partition offset by the number of records in this batch.
    _next_offset[key] = base_offset + records_count

    log.info(
        "MinIO PUT  s3://%s/%s  (%d bytes, %d record(s), next_offset=%d)",
        MINIO_BUCKET,
        object_key,
        len(record_set),
        records_count,
        _next_offset[key],
    )

    return base_offset


# ---------------------------------------------------------------------------
# Fetch path — read_batch()
# ---------------------------------------------------------------------------
# Strategy for finding the right batch object:
#
#   1. List all .batch objects for this topic/partition prefix.
#   2. Parse the base_offset from the 20-digit key name.
#   3. Sort by base_offset ascending (lexicographic ≡ numeric due to padding).
#   4. Find the last batch whose base_offset ≤ fetch_offset.
#   5. Patch the base_offset field in the RecordBatch header to the key value.
#      (bytes 0–7 are NOT covered by CRC32C, so the patch is safe.)
#   6. Return (batch_bytes, high_watermark).
#
# High-watermark recovery after broker restart:
#   When _next_offset is 0 (broker just started), scan the last batch to
#   get records_count and recompute high_watermark.  This means the consumer
#   can reconnect after a broker restart without data loss.

def read_batch(
    topic: str,
    partition: int,
    fetch_offset: int,
) -> tuple[bytes | None, int]:
    """
    Find and return the RecordBatch bytes that contain *fetch_offset*.

    Parameters
    ----------
    topic, partition : which partition to read from.
    fetch_offset     : the consumer's next expected offset.

    Returns
    -------
    (batch_bytes, high_watermark)
        batch_bytes:    raw RecordBatch bytes ready to embed in a Fetch
                        response, or None if no data exists at that offset.
        high_watermark: next offset after all known data.  If equal to
                        fetch_offset the consumer is fully caught up.
    """
    client = get_client()
    prefix = f"{topic}/{partition}/"

    # ── List all batch objects for this partition ─────────────────────────────
    try:
        objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix))
    except S3Error as exc:
        log.error("MinIO list error for %r: %s", prefix, exc)
        return None, _next_offset.get((topic, partition), 0)

    # Parse (base_offset, object_key) from each .batch filename
    batches: list[tuple[int, str]] = []
    for obj in objects:
        filename = obj.object_name.split("/")[-1]
        if not filename.endswith(".batch"):
            continue
        try:
            batches.append((int(filename[:-6]), obj.object_name))
        except ValueError:
            continue

    if not batches:
        log.debug("Fetch →  no objects in prefix %r", prefix)
        return None, 0

    batches.sort()   # ascending by base_offset (lexicographic == numeric)

    # ── Reconstruct high_watermark if broker just restarted ───────────────────
    key = (topic, partition)
    hw  = _next_offset.get(key, 0)

    if hw == 0:
        # Read the last batch's header to get records_count and recompute HW.
        last_base, last_obj_key = batches[-1]
        try:
            resp = client.get_object(MINIO_BUCKET, last_obj_key)
            last_bytes = resp.read()
            resp.close()
            # RecordBatch header: records_count at bytes 57-60 (INT32)
            records_count = struct.unpack_from(">i", last_bytes, 57)[0]
            hw = last_base + records_count
            _next_offset[key] = hw   # cache for future calls
            log.info("MinIO: recovered hw=%d for %s/%d from object listing",
                     hw, topic, partition)
        except Exception as exc:
            log.warning("MinIO: HW recovery failed for %r: %s", last_obj_key, exc)

    # ── Nothing to return if consumer is already caught up ────────────────────
    if fetch_offset >= hw:
        log.debug(
            "Fetch →  topic=%r partition=%d fetch_offset=%d >= hw=%d (empty)",
            topic, partition, fetch_offset, hw,
        )
        return None, hw

    # ── Find the batch that contains fetch_offset ─────────────────────────────
    # "The last batch whose base_offset ≤ fetch_offset"
    target_key  = None
    target_base = 0

    for (base_offset, obj_key) in batches:
        if base_offset <= fetch_offset:
            target_key  = obj_key
            target_base = base_offset
        else:
            break

    if target_key is None:
        log.warning("Fetch →  fetch_offset=%d before first batch base=%d",
                    fetch_offset, batches[0][0])
        return None, hw

    # ── Read the batch bytes ──────────────────────────────────────────────────
    try:
        resp = client.get_object(MINIO_BUCKET, target_key)
        batch_bytes = resp.read()
        resp.close()
    except S3Error as exc:
        log.error("MinIO GET failed for %r: %s", target_key, exc)
        return None, hw

    # ── Patch base_offset in the RecordBatch header (bytes 0–7) ──────────────
    # The producer sets base_offset=0 in every batch it sends.  The broker
    # stores the batch at the key offset (e.g., 5), so we must patch the
    # header field to reflect the actual offset before serving it.
    #
    # This is safe: the CRC32C only covers bytes 21+, so bytes 0–7 are
    # outside the checksum — exactly as the real Kafka broker does it.
    batch_mutable = bytearray(batch_bytes)
    struct.pack_into(">q", batch_mutable, 0, target_base)
    batch_bytes = bytes(batch_mutable)

    log.info(
        "MinIO GET  s3://%s/%s  (%d bytes, fetch_offset=%d, hw=%d)",
        MINIO_BUCKET, target_key, len(batch_bytes), fetch_offset, hw,
    )

    return batch_bytes, hw


# ---------------------------------------------------------------------------
# Committed offset persistence  (mirrors __consumer_offsets in real Kafka)
# ---------------------------------------------------------------------------
# In real Kafka, committed offsets are stored in a compacted internal topic
# called __consumer_offsets.  Compaction keeps only the latest entry per
# (group, topic, partition) key — effectively the same as overwriting an
# S3 object with the same key on every commit.
#
# We use the key format:
#   __consumer_offsets/{group_id}/{topic}/{partition}.json
#
# Each object contains a tiny JSON payload:
#   {"group": "...", "topic": "...", "partition": 0, "offset": 42}
#
# This is human-readable, easily inspectable via the MinIO console, and
# gives us crash-safe persistence at the cost of one HTTP PUT per commit.


def commit_offset(group_id: str, topic: str, partition: int, offset: int) -> None:
    """
    Persist a committed offset to MinIO.

    The object is overwritten on every call — S3 PUT idempotency acts as
    the compaction mechanism: only the most recent value survives.

    Object key: __consumer_offsets/{group_id}/{topic}/{partition}.json
    """
    object_key = f"__consumer_offsets/{group_id}/{topic}/{partition}.json"
    payload = json.dumps({
        "group":     group_id,
        "topic":     topic,
        "partition": partition,
        "offset":    offset,
    }).encode("utf-8")

    client = get_client()
    client.put_object(
        MINIO_BUCKET,
        object_key,
        io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    log.debug(
        "MinIO PUT  s3://%s/%s  offset=%d",
        MINIO_BUCKET, object_key, offset,
    )


def load_committed_offsets() -> dict[tuple[str, str, int], int]:
    """
    Load all persisted committed offsets from MinIO on broker startup.

    Scans the __consumer_offsets/ prefix, reads every JSON object, and
    returns a dict keyed by (group_id, topic, partition) → offset.

    Called once at startup to pre-populate server.COMMITTED_OFFSETS so
    consumers can resume from where they left off after a broker restart.
    """
    client = get_client()
    prefix = "__consumer_offsets/"
    result: dict[tuple[str, str, int], int] = {}

    try:
        objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
    except S3Error as exc:
        log.warning("MinIO: could not list committed offsets: %s", exc)
        return result

    for obj in objects:
        if not obj.object_name.endswith(".json"):
            continue
        try:
            resp = client.get_object(MINIO_BUCKET, obj.object_name)
            data = json.loads(resp.read().decode("utf-8"))
            resp.close()
            key = (data["group"], data["topic"], data["partition"])
            result[key] = data["offset"]
            log.info(
                "MinIO: loaded committed offset  group=%r topic=%r "
                "partition=%d offset=%d",
                data["group"], data["topic"], data["partition"], data["offset"],
            )
        except Exception as exc:
            log.warning("MinIO: failed to read %r: %s", obj.object_name, exc)

    log.info("MinIO: loaded %d committed offset(s) from persistent storage", len(result))
    return result


# ---------------------------------------------------------------------------
# Topic config persistence  (mirrors ZooKeeper/KRaft topic metadata)
# ---------------------------------------------------------------------------
# In real Kafka, topic metadata (partition count, replication factor, leader
# assignments, ISR lists) is stored in ZooKeeper or the KRaft metadata log.
# Every broker has a full in-memory copy refreshed via watches/replication.
#
# We store a single JSON object in MinIO:
#   __topic_config/topics.json
#
# Schema:
#   {
#     "my-topic": {
#       "partitions": 3,
#       "replication_factor": 1
#     },
#     ...
#   }
#
# Why a single file and not one file per topic?
#   - Atomic reads: one GET always returns a consistent view of all topics.
#   - Simplicity: no need for a prefix scan on every Metadata request.
#   - Tradeoff: concurrent creates from two brokers would race (last PUT wins).
#     Acceptable for a single-broker cluster.

TOPIC_CONFIG_KEY = "__topic_config/topics.json"

# In-memory cache so we don't hit MinIO on every Metadata request.
# Invalidated whenever put_topic_config() is called.
_topic_config_cache: dict[str, dict] | None = None


def get_topic_config() -> dict[str, dict]:
    """
    Return the full topic config dict from MinIO (or the in-memory cache).

    Returns a dict:
        { topic_name: {"partitions": int, "replication_factor": int}, ... }

    Returns {} if no config object exists yet (first run).
    """
    global _topic_config_cache
    if _topic_config_cache is not None:
        return _topic_config_cache

    client = get_client()
    try:
        resp = client.get_object(MINIO_BUCKET, TOPIC_CONFIG_KEY)
        data = json.loads(resp.read().decode("utf-8"))
        resp.close()
        _topic_config_cache = data
        log.info("MinIO: loaded topic config — %d topic(s): %s",
                 len(data), list(data.keys()))
        return data
    except S3Error as exc:
        if exc.code == "NoSuchKey":
            log.info("MinIO: no topic config found — starting with empty registry")
            _topic_config_cache = {}
            return {}
        log.warning("MinIO: could not read topic config: %s", exc)
        return {}


def put_topic_config(config: dict[str, dict]) -> None:
    """
    Persist the full topic config dict to MinIO and update the cache.

    Call this whenever a topic is created or deleted.

    Parameters
    ----------
    config : { topic_name: {"partitions": int, "replication_factor": int} }
    """
    global _topic_config_cache
    client = get_client()
    payload = json.dumps(config, indent=2).encode("utf-8")
    client.put_object(
        MINIO_BUCKET,
        TOPIC_CONFIG_KEY,
        io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    _topic_config_cache = config
    log.info("MinIO: saved topic config — %d topic(s): %s",
             len(config), list(config.keys()))


def create_topic(topic: str, partitions: int, replication_factor: int = 1) -> None:
    """
    Register a topic in the MinIO topic config.

    Idempotent: if the topic already exists with the same settings, no-op.
    Raises ValueError if the topic exists with *different* settings.
    """
    config = get_topic_config()
    if topic in config:
        existing = config[topic]
        if existing["partitions"] == partitions:
            log.info("Topic %r already exists with %d partition(s) — no-op",
                     topic, partitions)
            return
        raise ValueError(
            f"Topic {topic!r} already exists with {existing['partitions']} "
            f"partition(s); refusing to overwrite (delete it first)."
        )
    config[topic] = {"partitions": partitions, "replication_factor": replication_factor}
    put_topic_config(config)
    log.info("Created topic %r  partitions=%d  replication_factor=%d",
             topic, partitions, replication_factor)
