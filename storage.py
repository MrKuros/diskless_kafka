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
