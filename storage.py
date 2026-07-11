"""Object-store data plane (MinIO / S3).

The broker keeps *no* local disk state — this is the "diskless" in the name.
Record batches, committed offsets and topic config all live in an object store.

Batches are written and served as whole opaque objects, keyed by a zero-padded
base offset so lexicographic key order equals log order::

    {topic}/{partition}/{base_offset:020d}.batch

Writing the batch verbatim (no parse, no re-serialise) preserves the producer's
CRC32C all the way to the consumer.

Everything lives on :class:`ObjectStore` so a broker owns exactly one client
and one in-memory high-watermark cache, and tests can point it at a fake.
"""

from __future__ import annotations

import io
import json
import logging
import struct
import time

from minio import Minio
from minio.error import S3Error

from config import Settings
from protocol import records_count as _records_count

log = logging.getLogger("kafka.storage")

_CONSUMER_OFFSETS_PREFIX = "__consumer_offsets/"
_TOPIC_CONFIG_KEY = "__topic_config/topics.json"
_TOPIC_CONFIG_TTL_S = 5.0

# The 4-byte records_count field sits at offset 57 of a RecordBatch header.
_RECORDS_COUNT_OFFSET = 57


class ObjectStore:
    """MinIO-backed storage for batches, offsets and topic config."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.bucket = settings.minio_bucket
        self._client: Minio | None = None
        # (topic, partition) -> next offset to assign (a.k.a. high watermark).
        self._high_watermarks: dict[tuple[str, int], int] = {}
        self._topic_config: dict[str, dict] | None = None
        self._topic_config_loaded_at = 0.0

    # ── Client / bucket ──────────────────────────────────────────────────────

    @property
    def client(self) -> Minio:
        """Lazily construct the client and ensure the bucket exists."""
        if self._client is None:
            s = self._settings
            self._client = Minio(
                s.minio_endpoint,
                access_key=s.minio_access_key,
                secret_key=s.minio_secret_key,
                secure=s.minio_secure,
            )
            if not self._client.bucket_exists(self.bucket):
                self._client.make_bucket(self.bucket)
                log.info("MinIO: created bucket %r", self.bucket)
        return self._client

    # ── Batch write / read ───────────────────────────────────────────────────

    def write_batch(
        self, topic: str, partition: int, record_set: bytes, record_count: int
    ) -> int:
        """Store a raw RecordBatch and return the base offset assigned to it."""
        base_offset = self.high_watermark(topic, partition)
        object_key = f"{topic}/{partition}/{base_offset:020d}.batch"

        self.client.put_object(
            self.bucket,
            object_key,
            io.BytesIO(record_set),
            length=len(record_set),
            content_type="application/octet-stream",
        )
        self._high_watermarks[(topic, partition)] = base_offset + record_count
        log.info(
            "MinIO PUT  s3://%s/%s  (%d bytes, %d record(s), next=%d)",
            self.bucket, object_key, len(record_set), record_count,
            self._high_watermarks[(topic, partition)],
        )
        return base_offset

    def read_batch(
        self, topic: str, partition: int, fetch_offset: int
    ) -> tuple[bytes | None, int]:
        """Return ``(batch_bytes, high_watermark)`` covering *fetch_offset*.

        ``batch_bytes`` is ``None`` when the consumer is caught up or no data
        exists.  The stored batch's base_offset header field (bytes 0-7, outside
        the CRC) is patched to the real offset before it is served.
        """
        batches = self._list_batches(topic, partition)
        if not batches:
            return None, 0

        hw = self.high_watermark(topic, partition)
        if fetch_offset >= hw:
            return None, hw

        # Last batch whose base_offset <= fetch_offset.
        target_key = target_base = None
        for base_offset, obj_key in batches:
            if base_offset <= fetch_offset:
                target_key, target_base = obj_key, base_offset
            else:
                break
        if target_key is None:
            log.warning("Fetch: offset %d precedes first batch %d",
                        fetch_offset, batches[0][0])
            return None, hw

        try:
            resp = self.client.get_object(self.bucket, target_key)
            batch_bytes = bytearray(resp.read())
            resp.close()
        except S3Error as exc:
            log.error("MinIO GET failed for %r: %s", target_key, exc)
            return None, hw

        struct.pack_into(">q", batch_bytes, 0, target_base)  # patch base_offset
        log.info("MinIO GET  s3://%s/%s  (%d bytes, fetch_offset=%d, hw=%d)",
                 self.bucket, target_key, len(batch_bytes), fetch_offset, hw)
        return bytes(batch_bytes), hw

    def high_watermark(self, topic: str, partition: int) -> int:
        """Next offset for the partition, recovering from the store if cold."""
        cached = self._high_watermarks.get((topic, partition), 0)
        if cached > 0:
            return cached

        batches = self._list_batches(topic, partition)
        if not batches:
            return 0

        last_base, last_key = batches[-1]
        try:
            resp = self.client.get_object(self.bucket, last_key)
            last_bytes = resp.read()
            resp.close()
            hw = last_base + _records_count(last_bytes)
            self._high_watermarks[(topic, partition)] = hw
            log.info("MinIO: recovered hw=%d for %s/%d", hw, topic, partition)
            return hw
        except Exception as exc:
            log.warning("MinIO: hw recovery failed for %r: %s", last_key, exc)
            return 0

    def _list_batches(self, topic: str, partition: int) -> list[tuple[int, str]]:
        """Sorted ``(base_offset, object_key)`` for a partition's batch objects."""
        prefix = f"{topic}/{partition}/"
        try:
            objects = list(self.client.list_objects(self.bucket, prefix=prefix))
        except S3Error as exc:
            log.error("MinIO list error for %r: %s", prefix, exc)
            return []

        batches: list[tuple[int, str]] = []
        for obj in objects:
            filename = obj.object_name.rsplit("/", 1)[-1]
            if not filename.endswith(".batch"):
                continue
            try:
                batches.append((int(filename[:-6]), obj.object_name))
            except ValueError:
                continue
        batches.sort()
        return batches

    # ── Committed offsets (mirrors __consumer_offsets) ───────────────────────

    def commit_offset(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        """Persist a committed offset; the PUT overwrite acts as log compaction."""
        object_key = f"{_CONSUMER_OFFSETS_PREFIX}{group_id}/{topic}/{partition}.json"
        payload = json.dumps(
            {"group": group_id, "topic": topic, "partition": partition, "offset": offset}
        ).encode("utf-8")
        self.client.put_object(
            self.bucket, object_key, io.BytesIO(payload),
            length=len(payload), content_type="application/json",
        )

    def load_committed_offsets(self) -> dict[tuple[str, str, int], int]:
        """Load all persisted offsets, keyed by ``(group, topic, partition)``."""
        result: dict[tuple[str, str, int], int] = {}
        try:
            objects = list(self.client.list_objects(
                self.bucket, prefix=_CONSUMER_OFFSETS_PREFIX, recursive=True))
        except S3Error as exc:
            log.warning("MinIO: could not list committed offsets: %s", exc)
            return result

        for obj in objects:
            if not obj.object_name.endswith(".json"):
                continue
            try:
                resp = self.client.get_object(self.bucket, obj.object_name)
                data = json.loads(resp.read().decode("utf-8"))
                resp.close()
                result[(data["group"], data["topic"], data["partition"])] = data["offset"]
            except Exception as exc:
                log.warning("MinIO: failed to read %r: %s", obj.object_name, exc)

        log.info("MinIO: loaded %d committed offset(s)", len(result))
        return result

    # ── Topic config (mirrors ZooKeeper/KRaft metadata) ──────────────────────

    def get_topic_config(self) -> dict[str, dict]:
        """Topic config ``{name: {partitions, replication_factor}}`` (5s cached)."""
        now = time.time()
        if self._topic_config is not None and now - self._topic_config_loaded_at < _TOPIC_CONFIG_TTL_S:
            return self._topic_config

        try:
            resp = self.client.get_object(self.bucket, _TOPIC_CONFIG_KEY)
            data = json.loads(resp.read().decode("utf-8"))
            resp.close()
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                data = {}
            else:
                log.warning("MinIO: could not read topic config: %s", exc)
                return self._topic_config or {}

        self._topic_config = data
        self._topic_config_loaded_at = now
        return data

    def put_topic_config(self, config: dict[str, dict]) -> None:
        data = json.dumps(config).encode("utf-8")
        self.client.put_object(
            self.bucket, _TOPIC_CONFIG_KEY, io.BytesIO(data),
            length=len(data), content_type="application/json",
        )
        self._topic_config = config
        self._topic_config_loaded_at = time.time()
        log.info("MinIO: saved topic config — %d topic(s): %s",
                 len(config), list(config.keys()))

    def create_topic(self, topic: str, partitions: int, replication_factor: int = 1) -> None:
        """Register a topic. Idempotent; raises on conflicting partition count."""
        config = self.get_topic_config()
        if topic in config:
            if config[topic]["partitions"] == partitions:
                log.info("Topic %r already exists (%d partitions) — no-op", topic, partitions)
                return
            raise ValueError(
                f"Topic {topic!r} exists with {config[topic]['partitions']} "
                f"partition(s); delete it before recreating."
            )
        config[topic] = {"partitions": partitions, "replication_factor": replication_factor}
        self.put_topic_config(config)
        log.info("Created topic %r  partitions=%d  replication_factor=%d",
                 topic, partitions, replication_factor)
