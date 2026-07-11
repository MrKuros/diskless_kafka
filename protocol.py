"""Kafka wire protocol: header, record batches, request parsers, response builders.

Every request body and response body is a sequence of big-endian primitives,
length-prefixed strings/bytes and (inside record batches) zigzag varints.  All
cursor walking and struct packing is delegated to :mod:`codec`, so the code
here reads as field lists and version ladders rather than offset arithmetic.

Sections: request header · record batches · request parsers · response builders.
"""

from __future__ import annotations

import struct
from collections import defaultdict
from dataclasses import dataclass

from codec import BinaryReader, BinaryWriter, frame
from config import BrokerAddress
from errors import SUPPORTED_APIS, ErrorCode, ParseError


# ═══════════════════════════════════════════════════════════════════════════
# Request header (v0/v1, pre-flexible)
# ═══════════════════════════════════════════════════════════════════════════
#
#   0..1   int16   api_key
#   2..3   int16   api_version
#   4..7   int32   correlation_id
#   8..9   int16   client_id length   (-1 = NULL)
#   10..N  bytes   client_id (UTF-8)

from errors import ApiKey  # noqa: E402  (kept near its user for readability)

_MIN_HEADER_BYTES = 10


@dataclass(frozen=True)
class RequestHeader:
    """Parsed request header plus the derived body offset."""

    api_key: int
    api_version: int
    correlation_id: int
    client_id: str | None

    @property
    def api_name(self) -> str:
        return ApiKey.name_for(self.api_key)

    @property
    def header_size(self) -> int:
        """Bytes consumed by this header — where the request body begins."""
        client_id_bytes = len(self.client_id.encode("utf-8")) if self.client_id else 0
        return _MIN_HEADER_BYTES + client_id_bytes

    def summary(self) -> str:
        cid = "NULL" if self.client_id is None else repr(self.client_id)
        return (
            f"{self.api_name}(key={self.api_key}) v{self.api_version} "
            f"corr_id={self.correlation_id} client_id={cid}"
        )


def parse_request_header(payload: bytes) -> RequestHeader:
    """Parse a request header from *payload*; raise :class:`ParseError` if malformed."""
    if len(payload) < _MIN_HEADER_BYTES:
        raise ParseError(
            f"payload too short: need >= {_MIN_HEADER_BYTES} bytes, got {len(payload)}"
        )

    api_key, api_version, correlation_id, client_id_len = struct.unpack_from(
        ">hhih", payload, 0
    )

    if client_id_len == -1:
        client_id: str | None = None
    elif client_id_len < -1:
        raise ParseError(f"invalid client_id length: {client_id_len}")
    else:
        start = _MIN_HEADER_BYTES
        end = start + client_id_len
        if len(payload) < end:
            raise ParseError(
                f"payload truncated: client_id claims {client_id_len} bytes, "
                f"only {len(payload) - start} remain"
            )
        client_id = payload[start:end].decode("utf-8")

    return RequestHeader(api_key, api_version, correlation_id, client_id)


# ═══════════════════════════════════════════════════════════════════════════
# Record batches (magic=2)
# ═══════════════════════════════════════════════════════════════════════════
#
# The broker treats record batches as opaque blobs on the storage path — it
# never re-serialises them, so producer CRC32C survives to the consumer.  These
# helpers exist only for inspection and for the records_count needed to advance
# offsets.

HEADER_SIZE = 61  # a RecordBatch opens with a fixed 61-byte header

_COMPRESSION_NAMES = {0: "none", 1: "gzip", 2: "snappy", 3: "lz4", 4: "zstd"}
_OFF_MAGIC = 16
_OFF_ATTRIBUTES = 21
_OFF_RECORDS_COUNT = 57


def records_count(data: bytes) -> int:
    """Read just the ``records_count`` field (offset 57) from a batch header."""
    return struct.unpack_from(">i", data, _OFF_RECORDS_COUNT)[0]


def parse_batch_header(data: bytes) -> dict:
    """Decode the fixed 61-byte RecordBatch header.

    Raises :class:`ValueError` if the batch is truncated or ``magic != 2``.
    ``attributes`` bitmask: bits 0-2 = compression, bit 3 = timestamp type,
    bit 4 = transactional, bit 5 = control.
    """
    if len(data) < HEADER_SIZE:
        raise ValueError(f"RecordBatch is {len(data)} bytes — need >= {HEADER_SIZE}")

    magic = struct.unpack_from(">b", data, _OFF_MAGIC)[0]
    if magic != 2:
        raise ValueError(f"unsupported RecordBatch magic={magic} (only 2 supported)")

    attributes = struct.unpack_from(">h", data, _OFF_ATTRIBUTES)[0]
    crc = struct.unpack_from(">I", data, 17)[0]
    return {
        "base_offset": struct.unpack_from(">q", data, 0)[0],
        "batch_length": struct.unpack_from(">i", data, 8)[0],
        "partition_leader_epoch": struct.unpack_from(">i", data, 12)[0],
        "magic": magic,
        "crc": f"{crc:08x}",
        "compression": _COMPRESSION_NAMES.get(attributes & 0x07, "unknown"),
        "attributes": attributes,
        "last_offset_delta": struct.unpack_from(">i", data, 23)[0],
        "base_timestamp": struct.unpack_from(">q", data, 27)[0],
        "max_timestamp": struct.unpack_from(">q", data, 35)[0],
        "producer_id": struct.unpack_from(">q", data, 43)[0],
        "producer_epoch": struct.unpack_from(">h", data, 51)[0],
        "base_sequence": struct.unpack_from(">i", data, 53)[0],
        "records_count": struct.unpack_from(">i", data, _OFF_RECORDS_COUNT)[0],
        "is_transactional": bool(attributes & 0x10),
        "is_control": bool(attributes & 0x20),
    }


def parse_records(record_set: bytes) -> list[dict]:
    """Decode individual records inside an *uncompressed* batch.

    Returns an empty list for compressed or header-only buffers.  Each record
    dict carries ``offset_delta``, ``timestamp_delta``, ``key``, ``value``,
    ``headers``.
    """
    if len(record_set) < HEADER_SIZE:
        return []

    attributes = struct.unpack_from(">h", record_set, _OFF_ATTRIBUTES)[0]
    if attributes & 0x07:  # compressed
        return []

    reader = BinaryReader(record_set[HEADER_SIZE:])
    records: list[dict] = []
    total = len(record_set) - HEADER_SIZE

    while reader.pos < total:
        rec_len = reader.varint()
        if rec_len <= 0:
            break
        rec_end = reader.pos + rec_len

        reader.raw(1)  # per-record attributes INT8 (always 0)
        ts_delta = reader.varint()
        off_delta = reader.varint()

        key_len = reader.varint()
        key = None if key_len < 0 else reader.raw(key_len)

        val_len = reader.varint()
        value = None if val_len < 0 else reader.raw(val_len)

        headers: list[tuple[str, bytes]] = []
        for _ in range(reader.varint()):
            hk = reader.raw(reader.varint()).decode("utf-8")
            hv = reader.raw(reader.varint())
            headers.append((hk, hv))

        records.append({
            "offset_delta": off_delta,
            "timestamp_delta": ts_delta,
            "key": key,
            "value": value,
            "headers": headers,
        })
        reader.pos = rec_end  # resync, guarding against malformed trailing bytes

    return records


# ═══════════════════════════════════════════════════════════════════════════
# Request parsers
# ═══════════════════════════════════════════════════════════════════════════

def _body(payload: bytes, header: RequestHeader) -> BinaryReader:
    return BinaryReader(payload, header.header_size)


# ── Metadata ────────────────────────────────────────────────────────────────

def parse_metadata_topics(payload: bytes, header: RequestHeader) -> list[str]:
    """Requested topic names; empty list means "all topics"."""
    r = _body(payload, header)
    if r.remaining() < 4:
        return []
    count = r.i32()
    if count <= 0:  # 0 = all topics, -1 = null array
        return []

    topics: list[str] = []
    for _ in range(count):
        if r.remaining() < 2:
            break
        name = r.string()
        if not name:
            continue
        topics.append(name)
    return topics


# ── Produce ─────────────────────────────────────────────────────────────────

@dataclass
class ProducePartition:
    topic: str | None
    partition: int
    record_set: bytes
    record_set_size: int


@dataclass
class ProduceRequest:
    acks: int
    timeout_ms: int
    transactional_id: str | None
    partitions: list[ProducePartition]


def parse_produce(payload: bytes, header: RequestHeader) -> ProduceRequest:
    r = _body(payload, header)
    transactional_id = r.string() if header.api_version >= 3 else None
    acks = r.i16()
    timeout_ms = r.i32()

    partitions: list[ProducePartition] = []
    for _ in range(r.i32()):  # topics
        topic = r.string()
        for _ in range(r.i32()):  # partitions
            partition = r.i32()
            size = r.i32()
            record_set = r.raw(size) if size > 0 else b""
            partitions.append(ProducePartition(topic, partition, record_set, size))

    return ProduceRequest(acks, timeout_ms, transactional_id, partitions)


# ── Fetch ───────────────────────────────────────────────────────────────────

@dataclass
class FetchTarget:
    topic: str | None
    partition: int
    fetch_offset: int
    partition_max_bytes: int


@dataclass
class FetchRequest:
    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    targets: list[FetchTarget]


def parse_fetch(payload: bytes, header: RequestHeader) -> FetchRequest:
    v = header.api_version
    r = _body(payload, header)

    r.i32()  # replica_id
    max_wait_ms = r.i32()
    min_bytes = r.i32()
    max_bytes = r.i32() if v >= 3 else (2**31 - 1)
    isolation_level = r.i8() if v >= 4 else 0
    if v >= 7:
        r.i32()  # session_id
        r.i32()  # session_epoch

    targets: list[FetchTarget] = []
    for _ in range(r.i32()):  # topics
        topic = r.string()
        for _ in range(r.i32()):  # partitions
            partition = r.i32()
            if v >= 9:
                r.i32()  # current_leader_epoch
            fetch_offset = r.i64()
            if v >= 5:
                r.i64()  # log_start_offset
            partition_max_bytes = r.i32()
            targets.append(
                FetchTarget(topic, partition, fetch_offset, partition_max_bytes)
            )

    if v >= 7:  # forgotten_topics_data
        for _ in range(r.i32()):
            r.string()
            for _ in range(r.i32()):
                r.i32()

    return FetchRequest(max_wait_ms, min_bytes, max_bytes, isolation_level, targets)


# ── ListOffsets ─────────────────────────────────────────────────────────────

@dataclass
class ListOffsetTarget:
    topic: str | None
    partition: int
    timestamp: int


def parse_list_offsets(payload: bytes, header: RequestHeader) -> list[ListOffsetTarget]:
    v = header.api_version
    r = _body(payload, header)

    r.i32()  # replica_id
    if v >= 2:
        r.i8()  # isolation_level

    targets: list[ListOffsetTarget] = []
    for _ in range(r.i32()):  # topics
        topic = r.string()
        for _ in range(r.i32()):  # partitions
            partition = r.i32()
            timestamp = r.i64()
            if v == 0:
                r.i32()  # max_num_offsets
            targets.append(ListOffsetTarget(topic, partition, timestamp))
    return targets


# ── FindCoordinator ─────────────────────────────────────────────────────────

@dataclass
class FindCoordinatorRequest:
    key: str
    coordinator_type: int


def parse_find_coordinator(payload: bytes, header: RequestHeader) -> FindCoordinatorRequest:
    r = _body(payload, header)
    key = r.string() or ""
    coordinator_type = r.i8() if header.api_version >= 1 else 0
    return FindCoordinatorRequest(key, coordinator_type)


# ── JoinGroup ───────────────────────────────────────────────────────────────

@dataclass
class JoinGroupRequest:
    group_id: str
    session_timeout: int
    member_id: str
    protocol_type: str
    protocols: list[tuple[str, bytes]]


def parse_join_group(payload: bytes, header: RequestHeader) -> JoinGroupRequest:
    v = header.api_version
    r = _body(payload, header)

    group_id = r.string() or ""
    session_timeout = r.i32()
    if v >= 1:
        r.i32()  # rebalance_timeout
    member_id = r.string() or ""
    if v >= 5:
        r.string()  # group_instance_id (nullable)
    protocol_type = r.string() or ""

    protocols: list[tuple[str, bytes]] = []
    for _ in range(r.i32()):
        name = r.string() or ""
        protocols.append((name, r.blob()))

    return JoinGroupRequest(group_id, session_timeout, member_id, protocol_type, protocols)


# ── SyncGroup ───────────────────────────────────────────────────────────────

@dataclass
class SyncGroupRequest:
    group_id: str
    generation_id: int
    member_id: str
    assignments: dict[str, bytes]  # member_id -> assignment bytes (leader only)


def parse_sync_group(payload: bytes, header: RequestHeader) -> SyncGroupRequest:
    v = header.api_version
    r = _body(payload, header)

    group_id = r.string() or ""
    generation_id = r.i32()
    member_id = r.string() or ""
    if v >= 3:
        r.string()  # group_instance_id (nullable)

    assignments: dict[str, bytes] = {}
    for _ in range(r.i32()):
        mid = r.string() or ""
        assignments[mid] = r.blob()

    return SyncGroupRequest(group_id, generation_id, member_id, assignments)


# ── Heartbeat ───────────────────────────────────────────────────────────────

@dataclass
class HeartbeatRequest:
    group_id: str
    generation_id: int
    member_id: str


def parse_heartbeat(payload: bytes, header: RequestHeader) -> HeartbeatRequest:
    r = _body(payload, header)
    group_id = r.string() or ""
    generation_id = r.i32()
    member_id = r.string() or ""
    return HeartbeatRequest(group_id, generation_id, member_id)


# ── OffsetCommit ────────────────────────────────────────────────────────────

@dataclass
class OffsetCommitEntry:
    topic: str
    partition: int
    offset: int


@dataclass
class OffsetCommitRequest:
    group_id: str
    entries: list[OffsetCommitEntry]


def parse_offset_commit(payload: bytes, header: RequestHeader) -> OffsetCommitRequest:
    v = header.api_version
    r = _body(payload, header)

    group_id = r.string() or ""
    if v >= 1:
        r.i32()  # generation_id
        r.string()  # consumer_id
    if v >= 2:
        r.i64()  # retention_time

    entries: list[OffsetCommitEntry] = []
    for _ in range(r.i32()):  # topics
        topic = r.string() or ""
        for _ in range(r.i32()):  # partitions
            partition = r.i32()
            offset = r.i64()
            if v == 1:
                r.i64()  # timestamp (v1 only)
            r.string()  # metadata (all versions)
            entries.append(OffsetCommitEntry(topic, partition, offset))

    return OffsetCommitRequest(group_id, entries)


# ── OffsetFetch ─────────────────────────────────────────────────────────────

@dataclass
class OffsetFetchRequest:
    group_id: str
    targets: list[tuple[str, int]]  # (topic, partition)


def parse_offset_fetch(payload: bytes, header: RequestHeader) -> OffsetFetchRequest:
    r = _body(payload, header)
    group_id = r.string() or ""

    targets: list[tuple[str, int]] = []
    for _ in range(r.i32()):  # topics
        topic = r.string() or ""
        for _ in range(r.i32()):  # partitions
            targets.append((topic, r.i32()))

    return OffsetFetchRequest(group_id, targets)


# ═══════════════════════════════════════════════════════════════════════════
# Response builders
# ═══════════════════════════════════════════════════════════════════════════
#
# Each returns a fully framed response (4-byte length prefix + payload) and
# opens with the request's correlation_id so the client can match it.

def _resp(correlation_id: int) -> BinaryWriter:
    return BinaryWriter().i32(correlation_id)


def build_api_versions_response(correlation_id: int, api_version: int) -> bytes:
    """ApiVersions v0/v1/v2. v3+ (flexible) is unsupported → error in v0 format."""
    error_code = ErrorCode.NONE
    if api_version > 2:
        api_version = 0
        error_code = ErrorCode.UNSUPPORTED_VERSION

    w = _resp(correlation_id).i16(error_code)
    w.i32(len(SUPPORTED_APIS))
    for api_key, min_ver, max_ver in SUPPORTED_APIS:
        w.i16(api_key).i16(min_ver).i16(max_ver)
    if api_version >= 1:
        w.i32(0)  # throttle_time_ms
    return frame(w.getvalue())


def build_metadata_response(
    correlation_id: int,
    topics: list[str],
    cluster: list[BrokerAddress],
    api_version: int = 0,
    topic_config: dict[str, dict] | None = None,
    partition_leaders: dict[str, dict[int, int]] | None = None,
) -> bytes:
    topic_config = topic_config or {}
    partition_leaders = partition_leaders or {}
    topics_to_return = topics or list(topic_config.keys())

    w = _resp(correlation_id)

    # Brokers array: node_id | host | port | (rack, v1+).
    w.i32(len(cluster))
    for b in cluster:
        w.i32(b.node_id).string(b.host).i32(b.port)
        if api_version >= 1:
            w.null_string()  # rack

    # controller_id (v1+) sits between brokers and topics.
    if api_version >= 1:
        w.i32(cluster[0].node_id if cluster else 1)

    w.i32(len(topics_to_return))
    for topic_name in topics_to_return:
        num_partitions = topic_config.get(topic_name, {}).get("partitions", 1)
        w.i16(ErrorCode.NONE).string(topic_name)
        if api_version >= 1:
            w.boolean(False)  # is_internal
        w.i32(num_partitions)

        for part_id in range(num_partitions):
            leader = partition_leaders.get(topic_name, {}).get(part_id)
            if leader is None:
                w.i16(ErrorCode.LEADER_NOT_AVAILABLE).i32(part_id).i32(-1)
                w.i32(0)  # replicas
                w.i32(0)  # isr
            else:
                w.i16(ErrorCode.NONE).i32(part_id).i32(leader)
                w.i32(1).i32(leader)  # replicas
                w.i32(1).i32(leader)  # isr

    return frame(w.getvalue())


def build_produce_response(
    correlation_id: int,
    results: list[tuple[str, int, int, int]],  # (topic, partition, error, base_offset)
    api_version: int = 7,
) -> bytes:
    by_topic: dict[str, list[tuple[int, int, int]]] = defaultdict(list)
    for topic, partition, error_code, base_offset in results:
        by_topic[topic].append((partition, error_code, base_offset))

    w = _resp(correlation_id).i32(len(by_topic))
    for topic_name, parts in by_topic.items():
        w.string(topic_name).i32(len(parts))
        for partition, error_code, base_offset in parts:
            w.i32(partition).i16(error_code).i64(base_offset)
            if api_version >= 2:
                w.i64(-1)  # log_append_time
            if api_version >= 5:
                w.i64(0)  # log_start_offset
    if api_version >= 1:
        w.i32(0)  # throttle_time_ms
    return frame(w.getvalue())


def build_fetch_response(
    correlation_id: int,
    results: list[tuple[str, int, int, int, bytes | None]],
    # (topic, partition, error_code, high_watermark, records_bytes)
    api_version: int = 4,
) -> bytes:
    w = _resp(correlation_id)
    if api_version >= 1:
        w.i32(0)  # throttle_time_ms
    if api_version >= 7:
        w.i16(ErrorCode.NONE)  # top-level error_code
        w.i32(0)  # session_id

    by_topic: dict[str, list] = defaultdict(list)
    for topic, partition, error_code, hw, records in results:
        by_topic[topic].append((partition, error_code, hw, records))

    w.i32(len(by_topic))
    for topic_name, parts in by_topic.items():
        w.string(topic_name).i32(len(parts))
        for partition, error_code, hw, records in parts:
            w.i32(partition).i16(error_code).i64(hw)
            if api_version >= 4:
                w.i64(-1)  # last_stable_offset
            if api_version >= 5:
                w.i64(0)  # log_start_offset
            if api_version >= 4:
                w.i32(-1)  # aborted_transactions = null
            if records:
                w.blob(records)
            else:
                w.i32(0)  # empty records buffer
    return frame(w.getvalue())


def build_list_offsets_response(
    correlation_id: int,
    results: list[tuple[str, int, int, int, int]],
    # (topic, partition, error_code, timestamp, offset)
    api_version: int = 1,
) -> bytes:
    w = _resp(correlation_id)
    if api_version >= 2:
        w.i32(0)  # throttle_time_ms

    by_topic: dict[str, list] = defaultdict(list)
    for topic, partition, error_code, timestamp, offset in results:
        by_topic[topic].append((partition, error_code, timestamp, offset))

    w.i32(len(by_topic))
    for topic_name, parts in by_topic.items():
        w.string(topic_name).i32(len(parts))
        for partition, error_code, timestamp, offset in parts:
            w.i32(partition).i16(error_code)
            if api_version == 0:
                w.i32(1).i64(offset)  # legacy offsets array (count=1)
            else:
                w.i64(timestamp).i64(offset)
    return frame(w.getvalue())


def build_find_coordinator_response(
    correlation_id: int,
    api_version: int = 0,
    error_code: int = ErrorCode.NONE,
    coordinator_id: int = 1,
    host: str = "localhost",
    port: int = 9092,
) -> bytes:
    w = _resp(correlation_id)
    if api_version >= 1:
        w.i32(0)  # throttle_time_ms
    w.i16(error_code)
    if api_version >= 1:
        w.null_string()  # error_message
    w.i32(coordinator_id).string(host).i32(port)
    return frame(w.getvalue())


def build_join_group_response(
    correlation_id: int,
    api_version: int,
    error_code: int,
    generation_id: int,
    protocol_name: str,
    leader_id: str,
    member_id: str,
    members: list[tuple[str, bytes]],  # (member_id, metadata) — leader only
) -> bytes:
    w = _resp(correlation_id)
    if api_version >= 2:
        w.i32(0)  # throttle_time_ms
    w.i16(error_code).i32(generation_id)
    w.string(protocol_name).string(leader_id).string(member_id)
    w.i32(len(members))
    for mid, metadata in members:
        w.string(mid)
        if api_version >= 5:
            w.null_string()  # group_instance_id
        w.blob(metadata)
    return frame(w.getvalue())


def encode_member_assignment(topic_partitions: dict[str, list[int]]) -> bytes:
    """Encode a ConsumerProtocolAssignment (version 0) body — no length prefix."""
    w = BinaryWriter().i16(0).i32(len(topic_partitions))  # version, topic count
    for topic, parts in topic_partitions.items():
        w.string(topic).i32(len(parts))
        for p in parts:
            w.i32(p)
    w.i32(-1)  # user_data = null
    return w.getvalue()


def build_sync_group_response(
    correlation_id: int,
    api_version: int,
    error_code: int,
    member_assignment_bytes: bytes,
) -> bytes:
    w = _resp(correlation_id)
    if api_version >= 1:
        w.i32(0)  # throttle_time_ms
    w.i16(error_code).blob(member_assignment_bytes)
    return frame(w.getvalue())


def build_heartbeat_response(
    correlation_id: int, api_version: int, error_code: int = ErrorCode.NONE
) -> bytes:
    w = _resp(correlation_id)
    if api_version >= 1:
        w.i32(0)  # throttle_time_ms
    w.i16(error_code)
    return frame(w.getvalue())


def build_offset_commit_response(
    correlation_id: int,
    api_version: int,
    results: list[tuple[str, list[int]]],  # [(topic, [partition, ...]), ...]
) -> bytes:
    w = _resp(correlation_id)
    if api_version >= 3:
        w.i32(0)  # throttle_time_ms
    w.i32(len(results))
    for topic, partitions in results:
        w.string(topic).i32(len(partitions))
        for p in partitions:
            w.i32(p).i16(ErrorCode.NONE)
    return frame(w.getvalue())


def build_offset_fetch_response(
    correlation_id: int,
    api_version: int,
    results: list[tuple[str, list[tuple[int, int]]]],
    # [(topic, [(partition, committed_offset), ...]), ...]
) -> bytes:
    w = _resp(correlation_id)
    if api_version >= 3:
        w.i32(0)  # throttle_time_ms
    w.i32(len(results))
    for topic, part_offsets in results:
        w.string(topic).i32(len(part_offsets))
        for partition, offset in part_offsets:
            w.i32(partition).i64(offset).null_string().i16(ErrorCode.NONE)
    if api_version >= 2:
        w.i16(ErrorCode.NONE)  # top-level error_code
    return frame(w.getvalue())


def build_leave_group_response(correlation_id: int, api_version: int = 1) -> bytes:
    w = _resp(correlation_id)
    if api_version >= 1:
        w.i32(0)  # throttle_time_ms
    w.i16(ErrorCode.NONE)
    return frame(w.getvalue())
