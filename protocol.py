"""
diskless_kafka/protocol.py
──────────────────────────
Day 5: Parse the Kafka request header + encode ApiVersions/Metadata responses
       + parse Produce requests and RecordBatch contents.

Kafka wire format (Request Header v0 / v1):
──────────────────────────────────────────
  Offset  Size  Type     Field
  ──────  ────  ───────  ─────────────────────────────────────────
  0       2     int16    api_key
  2       2     int16    api_version
  4       4     int32    correlation_id
  8       2     int16    client_id length  (-1 = NULL)
  10      N     bytes    client_id UTF-8 string (N = length above)

NOTE: The 4-byte frame length prefix is stripped by the TCP layer before
these bytes arrive here.  Everything in this file operates on the *payload*
(the bytes after the length prefix).

References:
  https://kafka.apache.org/protocol.html#protocol_messages
  https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
"""

import struct
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Well-known API keys  (a partial list — enough for the bootstrap handshake)
# ---------------------------------------------------------------------------
_API_KEY_NAMES: dict[int, str] = {
    0:  "Produce",
    1:  "Fetch",
    2:  "ListOffsets",
    3:  "Metadata",
    4:  "LeaderAndIsr",
    5:  "StopReplica",
    6:  "UpdateMetadata",
    7:  "ControlledShutdown",
    8:  "OffsetCommit",
    9:  "OffsetFetch",
    10: "FindCoordinator",
    11: "JoinGroup",
    12: "Heartbeat",
    13: "LeaveGroup",
    14: "SyncGroup",
    15: "DescribeGroups",
    16: "ListGroups",
    17: "SaslHandshake",
    18: "ApiVersions",
    19: "CreateTopics",
    20: "DeleteTopics",
    36: "SaslAuthenticate",
    37: "CreatePartitions",
}


def api_key_name(key: int) -> str:
    """Return the human-readable name for *key*, or 'Unknown(N)' if not found."""
    return _API_KEY_NAMES.get(key, f"Unknown({key})")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------
@dataclass
class RequestHeader:
    """
    Parsed Kafka request header (v0 / v1 — the pre-flexible format).

    All header versions share the same four fields; v2+ ("flexible" headers)
    add a tagged-fields section, which we don't handle yet.
    """
    api_key:        int            # int16
    api_version:    int            # int16
    correlation_id: int            # int32
    client_id:      Optional[str]  # nullable UTF-8 string; None if length == -1

    # ── derived properties ──────────────────────────────────────────────────

    @property
    def api_name(self) -> str:
        """Human-readable API key name, e.g. 'ApiVersions'."""
        return api_key_name(self.api_key)

    @property
    def header_size(self) -> int:
        """
        Total bytes consumed from the payload by this header.

        Fixed part:  2 (api_key) + 2 (api_version) + 4 (corr_id) + 2 (len) = 10
        Variable:    len(client_id.encode()) if client_id is not None else 0
        """
        client_id_bytes = len(self.client_id.encode("utf-8")) if self.client_id is not None else 0
        return 10 + client_id_bytes

    def pretty(self) -> str:
        """
        Return a multi-line human-readable summary of the header fields,
        annotated with their byte offsets and raw values for easy cross-
        referencing against the Kafka protocol documentation.

        Example output:

          ┌─ Kafka Request Header ──────────────────────────────────┐
          │  api_key        = 18   (0x0012)  ApiVersions            │
          │  api_version    = 0    (0x0000)                         │
          │  correlation_id = 1    (0x00000001)                     │
          │  client_id      = "kafka-python-producer-1"  (23 bytes) │
          └─ header size: 33 bytes ─────────────────────────────────┘
        """
        cid_repr: str
        if self.client_id is None:
            cid_repr = "NULL"
        else:
            cid_repr = f'"{self.client_id}"  ({len(self.client_id.encode())} bytes)'

        width = 58
        bar   = "─" * width

        lines = [
            f"  ┌─ Kafka Request Header {bar[22:]}┐",
            f"  │  {'api_key':<16} = {self.api_key:<6}(0x{self.api_key:04x})  {self.api_name:<22}│",
            f"  │  {'api_version':<16} = {self.api_version:<6}(0x{self.api_version:04x}){'':>30}│",
            f"  │  {'correlation_id':<16} = {self.correlation_id:<6}(0x{self.correlation_id:08x}){'':>24}│",
            f"  │  {'client_id':<16} = {cid_repr:<42}│",
            f"  └─ header size: {self.header_size} bytes {bar[self.header_size // 2 + 16:]}┘",
        ]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

# Minimum bytes needed before we can even attempt a parse:
#   2 (api_key) + 2 (api_version) + 4 (correlation_id) + 2 (client_id len) = 10
_MIN_HEADER_BYTES = 10


class ParseError(ValueError):
    """Raised when the payload is too short or contains invalid data."""


def parse_request_header(payload: bytes) -> RequestHeader:
    """
    Parse a Kafka request header from *payload*.

    *payload* is the raw bytes after the 4-byte frame length prefix — i.e.
    exactly the bytes your TCP server already reads in Day 1.

    Returns a :class:`RequestHeader` dataclass.
    Raises :class:`ParseError` on malformed input.

    Layout parsed:
    ┌──────┬──────┬──────────┬──────────────┬────────────────────┐
    │ 0..1 │ 2..3 │  4..7    │    8..9      │ 10..(10+N-1)       │
    │ key  │ ver  │ corr_id  │ client_id_len│ client_id (N bytes)│
    └──────┴──────┴──────────┴──────────────┴────────────────────┘
    """
    if len(payload) < _MIN_HEADER_BYTES:
        raise ParseError(
            f"payload too short: need at least {_MIN_HEADER_BYTES} bytes, "
            f"got {len(payload)}"
        )

    # ── Fixed-width fields ──────────────────────────────────────────────────
    # struct format: > = big-endian
    #                h = int16 (signed short)   × 2
    #                i = int32 (signed int)      × 1
    #                h = int16 (signed short)   × 1  ← client_id length
    api_key, api_version, correlation_id, client_id_len = struct.unpack_from(
        ">hhih", payload, offset=0
    )
    # offset 0: api_key       (2 bytes)  → ">h"
    # offset 2: api_version   (2 bytes)  → "h"
    # offset 4: correlation_id(4 bytes)  → "i"
    # offset 8: client_id_len (2 bytes)  → "h"   total consumed: 10 bytes

    # ── client_id string ───────────────────────────────────────────────────
    if client_id_len == -1:
        # Kafka's nullable string sentinel: -1 means NULL (no client_id sent)
        client_id: Optional[str] = None
    elif client_id_len < -1:
        raise ParseError(f"invalid client_id length: {client_id_len}")
    else:
        start = _MIN_HEADER_BYTES          # byte 10
        end   = start + client_id_len      # byte 10 + N

        if len(payload) < end:
            raise ParseError(
                f"payload truncated: client_id claims {client_id_len} bytes "
                f"but only {len(payload) - start} remain"
            )

        client_id = payload[start:end].decode("utf-8")

    return RequestHeader(
        api_key        = api_key,
        api_version    = api_version,
        correlation_id = correlation_id,
        client_id      = client_id,
    )


# ---------------------------------------------------------------------------
# Supported API versions we advertise to clients
# ---------------------------------------------------------------------------
# Format: (api_key, min_version, max_version)
#
# Rule: only advertise versions you actually handle.  Advertising a version
# you don't implement means a client will send requests in that format and
# your broker won't be able to decode them — silent corruption.
#
# For this learning broker we advertise a conservative v0 baseline for the
# APIs involved in the bootstrap handshake (ApiVersions + Metadata) plus
# the minimum needed for a producer (Produce).  The remaining entries are
# included so kafka-python's version negotiation succeeds without errors;
# we'll implement them in later days.
SUPPORTED_APIS: list[tuple[int, int, int]] = [
    # (api_key, min_version, max_version)
    #
    # kafka-python infers broker version from these ranges.
    # MetadataRequest[5] (key=3, v5) → detected as Kafka ≥1.0.0
    # → sends Produce v3+ (RecordBatch/magic=2) instead of old MessageSet.
    (0,  0,  7),  # Produce          — Day 6  (v3+ = RecordBatch/magic=2)
    (1,  0,  4),  # Fetch            — Day 7  (capped at v4: no session mgmt)
    (2,  0,  2),  # ListOffsets      — Day 7  (v2 for ≥1.0.0 compat)
    (3,  0,  5),  # Metadata         — Day 4  (v5 → ≥1.0.0 detection)
    (10, 0,  0),  # FindCoordinator  — Day 8  (v0 is all kafka-python needs)
    (11, 0,  1),  # JoinGroup        — Day 9  (v1 adds rebalance_timeout)
    (12, 0,  1),  # Heartbeat        — Day 11 (v1 adds throttle_time_ms)
    (14, 0,  1),  # SyncGroup        — Day 10 (v1 adds throttle_time_ms)
    (18, 0,  0),  # ApiVersions      — Day 3
]


# ---------------------------------------------------------------------------
# Response encoding helpers
# ---------------------------------------------------------------------------

def build_frame(payload: bytes) -> bytes:
    """
    Wrap *payload* in a 4-byte big-endian length prefix.

    Every Kafka response (and request) on the wire is:
        [int32 length][payload bytes]

    This function produces that framed form.
    """
    return struct.pack(">I", len(payload)) + payload


def build_api_versions_response(correlation_id: int, api_version: int) -> bytes:
    """
    Build a complete, framed ApiVersions response.

    Handles v0, v1, and v2 of the response format:

    v0 payload layout:
    ┌──────────────┬────────────┬──────────────────────────────────────┐
    │ correlation  │ error_code │ api_keys array                       │
    │ _id (int32)  │  (int16)   │ [count(int32)] [key min max]*        │
    └──────────────┴────────────┴──────────────────────────────────────┘
    Each array entry: api_key(int16) min_version(int16) max_version(int16)

    v1 / v2 add a throttle_time_ms (int32) field AFTER the array.
    (v3+ uses flexible/compact encoding — not implemented here.)

    Parameters
    ----------
    correlation_id:
        Copied from the request header so the client can match this
        response to its outstanding request.
    api_version:
        The version number from the *request* header — determines which
        response format to use.

    Returns
    -------
    bytes
        A ready-to-send frame: 4-byte length prefix + full response payload.
    """
    # ── Response header ─────────────────────────────────────────────────────
    # Every Kafka response starts with the correlation_id (4 bytes).
    # (The client uses this to route the response to the right waiting future.)
    resp_header = struct.pack(">i", correlation_id)

    # ── API versions array ───────────────────────────────────────────────────
    # Encode the array: first its length (int32), then each 6-byte entry.
    #   struct format ">i"  → big-endian signed int32  (array element count)
    #   struct format ">hhh" → three big-endian signed int16s per entry
    api_array  = struct.pack(">i", len(SUPPORTED_APIS))
    for api_key, min_ver, max_ver in SUPPORTED_APIS:
        api_array += struct.pack(">hhh", api_key, min_ver, max_ver)

    # ── Response body (version-dependent) ───────────────────────────────────
    error_code = 0  # 0 = no error

    if api_version == 0:
        # v0: error_code + array (no throttle field)
        resp_body = struct.pack(">h", error_code) + api_array

    else:
        # v1, v2: error_code + array + throttle_time_ms
        # throttle_time_ms: how many ms the broker is rate-limiting this client.
        # We never throttle, so always 0.
        throttle_time_ms = 0
        resp_body = (
            struct.pack(">h", error_code)
            + api_array
            + struct.pack(">i", throttle_time_ms)
        )
        # Note: v3+ switches to "flexible" encoding (compact arrays + tagged
        # fields using unsigned varints).  We don't implement that yet.

    payload = resp_header + resp_body
    return build_frame(payload)


# ---------------------------------------------------------------------------
# Hardcoded broker identity
# ---------------------------------------------------------------------------
# We are the only broker in this single-node cluster.  Every Metadata response
# will advertise exactly one broker — ourselves — and claim we are the leader
# of every partition of every topic.
#
# In a real cluster these values come from the broker's config file and from
# ZooKeeper/KRaft consensus.  Here we just hard-code them.

BROKER_NODE_ID: int   = 1
BROKER_HOST:    bytes = b"localhost"
BROKER_PORT:    int   = 9092


# ---------------------------------------------------------------------------
# Metadata request parser
# ---------------------------------------------------------------------------

def parse_metadata_request_topics(payload: bytes, header_size: int) -> list[str]:
    """
    Extract the list of requested topic names from a Metadata v0 request body.

    The body (bytes after the header) is a standard Kafka ARRAY of STRING:
        int32  topic_count          (number of topics; 0 means "all topics")
        for each topic:
            int16  name_length
            bytes  name_length bytes of UTF-8

    Returns a list of topic name strings.
    Returns an empty list when topic_count == 0 (client asked for all topics)
    or when the body is malformed.

    Parameters
    ----------
    payload:
        The full request payload (header + body together).
    header_size:
        Number of bytes consumed by the request header (from RequestHeader.header_size).
        The body starts at payload[header_size:].
    """
    body = payload[header_size:]
    if len(body) < 4:
        return []

    # ── Array count ─────────────────────────────────────────────────────────
    # struct ">i" → big-endian signed int32
    (count,) = struct.unpack_from(">i", body, 0)
    if count <= 0:
        # count == 0  → client asked for "all topics" (we have none)
        # count == -1 → null array (unusual but handle gracefully)
        return []

    # ── Topic name strings ───────────────────────────────────────────────────
    offset = 4
    topics: list[str] = []

    for _ in range(count):
        if len(body) < offset + 2:
            break
        # Each string: int16 length prefix (signed; -1 = NULL string)
        (name_len,) = struct.unpack_from(">h", body, offset)
        offset += 2

        if name_len <= 0:
            continue  # NULL or empty — skip
        if len(body) < offset + name_len:
            break     # truncated frame — stop early

        topics.append(body[offset : offset + name_len].decode("utf-8"))
        offset += name_len

    return topics


# ---------------------------------------------------------------------------
# Metadata response builder
# ---------------------------------------------------------------------------

def build_metadata_response(correlation_id: int, topics: list[str], api_version: int = 0) -> bytes:
    """
    Build a complete, framed Metadata v0 response.

    Binary layout (v0):
    ┌─────────────┬───────────────────────────┬──────────────────────────────┐
    │ corr_id     │ brokers array             │ topics array                 │
    │ (int32)     │ [count][node_id host port]│ [count][err name partitions] │
    └─────────────┴───────────────────────────┴──────────────────────────────┘

    Broker entry (one per entry in brokers array):
        node_id  INT32
        host     STRING  (int16 length + UTF-8 bytes)
        port     INT32

    Topic entry:
        error_code      INT16
        name            STRING
        partitions      ARRAY of:
            error_code  INT16
            partition   INT32   (partition index, 0-based)
            leader      INT32   (node_id of the leader broker)
            replicas    [INT32] (all brokers that have a copy)
            isr         [INT32] (in-sync replicas — caught-up followers)

    Parameters
    ----------
    correlation_id:
        Copied from the request; lets the client match response to request.
    topics:
        List of topic names to include.  For each, we claim broker 1 is the
        leader of partition 0.  If empty, the topics array in the response
        will also be empty (client asked for "all topics" but we have none).
    """
    # ── Response header ──────────────────────────────────────────────────────
    resp_header = struct.pack(">i", correlation_id)

    # ── Brokers section ──────────────────────────────────────────────────────
    # v0: node_id | host | port
    # v1: node_id | host | port | rack (NULLABLE_STRING, -1 = NULL)
    broker_entry = (
        struct.pack(">i", BROKER_NODE_ID)                  # node_id
        + struct.pack(">h", len(BROKER_HOST)) + BROKER_HOST  # host
        + struct.pack(">i", BROKER_PORT)                   # port
    )
    if api_version >= 1:
        # rack: ff ff = int16(-1) = NULL (no rack assignment)
        broker_entry += struct.pack(">h", -1)
    brokers = struct.pack(">i", 1) + broker_entry  # array count=1

    # ── controller_id (v1 only, sits between brokers array and topics array) ──
    # The "controller" is the broker that manages partition leader elections.
    # In our single-node cluster, we are the controller.
    # v0 has no controller_id field at all; v1 adds it as a standalone INT32.
    controller_block = b""
    if api_version >= 1:
        controller_block = struct.pack(">i", BROKER_NODE_ID)

    # ── Topics section ───────────────────────────────────────────────────────
    if not topics:
        # Client requested all topics; we have none yet.
        topic_bytes = struct.pack(">i", 0)  # empty array
    else:
        topic_bytes = struct.pack(">i", len(topics))

        for topic_name in topics:
            topic_b = topic_name.encode("utf-8")

            # Partition entry is the same across v0 and v1.
            # Wire: [part_err][part_id][leader][replica_count][replica_0][isr_count][isr_0]
            #         int16    int32    int32      int32          int32      int32     int32
            partition = (
                struct.pack(">h", 0)                   # partition error_code = 0
                + struct.pack(">i", 0)                 # partition_id = 0
                + struct.pack(">i", BROKER_NODE_ID)    # leader = us
                + struct.pack(">i", 1)                 # replicas: 1 entry
                + struct.pack(">i", BROKER_NODE_ID)    # replica[0] = us
                + struct.pack(">i", 1)                 # isr: 1 entry
                + struct.pack(">i", BROKER_NODE_ID)    # isr[0] = us
            )

            # Topic entry fields differ by version.
            # v0: error_code | name | [partitions]
            # v1: error_code | name | is_internal(bool) | [partitions]
            topic_entry = (
                struct.pack(">h", 0)                          # topic error_code = 0
                + struct.pack(">h", len(topic_b)) + topic_b  # topic name
            )
            if api_version >= 1:
                # is_internal BOOLEAN: false (0x00) for all user topics
                topic_entry += struct.pack(">?", False)
            topic_entry += struct.pack(">i", 1) + partition   # 1 partition
            topic_bytes += topic_entry

    payload = resp_header + brokers + controller_block + topic_bytes
    return build_frame(payload)


# ---------------------------------------------------------------------------
# Produce response builder
# ---------------------------------------------------------------------------
# The Produce response mirrors the request's topic/partition structure.
# Version history (fields added per version):
#
#   v0:  [responses](topic, [(partition, error_code, base_offset)])
#   v1:  + throttle_time_ms (INT32) at top level
#   v2:  partition_response gets log_append_time (INT64)
#   v5:  partition_response gets log_start_offset (INT64)
#   v7:  same schema as v5/v6  ← kafka-python sends this
#
# kafka-python matches request version → response version, so when it sends
# Produce v7 it decodes the response using ProduceResponse_v7 (= v5 schema).


def build_produce_response(
    correlation_id: int,
    results: list[tuple[str, int, int, int]],
    api_version: int = 7,
) -> bytes:
    """
    Build a framed Produce response.

    Parameters
    ----------
    correlation_id:
        Copied from the request header.
    results:
        One tuple per (topic, partition) that was in the request:
            (topic_name, partition, error_code, base_offset)
        error_code 0 = success.
    api_version:
        Produce API version of the request.  Determines which optional
        fields to include.

    Wire layout (response header + body):
        correlation_id     INT32
        [responses]
            topic          STRING
            [partition_responses]
                partition        INT32
                error_code       INT16
                base_offset      INT64
                log_append_time  INT64   (v2+)   -1 = broker uses CreateTime
                log_start_offset INT64   (v5+)    0 = start of log
        throttle_time_ms   INT32   (v1+)    0 = no throttle
    """
    resp_header = struct.pack(">i", correlation_id)

    # Group results by topic so we can build one topic entry per topic name.
    # In practice a single Produce request usually has one topic, but the
    # protocol allows many.
    from collections import defaultdict
    by_topic: dict[str, list[tuple[int, int, int]]] = defaultdict(list)
    for (topic, partition, error_code, base_offset) in results:
        by_topic[topic].append((partition, error_code, base_offset))

    # ── Build each topic entry ────────────────────────────────────────────────
    topics_bytes = struct.pack(">i", len(by_topic))   # topic array count

    for topic_name, parts in by_topic.items():
        topic_b = topic_name.encode("utf-8")
        part_array = struct.pack(">i", len(parts))    # partition array count

        for (partition, error_code, base_offset) in parts:
            entry = (
                struct.pack(">i", partition)    # partition  INT32
                + struct.pack(">h", error_code) # error_code INT16
                + struct.pack(">q", base_offset) # base_offset INT64
            )
            if api_version >= 2:
                # log_append_time: -1 means "use CreateTime from the batch"
                entry += struct.pack(">q", -1)
            if api_version >= 5:
                # log_start_offset: 0 = beginning of the partition log
                entry += struct.pack(">q", 0)

            part_array += entry

        topics_bytes += struct.pack(">h", len(topic_b)) + topic_b + part_array

    payload = resp_header + topics_bytes

    if api_version >= 1:
        # throttle_time_ms: 0 = no throttling applied
        payload += struct.pack(">i", 0)

    return build_frame(payload)


# ---------------------------------------------------------------------------
# Fetch request parser  (API key 1)
# ---------------------------------------------------------------------------
# Version ladder (fields added per version):
#   v0: replica_id, max_wait_ms, min_bytes, [topics]
#       partition_data: partition, fetch_offset, partition_max_bytes
#   v3: + max_bytes (INT32) before [topics]
#   v4: + isolation_level (INT8) before [topics]
#   v5: partition gets log_start_offset (INT64) after fetch_offset
#   v7: + session_id + session_epoch before [topics];
#       + [forgotten_topics_data] after [topics]
#   v9: partition gets current_leader_epoch (INT32) before fetch_offset

def parse_fetch_request(
    payload: bytes,
    header_size: int,
    api_version: int,
) -> list[dict]:
    """
    Parse a Fetch v0-v9 request body.

    Returns one dict per (topic, partition) pair:
        topic, partition, fetch_offset, partition_max_bytes,
        max_wait_ms, min_bytes, max_bytes, isolation_level
    """
    body = payload[header_size:]
    pos  = 0

    def rd_i8() -> int:
        nonlocal pos
        v = struct.unpack_from(">b", body, pos)[0]; pos += 1; return v

    def rd_i32() -> int:
        nonlocal pos
        v = struct.unpack_from(">i", body, pos)[0]; pos += 4; return v

    def rd_i64() -> int:
        nonlocal pos
        v = struct.unpack_from(">q", body, pos)[0]; pos += 8; return v

    def rd_str() -> str | None:
        nonlocal pos
        n = struct.unpack_from(">h", body, pos)[0]; pos += 2
        if n == -1:
            return None
        s = body[pos : pos + n].decode("utf-8"); pos += n
        return s

    _replica_id = rd_i32()
    max_wait_ms = rd_i32()
    min_bytes   = rd_i32()
    max_bytes   = (2**31 - 1) if api_version < 3 else rd_i32()

    isolation_level = 0
    if api_version >= 4:
        isolation_level = rd_i8()

    if api_version >= 7:
        _session_id    = rd_i32()
        _session_epoch = rd_i32()

    topic_count = rd_i32()
    results: list[dict] = []

    for _ in range(topic_count):
        topic_name      = rd_str()
        partition_count = rd_i32()

        for _ in range(partition_count):
            partition = rd_i32()
            if api_version >= 9:
                _current_leader_epoch = rd_i32()
            fetch_offset = rd_i64()
            if api_version >= 5:
                _log_start_offset = rd_i64()
            partition_max_bytes = rd_i32()

            results.append({
                "topic":               topic_name,
                "partition":           partition,
                "fetch_offset":        fetch_offset,
                "partition_max_bytes": partition_max_bytes,
                "max_wait_ms":         max_wait_ms,
                "min_bytes":           min_bytes,
                "max_bytes":           max_bytes,
                "isolation_level":     isolation_level,
            })

    if api_version >= 7:
        forgotten_count = rd_i32()
        for _ in range(forgotten_count):
            rd_str()
            n = rd_i32()
            for _ in range(n):
                rd_i32()

    return results


# ---------------------------------------------------------------------------
# Fetch response builder  (API key 1)
# ---------------------------------------------------------------------------
# Wire layout (Fetch v4 — what we advertise as max):
#
#   correlation_id        INT32
#   throttle_time_ms      INT32       (v1+)
#   [responses]
#     topic               STRING
#     [partition_responses]
#       partition             INT32
#       error_code            INT16    0 = NONE
#       high_watermark        INT64    next offset after all written data
#       last_stable_offset    INT64    (v4+)  -1 = non-transactional
#       log_start_offset      INT64    (v5+)   0 = start of log
#       aborted_transactions  ARRAY    (v4+)  -1 = null
#       records               RECORDS  INT32 size + RecordBatch bytes, or -1
#
# v7 inserts top-level error_code (INT16) + session_id (INT32)
# between throttle_time_ms and [responses].

def build_fetch_response(
    correlation_id: int,
    results: list[tuple[str, int, int, int, bytes | None]],
    api_version: int = 4,
) -> bytes:
    """
    Build a framed Fetch response.

    results: list of (topic, partition, error_code, high_watermark, records_bytes)
        records_bytes: raw RecordBatch bytes to return, or None if no data yet.
    """
    from collections import defaultdict

    resp_header = struct.pack(">i", correlation_id)
    body        = b""

    if api_version >= 1:
        body += struct.pack(">i", 0)   # throttle_time_ms

    if api_version >= 7:
        body += struct.pack(">h", 0)   # top-level error_code = NONE
        body += struct.pack(">i", 0)   # session_id = 0 (stateless)

    by_topic: dict[str, list] = defaultdict(list)
    for (topic, partition, error_code, high_watermark, records_bytes) in results:
        by_topic[topic].append((partition, error_code, high_watermark, records_bytes))

    body += struct.pack(">i", len(by_topic))

    for topic_name, parts in by_topic.items():
        topic_b  = topic_name.encode("utf-8")
        body    += struct.pack(">h", len(topic_b)) + topic_b
        body    += struct.pack(">i", len(parts))

        for (partition, error_code, high_watermark, records_bytes) in parts:
            body += struct.pack(">i", partition)
            body += struct.pack(">h", error_code)
            body += struct.pack(">q", high_watermark)
            if api_version >= 4:
                body += struct.pack(">q", -1)   # last_stable_offset
            if api_version >= 5:
                body += struct.pack(">q", 0)    # log_start_offset
            if api_version >= 4:
                body += struct.pack(">i", -1)   # aborted_transactions = null
            if records_bytes:
                body += struct.pack(">i", len(records_bytes)) + records_bytes
            else:
                body += struct.pack(">i", -1)   # null records

    return build_frame(resp_header + body)


# ---------------------------------------------------------------------------
# ListOffsets request parser  (API key 2)
# ---------------------------------------------------------------------------
# timestamp = -2  →  earliest offset in the partition
# timestamp = -1  →  latest offset (= high_watermark)
#
# v0: partition + timestamp + max_num_offsets
# v1: partition + timestamp  (max_num_offsets removed)
# v2: + isolation_level (INT8) before [topics]

def parse_list_offsets_request(
    payload: bytes,
    header_size: int,
    api_version: int,
) -> list[dict]:
    """
    Parse a ListOffsets v0-v2 request body.
    Returns one dict per (topic, partition): {topic, partition, timestamp}.
    """
    body = payload[header_size:]
    pos  = 0

    def rd_i32() -> int:
        nonlocal pos
        v = struct.unpack_from(">i", body, pos)[0]; pos += 4; return v

    def rd_i64() -> int:
        nonlocal pos
        v = struct.unpack_from(">q", body, pos)[0]; pos += 8; return v

    def rd_str() -> str | None:
        nonlocal pos
        n = struct.unpack_from(">h", body, pos)[0]; pos += 2
        if n == -1:
            return None
        s = body[pos : pos + n].decode("utf-8"); pos += n
        return s

    _replica_id = rd_i32()
    if api_version >= 2:
        pos += 1   # isolation_level INT8

    topic_count = rd_i32()
    results: list[dict] = []

    for _ in range(topic_count):
        topic_name      = rd_str()
        partition_count = rd_i32()
        for _ in range(partition_count):
            partition = rd_i32()
            timestamp = rd_i64()
            if api_version == 0:
                _max_num_offsets = rd_i32()
            results.append({"topic": topic_name, "partition": partition, "timestamp": timestamp})

    return results


# ---------------------------------------------------------------------------
# ListOffsets response builder  (API key 2)
# ---------------------------------------------------------------------------
# v0: [responses](topic, [(partition, error_code, [offsets INT64])])
# v1+: throttle_time_ms + [responses](topic, [(partition, error_code, timestamp, offset)])

def build_list_offsets_response(
    correlation_id: int,
    results: list[tuple[str, int, int, int, int]],
    api_version: int = 1,
) -> bytes:
    """
    Build a framed ListOffsets response.
    results: list of (topic, partition, error_code, timestamp, offset).
    """
    from collections import defaultdict

    resp_header = struct.pack(">i", correlation_id)
    body        = b""

    if api_version >= 2:
        body += struct.pack(">i", 0)   # throttle_time_ms

    by_topic: dict[str, list] = defaultdict(list)
    for (topic, partition, error_code, timestamp, offset) in results:
        by_topic[topic].append((partition, error_code, timestamp, offset))

    body += struct.pack(">i", len(by_topic))

    for topic_name, parts in by_topic.items():
        topic_b  = topic_name.encode("utf-8")
        body    += struct.pack(">h", len(topic_b)) + topic_b
        body    += struct.pack(">i", len(parts))
        for (partition, error_code, timestamp, offset) in parts:
            body += struct.pack(">i", partition)
            body += struct.pack(">h", error_code)
            if api_version == 0:
                body += struct.pack(">i", 1) + struct.pack(">q", offset)
            else:
                body += struct.pack(">q", timestamp)
                body += struct.pack(">q", offset)

    return build_frame(resp_header + body)


# ---------------------------------------------------------------------------
# FindCoordinator response builder  (API key 10)
# ---------------------------------------------------------------------------
# Wire format:
#   v0: error_code (INT16) | coordinator_id (INT32) | host (STRING) | port (INT32)
#   v1: adds throttle_time_ms (INT32) at the front and error_message (STRING)
#       after error_code
#
# Since we are a single-broker cluster we always point the client at ourselves.
# error_code = 0 (NONE), coordinator_id = 1, host = "localhost", port = 9092.

def build_find_coordinator_response(
    correlation_id: int,
    api_version: int = 0,
    error_code: int = 0,
    coordinator_id: int = 1,
    host: str = "localhost",
    port: int = 9092,
) -> bytes:
    """
    Build a framed FindCoordinator (GroupCoordinator) response.

    For our single-broker setup we always claim that *we* are the coordinator.
    The response is identical for any consumer group name.
    """
    resp_header = struct.pack(">i", correlation_id)
    host_b = host.encode("utf-8")

    body = b""
    if api_version >= 1:
        # v1+ prepends throttle_time_ms
        body += struct.pack(">i", 0)           # throttle_time_ms

    body += struct.pack(">h", error_code)      # error_code INT16

    if api_version >= 1:
        # v1+ includes a human-readable error message (nullable STRING)
        body += struct.pack(">h", -1)          # error_message = null

    body += struct.pack(">i", coordinator_id)  # coordinator_id INT32
    body += struct.pack(">h", len(host_b)) + host_b  # host STRING
    body += struct.pack(">i", port)            # port INT32

    return build_frame(resp_header + body)


# ---------------------------------------------------------------------------
# JoinGroup response builder  (API key 11)
# ---------------------------------------------------------------------------
# Wire format:
#   v0/v1: error_code | generation_id | group_protocol | leader_id |
#           member_id | members[]
#   v2+:   throttle_time_ms | (same as v0)
#
# The members array is only populated for the group leader.
# Non-leader members receive an empty array — they get their assignments
# later via SyncGroup.
#
# members element:
#   v0–v4: member_id (STRING) | member_metadata (BYTES)
#   v5+:   member_id (STRING) | group_instance_id (nullable STRING)
#           | member_metadata (BYTES)

def build_join_group_response(
    correlation_id: int,
    api_version:    int,
    error_code:     int,
    generation_id:  int,
    protocol_name:  str,
    leader_id:      str,
    member_id:      str,
    members: list[tuple[str, bytes]],  # (member_id, metadata) — leader only
) -> bytes:
    """
    Build a framed JoinGroup response.

    *members* should be the full list of (member_id, metadata) when
    responding to the leader, and an empty list for everyone else.
    """
    resp_header = struct.pack(">i", correlation_id)
    body = b""

    if api_version >= 2:
        body += struct.pack(">i", 0)            # throttle_time_ms

    body += struct.pack(">h", error_code)       # error_code INT16
    body += struct.pack(">i", generation_id)    # generation_id INT32

    proto_b = protocol_name.encode("utf-8")
    body += struct.pack(">h", len(proto_b)) + proto_b  # group_protocol STRING

    leader_b = leader_id.encode("utf-8")
    body += struct.pack(">h", len(leader_b)) + leader_b  # leader_id STRING

    member_b = member_id.encode("utf-8")
    body += struct.pack(">h", len(member_b)) + member_b  # member_id STRING

    # members array
    body += struct.pack(">i", len(members))
    for (mid, metadata) in members:
        mid_b = mid.encode("utf-8")
        body += struct.pack(">h", len(mid_b)) + mid_b   # member_id STRING
        if api_version >= 5:
            body += struct.pack(">h", -1)               # group_instance_id null
        # metadata BYTES: INT32 length prefix + raw bytes
        body += struct.pack(">i", len(metadata)) + metadata

    return build_frame(resp_header + body)


# ---------------------------------------------------------------------------
# SyncGroup helpers & response builder  (API key 14)
# ---------------------------------------------------------------------------
# The SyncGroup response carries a single BYTES field: member_assignment.
# Those bytes are NOT a raw blob from the request — they are a serialized
# ConsumerProtocolAssignment structure:
#
#   version         INT16           (always 0)
#   partitions      ARRAY
#     topic         STRING
#     partitions    ARRAY INT32
#   user_data       BYTES (nullable, use -1 = null)
#
# This nested structure is wrapped with an INT32 length prefix to form the
# BYTES field in the outer response.

def encode_member_assignment(topic_partitions: dict[str, list[int]]) -> bytes:
    """
    Encode a {topic: [partition, ...]} map into the Kafka
    ConsumerProtocolAssignment binary format (version 0).

    Returns the raw bytes (without the INT32 length prefix —
    the caller wraps them into a BYTES field).
    """
    buf = struct.pack(">h", 0)                      # version INT16 = 0
    buf += struct.pack(">i", len(topic_partitions)) # topic array count
    for topic, parts in topic_partitions.items():
        topic_b = topic.encode("utf-8")
        buf += struct.pack(">h", len(topic_b)) + topic_b   # topic STRING
        buf += struct.pack(">i", len(parts))               # partition array count
        for p in parts:
            buf += struct.pack(">i", p)                    # partition INT32
    buf += struct.pack(">i", -1)                    # user_data BYTES = null
    return buf


def build_sync_group_response(
    correlation_id: int,
    api_version:    int,
    error_code:     int,
    member_assignment_bytes: bytes,  # pre-encoded ConsumerProtocolAssignment
) -> bytes:
    """
    Build a framed SyncGroup response.

    v0: error_code | member_assignment (BYTES)
    v1+: throttle_time_ms | error_code | member_assignment (BYTES)
    """
    resp_header = struct.pack(">i", correlation_id)
    body = b""

    if api_version >= 1:
        body += struct.pack(">i", 0)                        # throttle_time_ms

    body += struct.pack(">h", error_code)                   # error_code INT16

    # member_assignment as BYTES: INT32 length + raw bytes
    body += struct.pack(">i", len(member_assignment_bytes)) + member_assignment_bytes

    return build_frame(resp_header + body)


# ---------------------------------------------------------------------------
# Heartbeat response builder  (API key 12)
# ---------------------------------------------------------------------------
# Wire format:
#   v0:  error_code (INT16)
#   v1+: throttle_time_ms (INT32) | error_code (INT16)
#
# Request fields (all versions up to v1):
#   group (STRING) | generation_id (INT32) | member_id (STRING)
# v2+ adds: group_instance_id (nullable STRING)
#
# Error codes we use:
#   0  — NONE            (heartbeat accepted, member is healthy)
#   22 — ILLEGAL_GENERATION (stale generation_id; consumer must rejoin)
#   25 — UNKNOWN_MEMBER_ID  (member_id not recognised; consumer must rejoin)
#   27 — REBALANCE_IN_PROGRESS (group is currently rebalancing)

def build_heartbeat_response(
    correlation_id: int,
    api_version:    int,
    error_code:     int = 0,
) -> bytes:
    """Build a framed Heartbeat response."""
    resp_header = struct.pack(">i", correlation_id)
    body = b""
    if api_version >= 1:
        body += struct.pack(">i", 0)        # throttle_time_ms
    body += struct.pack(">h", error_code)   # error_code INT16
    return build_frame(resp_header + body)


# ---------------------------------------------------------------------------
# Variable-length integer decoding (used inside RecordBatch records)
# ---------------------------------------------------------------------------
# Kafka uses zigzag encoding for signed integers inside Records.
# This is the same encoding Protocol Buffers uses for "sint32" / "sint64".
#
# Encoding rule:  n ≥ 0 → 2*n          n < 0 → -2*n - 1
# So:   0 → 0x00,  -1 → 0x01,   1 → 0x02,  -2 → 0x03 …
#
# The raw unsigned value is stored in 7-bit groups, LSB first.
# The MSB of each byte is a "more bytes follow" flag (1=more, 0=last).

def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    """
    Read one zigzag-encoded varint from *data* starting at *offset*.

    Returns (decoded_signed_value, new_offset_after_this_varint).
    """
    raw   = 0
    shift = 0
    while True:
        byte = data[offset]; offset += 1
        raw |= (byte & 0x7F) << shift
        if not (byte & 0x80):   # MSB=0 → this is the last byte
            break
        shift += 7
    # zigzag decode: undo (n << 1) ^ (n >> 63)
    return ((raw >> 1) ^ -(raw & 1)), offset


# ---------------------------------------------------------------------------
# Produce request parser
# ---------------------------------------------------------------------------

def parse_produce_request(payload: bytes, header_size: int, api_version: int = 7) -> list[dict]:
    """
    Parse a Produce v0–v8 request body (bytes after the request header).

    Version layout differences:
      v0–v2:  acks | timeout_ms | [topic_data]
      v3–v8:  transactional_id | acks | timeout_ms | [topic_data]

    Returns one dict per (topic, partition) pair:
        topic, partition, acks, timeout_ms, transactional_id,
        record_set_size, record_set (raw RecordBatch bytes).
    """
    body   = payload[header_size:]
    offset = 0

    def rd_i16() -> int:
        nonlocal offset
        v = struct.unpack_from(">h", body, offset)[0]; offset += 2; return v

    def rd_i32() -> int:
        nonlocal offset
        v = struct.unpack_from(">i", body, offset)[0]; offset += 4; return v

    def rd_nstr() -> str | None:
        """NULLABLE_STRING: int16 length (-1 = NULL) + UTF-8 bytes."""
        nonlocal offset
        n = rd_i16()
        if n == -1:
            return None
        s = body[offset : offset + n].decode("utf-8"); offset += n; return s

    # transactional_id was added in v3
    transactional_id: str | None = None
    if api_version >= 3:
        transactional_id = rd_nstr()

    acks       = rd_i16()
    timeout_ms = rd_i32()
    topic_count = rd_i32()

    results: list[dict] = []
    for _ in range(topic_count):
        topic_name      = rd_nstr()
        partition_count = rd_i32()

        for _ in range(partition_count):
            partition       = rd_i32()
            record_set_size = rd_i32()   # INT32 size; -1 = null records

            if record_set_size <= 0:
                record_set = b""
            else:
                record_set  = body[offset : offset + record_set_size]
                offset     += record_set_size

            results.append({
                "topic":             topic_name,
                "partition":         partition,
                "acks":              acks,
                "timeout_ms":        timeout_ms,
                "transactional_id":  transactional_id,
                "record_set_size":   record_set_size,
                "record_set":        record_set,
            })

    return results


# ---------------------------------------------------------------------------
# RecordBatch header parser
# ---------------------------------------------------------------------------
# A RecordBatch starts with a fixed 61-byte header at known offsets.
# Everything after byte 61 is the compressed (or uncompressed) records section.

_COMPRESSION_NAMES = {0: "none", 1: "gzip", 2: "snappy", 3: "lz4", 4: "zstd"}
_RECORD_BATCH_HEADER_SIZE = 61


def parse_record_batch_header(data: bytes) -> dict:
    """
    Decode the fixed 61-byte RecordBatch header.

    RecordBatch header field layout:

      Offset  Size  Type    Field
      ------  ----  ------  -------------------------------------------------
       0       8    INT64   base_offset       (0 from producer; set by broker)
       8       4    INT32   batch_length      (bytes from here to end of batch)
      12       4    INT32   partition_leader_epoch
      16       1    INT8    magic             (must be 2)
      17       4    UINT32  crc               (CRC32C of bytes 21 – end)
      21       2    INT16   attributes        (compression | ts_type | flags)
      23       4    INT32   last_offset_delta (last_record_offset – base_offset)
      27       8    INT64   base_timestamp    (ms since epoch, first record)
      35       8    INT64   max_timestamp     (ms since epoch, last record)
      43       8    INT64   producer_id       (-1 = non-idempotent)
      51       2    INT16   producer_epoch    (-1 = non-idempotent)
      53       4    INT32   base_sequence     (-1 = non-idempotent)
      57       4    INT32   records_count
      61     var    bytes   records           (Records section)

    attributes bitmask:
      bits 0–2  compression type  (0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd)
      bit  3    timestamp type    (0=CreateTime, 1=LogAppendTime)
      bit  4    is_transactional
      bit  5    is_control

    Raises ValueError if *data* is too short or magic ≠ 2.
    """
    if len(data) < _RECORD_BATCH_HEADER_SIZE:
        raise ValueError(
            f"RecordBatch is {len(data)} bytes — need at least {_RECORD_BATCH_HEADER_SIZE}"
        )

    base_offset            = struct.unpack_from(">q", data,  0)[0]
    batch_length           = struct.unpack_from(">i", data,  8)[0]
    partition_leader_epoch = struct.unpack_from(">i", data, 12)[0]
    magic                  = struct.unpack_from(">b", data, 16)[0]
    crc                    = struct.unpack_from(">I", data, 17)[0]   # unsigned
    attributes             = struct.unpack_from(">h", data, 21)[0]
    last_offset_delta      = struct.unpack_from(">i", data, 23)[0]
    base_timestamp         = struct.unpack_from(">q", data, 27)[0]
    max_timestamp          = struct.unpack_from(">q", data, 35)[0]
    producer_id            = struct.unpack_from(">q", data, 43)[0]
    producer_epoch         = struct.unpack_from(">h", data, 51)[0]
    base_sequence          = struct.unpack_from(">i", data, 53)[0]
    records_count          = struct.unpack_from(">i", data, 57)[0]

    if magic != 2:
        raise ValueError(f"Unsupported RecordBatch magic={magic} (only magic=2 supported)")

    return {
        "base_offset":            base_offset,
        "batch_length":           batch_length,
        "partition_leader_epoch": partition_leader_epoch,
        "magic":                  magic,
        "crc":                    f"{crc:08x}",
        "compression":            _COMPRESSION_NAMES.get(attributes & 0x07, "unknown"),
        "attributes":             attributes,
        "last_offset_delta":      last_offset_delta,
        "base_timestamp":         base_timestamp,
        "max_timestamp":          max_timestamp,
        "producer_id":            producer_id,
        "producer_epoch":         producer_epoch,
        "base_sequence":          base_sequence,
        "records_count":          records_count,
        "is_transactional":       bool(attributes & 0x10),
        "is_control":             bool(attributes & 0x20),
    }


# ---------------------------------------------------------------------------
# Individual Record parser (within a RecordBatch)
# ---------------------------------------------------------------------------

def parse_records_in_batch(record_set: bytes) -> list[dict]:
    """
    Decode the individual Records from a RecordBatch.

    Skips the 61-byte header and parses each Record using zigzag varints.
    Only handles **uncompressed** batches (attributes & 0x07 == 0).
    Returns an empty list for compressed batches (decompression is Day 7+).

    Record wire format (magic=2):
        length         VARINT    total bytes of this record (not counting this field)
        attributes     INT8      currently always 0
        timestamp_delta VARINT   ms offset from base_timestamp
        offset_delta   VARINT    this record's offset minus base_offset
        key_length     VARINT    -1 = null key; else byte count
        key            bytes
        value_length   VARINT    -1 = null value; else byte count
        value          bytes
        headers_count  VARINT    number of header key/value pairs
        headers        [(VARINT key_len)(key bytes)(VARINT val_len)(val bytes)]

    Returns list of dicts:
        offset_delta    int
        timestamp_delta int
        key             bytes | None
        value           bytes | None
        headers         list[tuple[str, bytes]]
    """
    if len(record_set) < _RECORD_BATCH_HEADER_SIZE:
        return []

    # Check compression — bail if compressed
    attributes  = struct.unpack_from(">h", record_set, 21)[0]
    compression = attributes & 0x07
    if compression != 0:
        return []   # compressed — skip for now

    data   = record_set[_RECORD_BATCH_HEADER_SIZE:]  # records section only
    offset = 0
    parsed: list[dict] = []

    while offset < len(data):
        rec_len, offset = _read_varint(data, offset)
        if rec_len <= 0:
            break
        rec_end = offset + rec_len   # byte just past this record

        # attributes INT8 (plain byte, not a varint)
        _attrs = data[offset]; offset += 1

        ts_delta,  offset = _read_varint(data, offset)
        off_delta, offset = _read_varint(data, offset)

        # key
        key_len, offset = _read_varint(data, offset)
        if key_len < 0:
            key = None
        else:
            key = data[offset : offset + key_len]; offset += key_len

        # value
        val_len, offset = _read_varint(data, offset)
        if val_len < 0:
            value = None
        else:
            value = data[offset : offset + val_len]; offset += val_len

        # headers
        hdr_count, offset = _read_varint(data, offset)
        headers: list[tuple[str, bytes]] = []
        for _ in range(hdr_count):
            hk_len, offset = _read_varint(data, offset)
            hk = data[offset : offset + hk_len].decode("utf-8"); offset += hk_len
            hv_len, offset = _read_varint(data, offset)
            hv = data[offset : offset + hv_len];               offset += hv_len
            headers.append((hk, hv))

        parsed.append({
            "offset_delta":    off_delta,
            "timestamp_delta": ts_delta,
            "key":             key,
            "value":           value,
            "headers":         headers,
        })

        offset = rec_end   # skip to next record (guards against malformed data)

    return parsed
