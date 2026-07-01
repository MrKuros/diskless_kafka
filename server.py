"""
diskless_kafka/server.py
────────────────────────
Day 6: Write RecordBatch to MinIO and send a proper Produce response.

For each incoming frame:
  1. Read the 4-byte length prefix
  2. Read exactly that many payload bytes
  3. Log the raw hex dump
  4. Parse the request header (api_key, api_version, correlation_id, client_id)
  5. Dispatch to the matching handler
  6. Write the encoded response back to the client

Run:
    python server.py

Test:
    python test_client.py
"""

import asyncio
import struct
import logging
import sys

from protocol import (
    ParseError,
    RequestHeader,
    SUPPORTED_APIS,
    build_api_versions_response,
    build_find_coordinator_response,
    build_heartbeat_response,
    build_join_group_response,
    build_leave_group_response,
    build_metadata_response,
    build_offset_commit_response,
    build_offset_fetch_response,
    build_produce_response,
    build_sync_group_response,
    encode_member_assignment,
    parse_metadata_request_topics,
    parse_produce_request,
    parse_record_batch_header,
    parse_records_in_batch,
    parse_request_header,
)
from storage import commit_offset, load_committed_offsets, write_batch

import os
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "9092"))

# ---------------------------------------------------------------------------
# Logging setup — one line per event, timestamps included
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("kafka.raw")


# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------
LENGTH_PREFIX_BYTES = 4  # int32 big-endian


async def _read_exact(reader: asyncio.StreamReader, n: int) -> bytes:
    """Read exactly *n* bytes from *reader*, raising on premature EOF."""
    buf = await reader.readexactly(n)
    return buf


def _hex_dump(data: bytes, bytes_per_row: int = 16) -> str:
    """
    Format *data* as an annotated hex dump:

        0000  00 00 00 1b 00 12 00 03  00 00 00 01 00 09 6b 61  .......... ka
        0010  66 6b 61 2d 70 79 74 68  6f 6e 00 00             fka-python..
    """
    lines = []
    for offset in range(0, len(data), bytes_per_row):
        chunk = data[offset : offset + bytes_per_row]
        hex_part = " ".join(f"{b:02x}" for b in chunk)
        # pad so the printable column lines up
        hex_part = f"{hex_part:<{bytes_per_row * 3 - 1}}"
        printable = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"  {offset:04x}  {hex_part}  {printable}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------
# Each handler receives the parsed header and the full payload bytes, and
# returns a ready-to-send response frame (including the 4-byte length prefix)
# or None if we don't handle that API yet.

# ---------------------------------------------------------------------------
# In-memory group state
# ---------------------------------------------------------------------------
# Keyed by group_id.  Each entry:
#   {
#     "generation_id":  int,           current epoch (starts at 1)
#     "leader_id":      str,           member_id of the elected leader
#     "protocol":       str,           chosen protocol name (e.g. "range")
#     "members": {
#       member_id: {"metadata": bytes, "client_id": str}
#     }
#   }
#
# This is intentionally simple — no persistence, no locks (single-threaded
# asyncio event loop), no expiry / heartbeat tracking yet.

GROUP_STATE: dict[str, dict] = {}

# Committed offsets store — keyed by (group_id, topic, partition).
# Value is the committed offset integer.
# -1 sentinel is used in OffsetFetch responses to mean "no offset stored yet".
#
# Pre-populated from MinIO at startup via load_committed_offsets().
# Updated in memory on every OffsetCommit, then persisted to MinIO.
COMMITTED_OFFSETS: dict[tuple[str, str, int], int] = {}

# ---------------------------------------------------------------------------
# Fetch long-poll: per-partition wakeup events (our simplified "purgatory")
# ---------------------------------------------------------------------------
# When a Fetch request arrives and fetch_offset >= hw (no new data), we park
# the coroutine here instead of returning an empty response immediately.
#
# Key:   (topic, partition)
# Value: asyncio.Event  — set() when a Produce writes to that partition,
#                          cleared() after each wait cycle.
#
# This is a simplified version of Kafka's DelayedOperation Purgatory:
#   • Real Kafka: TimingWheel + tryComplete on every Produce (O(1))
#   • Our version: asyncio.Event per partition, Produce calls set()
#
# Without this, the consumer would always wait the full max_wait_ms even
# if new data arrived 1ms after the Fetch. With it, the consumer wakes
# up within ~1ms of a matching Produce.
FETCH_WAITERS: dict[tuple[str, int], asyncio.Event] = {}

# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

async def dispatch(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Route the request to the appropriate handler based on api_key.

    Returns a framed response (bytes) ready to write to the socket,
    or None if no handler is registered for this API key.
    """
    if header.api_key == 18:  # ApiVersions
        return build_api_versions_response(
            correlation_id=header.correlation_id,
            api_version=header.api_version,
        )

    if header.api_key == 3:   # Metadata
        from storage import get_topic_config
        topics = parse_metadata_request_topics(payload, header.header_size)
        topic_config = get_topic_config()
        return build_metadata_response(
            correlation_id=header.correlation_id,
            topics=topics,
            api_version=header.api_version,
            topic_config=topic_config,
        )

    if header.api_key == 0:   # Produce
        return _handle_produce(header, payload)

    if header.api_key == 1:   # Fetch
        return await _handle_fetch(header, payload)

    if header.api_key == 2:   # ListOffsets
        return _handle_list_offsets(header, payload)

    if header.api_key == 8:   # OffsetCommit
        return _handle_offset_commit(header, payload)

    if header.api_key == 9:   # OffsetFetch
        return _handle_offset_fetch(header, payload)

    if header.api_key == 10:  # FindCoordinator
        return _handle_find_coordinator(header, payload)

    if header.api_key == 11:  # JoinGroup
        return _handle_join_group(header, payload)

    if header.api_key == 12:  # Heartbeat
        return _handle_heartbeat(header, payload)

    if header.api_key == 13:  # LeaveGroup
        return _handle_leave_group(header, payload)

    if header.api_key == 14:  # SyncGroup
        return _handle_sync_group(header, payload)

    return None

# ---------------------------------------------------------------------------
# Fetch handler — read batch from MinIO and send response
# ---------------------------------------------------------------------------

async def _handle_fetch(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Handle a Fetch request with long-polling.

    If fetch_offset >= high_watermark (no new data), we hold the response
    for up to max_wait_ms milliseconds before returning empty.  Two things
    can end the wait early:
      a) A Produce request writes to a watched partition  →  FETCH_WAITERS
         event is set(), we wake up and re-check hw.
      b) max_wait_ms expires  →  we return empty unconditionally.

    This mirrors Kafka's DelayedFetch purgatory at a small scale.
    """
    from protocol import parse_fetch_request, build_fetch_response
    from storage import read_batch

    try:
        fetches = parse_fetch_request(payload, header.header_size, header.api_version)
    except Exception as exc:
        log.warning("Fetch — parse error: %s", exc)
        return None

    # max_wait_ms is the same for all partitions in this request
    max_wait_ms = fetches[0]["max_wait_ms"] if fetches else 500
    deadline    = asyncio.get_event_loop().time() + max_wait_ms / 1000.0

    # ── Long-poll loop ────────────────────────────────────────────────────
    # We wait until at least one partition has data OR the deadline expires.
    # Poll interval: 50 ms — fine-grained enough, coarse enough to avoid spin.
    POLL_INTERVAL = 0.050   # 50 ms

    while True:
        results = []
        any_data = False

        for f in fetches:
            topic        = f["topic"]
            partition    = f["partition"]
            fetch_offset = f["fetch_offset"]

            try:
                batch_bytes, hw = read_batch(topic, partition, fetch_offset)
                error_code = 0
            except Exception as exc:
                log.error("Fetch → failed to read %s/%d: %s", topic, partition, exc)
                batch_bytes = None
                hw          = 0
                error_code  = 5   # LEADER_NOT_AVAILABLE

            if batch_bytes:
                any_data = True
            results.append((topic, partition, error_code, hw, batch_bytes))

        if any_data:
            # We have real data — send it immediately.
            break

        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            # max_wait_ms elapsed — send empty response.
            log.debug(
                "Fetch long-poll expired after %d ms — returning empty",
                max_wait_ms,
            )
            break

        # No data yet — park until a Produce wakes us or the interval elapses.
        # We watch the event for the first requested partition (simplification:
        # a multi-partition Fetch wakes on any produce to partition 0 of the
        # first topic; good enough for our single-partition topology).
        if fetches:
            watch_key = (fetches[0]["topic"], fetches[0]["partition"])
            event = FETCH_WAITERS.setdefault(watch_key, asyncio.Event())
            sleep_time = min(POLL_INTERVAL, remaining)
            try:
                await asyncio.wait_for(event.wait(), timeout=sleep_time)
                event.clear()   # reset so next wait works
                log.debug("Fetch long-poll woken by Produce on %s/%d",
                          watch_key[0], watch_key[1])
            except asyncio.TimeoutError:
                pass  # interval elapsed; loop and re-check hw
        else:
            break

    # ── Log and respond ───────────────────────────────────────────────────
    for (topic, partition, error_code, hw, batch_bytes) in results:
        log.info(
            "Fetch ←  topic=%r  partition=%d  fetch_offset=%d  hw=%d  bytes=%d",
            topic, partition, fetch_offset, hw,
            len(batch_bytes) if batch_bytes else 0,
        )

    return build_fetch_response(header.correlation_id, results, header.api_version)


# ---------------------------------------------------------------------------
# ListOffsets handler — return high_watermark or 0
# ---------------------------------------------------------------------------

def _handle_list_offsets(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Handle a ListOffsets request.
    Consumers use this to find the start (-2) or end (-1) of a partition log.
    Since we only have one segment right now, earliest is 0 and latest is hw.
    """
    from protocol import parse_list_offsets_request, build_list_offsets_response
    from storage import _next_offset, read_batch

    try:
        requests = parse_list_offsets_request(payload, header.header_size, header.api_version)
    except Exception as exc:
        log.warning("ListOffsets — parse error: %s", exc)
        return None

    results = []
    for req in requests:
        topic = req["topic"]
        partition = req["partition"]
        timestamp = req["timestamp"]

        # Get HW from in-memory cache.  If 0, the broker may have restarted —
        # call read_batch to trigger MinIO HW recovery (it will scan the last
        # batch object and restore _next_offset as a side-effect).
        hw = _next_offset.get((topic, partition), 0)
        if hw == 0:
            _, hw = read_batch(topic, partition, 2**62)

        if timestamp == -2:
            offset = 0          # earliest
        elif timestamp == -1:
            offset = hw         # latest
        else:
            offset = hw         # timestamp search not supported; return latest

        log.info(
            "ListOffsets ← topic=%r partition=%d timestamp=%d -> offset=%d",
            topic, partition, timestamp, offset
        )
        results.append((topic, partition, 0, timestamp, offset))

    return build_list_offsets_response(header.correlation_id, results, header.api_version)


# ---------------------------------------------------------------------------
# FindCoordinator handler  (API key 10)
# ---------------------------------------------------------------------------

def _handle_find_coordinator(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Handle a FindCoordinator (GroupCoordinator) request.

    The client sends a group_id string and asks: "Which broker is the
    coordinator for this consumer group?"

    In a real multi-broker Kafka cluster the answer is determined by:
        coordinator_id = hash(group_id) % num_partitions(__consumer_offsets)
    then whoever leads that __consumer_offsets partition is the coordinator.

    Since we are a single-broker cluster, the answer is always: us.
    We return error_code=0, coordinator_id=1, host="localhost", port=9092.
    """
    body = payload[header.header_size:]
    pos  = 0

    def rd_str() -> str:
        nonlocal pos
        n = struct.unpack_from(">h", body, pos)[0]; pos += 2
        if n < 0:
            return ""
        s = body[pos: pos + n].decode("utf-8"); pos += n
        return s

    # v0: coordinator_key = consumer_group  (STRING)
    # v1: coordinator_key = STRING, coordinator_type = INT8  (0=group, 1=txn)
    group_id = rd_str()

    coordinator_type = 0  # default = consumer group
    if header.api_version >= 1:
        coordinator_type = struct.unpack_from(">b", body, pos)[0]

    log.info(
        "FindCoordinator ← group_id=%r type=%d → returning localhost:9092 (broker_id=1)",
        group_id,
        coordinator_type,
    )

    return build_find_coordinator_response(
        correlation_id=header.correlation_id,
        api_version=header.api_version,
        error_code=0,
        coordinator_id=1,
        host="localhost",
        port=9092,
    )


# ---------------------------------------------------------------------------
# JoinGroup handler  (API key 11)
# ---------------------------------------------------------------------------

def _handle_join_group(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Handle a JoinGroup request.

    Protocol:
    1. Parse the request (group_id, session_timeout, member_id, protocols).
    2. If member_id == "" this is a fresh join — assign a new UUID-based ID.
    3. Register the member in GROUP_STATE[group_id].
    4. For a single-broker single-member setup, immediately elect this member
       as the group leader and return generation_id=1 with the full member list.
       (In a real broker we would wait for all members before responding.)
    """
    import uuid

    body = payload[header.header_size:]
    pos  = 0

    def rd_str() -> str:
        nonlocal pos
        n = struct.unpack_from(">h", body, pos)[0]; pos += 2
        if n < 0:
            return ""
        s = body[pos: pos + n].decode("utf-8"); pos += n
        return s

    def rd_i32() -> int:
        nonlocal pos
        v = struct.unpack_from(">i", body, pos)[0]; pos += 4; return v

    def rd_bytes() -> bytes:
        nonlocal pos
        n = struct.unpack_from(">i", body, pos)[0]; pos += 4
        if n < 0:
            return b""
        b_ = body[pos: pos + n]; pos += n
        return bytes(b_)

    # ── Parse request body ────────────────────────────────────────────────
    group_id        = rd_str()
    session_timeout = rd_i32()
    if header.api_version >= 1:
        _rebalance_timeout = rd_i32()
    member_id       = rd_str()
    if header.api_version >= 5:
        _group_instance_id = rd_str()   # nullable, ignore for now
    protocol_type   = rd_str()          # always "consumer"

    protocol_count  = rd_i32()
    protocols: list[tuple[str, bytes]] = []
    for _ in range(protocol_count):
        proto_name = rd_str()
        proto_meta = rd_bytes()
        protocols.append((proto_name, proto_meta))

    # ── Assign member_id if this is a fresh join ──────────────────────────
    if not member_id:
        member_id = f"{header.client_id}-{uuid.uuid4()}"
        log.info("JoinGroup: assigned new member_id=%r to client %r",
                 member_id, header.client_id)

    # ── Choose protocol (pick the first one the client advertises) ────────
    chosen_protocol = protocols[0][0] if protocols else "range"
    chosen_metadata = protocols[0][1] if protocols else b""

    # ── Update in-memory group state ──────────────────────────────────────
    if group_id not in GROUP_STATE:
        GROUP_STATE[group_id] = {
            "generation_id": 1,
            "leader_id":     member_id,
            "protocol":      chosen_protocol,
            "members":       {},
        }
        log.info("JoinGroup: created new group %r  generation=1  leader=%r",
                 group_id, member_id)
    else:
        # Existing group — increment generation (rebalance)
        GROUP_STATE[group_id]["generation_id"] += 1
        log.info("JoinGroup: group %r rebalancing  generation=%d",
                 group_id, GROUP_STATE[group_id]["generation_id"])

    grp = GROUP_STATE[group_id]
    grp["members"][member_id] = {
        "metadata":  chosen_metadata,
        "client_id": header.client_id,
    }

    leader_id     = grp["leader_id"]
    generation_id = grp["generation_id"]
    protocol_name = grp["protocol"]

    # Only the leader receives the full member list (Kafka spec requirement).
    # Followers receive an empty array and must wait for SyncGroup.
    if member_id == leader_id:
        member_list = [
            (mid, info["metadata"])
            for mid, info in grp["members"].items()
        ]
    else:
        member_list = []

    log.info(
        "JoinGroup ← group=%r generation=%d protocol=%r "
        "leader=%r member=%r (is_leader=%s)",
        group_id, generation_id, protocol_name,
        leader_id, member_id, member_id == leader_id,
    )

    return build_join_group_response(
        correlation_id=header.correlation_id,
        api_version=header.api_version,
        error_code=0,
        generation_id=generation_id,
        protocol_name=protocol_name,
        leader_id=leader_id,
        member_id=member_id,
        members=member_list,
    )


# ---------------------------------------------------------------------------
# OffsetCommit handler  (API key 8)
# ---------------------------------------------------------------------------

def _handle_offset_commit(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Handle an OffsetCommit request.

    Parses the topic/partition/offset list and stores each committed offset
    in COMMITTED_OFFSETS[(group_id, topic, partition)].
    Returns error_code=0 for every partition.

    Request versions:
      v0: group_id | topics[topic | partitions[partition, offset, metadata]]
      v1: adds generation_id + consumer_id; partitions also have timestamp
      v2: replaces timestamp with retention_time (after group_id fields)
      v3: same as v2 + throttle_time_ms in response
    """
    body = payload[header.header_size:]
    pos  = 0

    def rd_str() -> str:
        nonlocal pos
        n = struct.unpack_from(">h", body, pos)[0]; pos += 2
        if n < 0:
            return ""
        s = body[pos: pos + n].decode("utf-8"); pos += n
        return s

    def rd_i32() -> int:
        nonlocal pos
        v = struct.unpack_from(">i", body, pos)[0]; pos += 4; return v

    def rd_i64() -> int:
        nonlocal pos
        v = struct.unpack_from(">q", body, pos)[0]; pos += 8; return v

    group_id = rd_str()

    if header.api_version >= 1:
        _generation_id = rd_i32()
        _consumer_id   = rd_str()
    if header.api_version >= 2:
        _retention_time = rd_i64()   # -1 means use broker default

    topic_count = rd_i32()
    results: list[tuple[str, list[int]]] = []

    for _ in range(topic_count):
        topic = rd_str()
        partition_count = rd_i32()
        committed_partitions: list[int] = []

        for _ in range(partition_count):
            partition = rd_i32()
            offset    = rd_i64()
            if header.api_version == 1:
                _timestamp = rd_i64()   # v1 only
            _metadata = rd_str()        # all versions

            key = (group_id, topic, partition)
            COMMITTED_OFFSETS[key] = offset
            committed_partitions.append(partition)
            log.info(
                "OffsetCommit ← group=%r topic=%r partition=%d offset=%d",
                group_id, topic, partition, offset,
            )
            # Persist to MinIO so offsets survive broker restarts.
            # This is a synchronous HTTP PUT — acceptable for now since
            # OffsetCommit is infrequent (every auto.commit.interval.ms = 5s).
            try:
                commit_offset(group_id, topic, partition, offset)
            except Exception as exc:
                log.warning(
                    "OffsetCommit: MinIO persist failed for %r/%r/%d: %s",
                    group_id, topic, partition, exc,
                )

        results.append((topic, committed_partitions))

    return build_offset_commit_response(
        correlation_id=header.correlation_id,
        api_version=header.api_version,
        results=results,
    )


# ---------------------------------------------------------------------------
# OffsetFetch handler  (API key 9)
# ---------------------------------------------------------------------------

def _handle_offset_fetch(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Handle an OffsetFetch request.

    The consumer sends the list of (topic, partition) pairs it wants to know
    the last committed offset for.  We look each up in COMMITTED_OFFSETS.
    If nothing is stored yet, we return -1, which tells the consumer to start
    from the beginning (auto_offset_reset='earliest') or the end ('latest').

    Request schema is the same for v0–v3:
      consumer_group STRING | topics ARRAY(topic STRING, partitions ARRAY INT32)
    """
    body = payload[header.header_size:]
    pos  = 0

    def rd_str() -> str:
        nonlocal pos
        n = struct.unpack_from(">h", body, pos)[0]; pos += 2
        if n < 0:
            return ""
        s = body[pos: pos + n].decode("utf-8"); pos += n
        return s

    def rd_i32() -> int:
        nonlocal pos
        v = struct.unpack_from(">i", body, pos)[0]; pos += 4; return v

    group_id    = rd_str()
    topic_count = rd_i32()
    results: list[tuple[str, list[tuple[int, int]]]] = []

    for _ in range(topic_count):
        topic      = rd_str()
        part_count = rd_i32()
        part_offsets: list[tuple[int, int]] = []

        for _ in range(part_count):
            partition = rd_i32()
            committed = COMMITTED_OFFSETS.get((group_id, topic, partition), -1)
            part_offsets.append((partition, committed))
            log.info(
                "OffsetFetch ← group=%r topic=%r partition=%d → committed=%d",
                group_id, topic, partition, committed,
            )

        results.append((topic, part_offsets))

    return build_offset_fetch_response(
        correlation_id=header.correlation_id,
        api_version=header.api_version,
        results=results,
    )


# ---------------------------------------------------------------------------
# Heartbeat handler  (API key 12)
# ---------------------------------------------------------------------------

def _handle_heartbeat(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Handle a Heartbeat request.

    kafka-python sends heartbeats from a background thread every
    heartbeat.interval.ms (default 3s). We must reply quickly so the
    client doesn't declare the coordinator dead and trigger a rebalance.

    Validation:
      - group_id must exist in GROUP_STATE
      - member_id must be a known member of that group
      - generation_id must match the current stored generation
        (mismatch = ILLEGAL_GENERATION, consumer must rejoin)
    """
    body = payload[header.header_size:]
    pos  = 0

    def rd_str() -> str:
        nonlocal pos
        n = struct.unpack_from(">h", body, pos)[0]; pos += 2
        if n < 0:
            return ""
        s = body[pos: pos + n].decode("utf-8"); pos += n
        return s

    def rd_i32() -> int:
        nonlocal pos
        v = struct.unpack_from(">i", body, pos)[0]; pos += 4; return v

    group_id      = rd_str()
    generation_id = rd_i32()
    member_id     = rd_str()
    # v2+ has group_instance_id — we don't need to parse it for validation

    grp = GROUP_STATE.get(group_id)

    # ── Unknown group ──────────────────────────────────────────────────────
    if grp is None:
        log.warning(
            "Heartbeat: unknown group %r from member %r — UNKNOWN_MEMBER_ID",
            group_id, member_id,
        )
        return build_heartbeat_response(
            correlation_id=header.correlation_id,
            api_version=header.api_version,
            error_code=25,  # UNKNOWN_MEMBER_ID
        )

    # ── Unknown member ─────────────────────────────────────────────────────
    if member_id not in grp["members"]:
        log.warning(
            "Heartbeat: unknown member %r in group %r — UNKNOWN_MEMBER_ID",
            member_id, group_id,
        )
        return build_heartbeat_response(
            correlation_id=header.correlation_id,
            api_version=header.api_version,
            error_code=25,  # UNKNOWN_MEMBER_ID
        )

    # ── Stale generation ───────────────────────────────────────────────────
    if generation_id != grp["generation_id"]:
        log.warning(
            "Heartbeat: stale generation_id=%d (current=%d) "
            "from member %r in group %r — ILLEGAL_GENERATION",
            generation_id, grp["generation_id"], member_id, group_id,
        )
        return build_heartbeat_response(
            correlation_id=header.correlation_id,
            api_version=header.api_version,
            error_code=22,  # ILLEGAL_GENERATION
        )

    # ── Healthy heartbeat ──────────────────────────────────────────────────
    log.debug(
        "Heartbeat ← group=%r generation=%d member=%r → OK",
        group_id, generation_id, member_id,
    )
    return build_heartbeat_response(
        correlation_id=header.correlation_id,
        api_version=header.api_version,
        error_code=0,
    )


def _handle_leave_group(header: RequestHeader, payload: bytes) -> bytes:
    """LeaveGroup (API 13) -> Returns success to unblock consumer.close()"""
    log.info("LeaveGroup ← received")
    return build_leave_group_response(header.correlation_id, header.api_version)


# ---------------------------------------------------------------------------
# SyncGroup handler  (API key 14)
# ---------------------------------------------------------------------------

def _handle_sync_group(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Handle a SyncGroup request.

    The group leader sends a group_assignment array (one entry per member).
    Each entry maps a member_id to a ConsumerProtocolAssignment blob.
    Followers send an empty group_assignment array.

    We:
    1. Parse the request.
    2. If this member sent assignments (i.e. it's the leader), store them
       in GROUP_STATE under the group.
    3. Look up and return the assignment for this specific member_id.
    """
    body = payload[header.header_size:]
    pos  = 0

    def rd_str() -> str:
        nonlocal pos
        n = struct.unpack_from(">h", body, pos)[0]; pos += 2
        if n < 0:
            return ""
        s = body[pos: pos + n].decode("utf-8"); pos += n
        return s

    def rd_i32() -> int:
        nonlocal pos
        v = struct.unpack_from(">i", body, pos)[0]; pos += 4; return v

    def rd_bytes() -> bytes:
        nonlocal pos
        n = struct.unpack_from(">i", body, pos)[0]; pos += 4
        if n < 0:
            return b""
        b_ = body[pos: pos + n]; pos += n
        return bytes(b_)

    # ── Parse request body ────────────────────────────────────────────────
    group_id      = rd_str()
    generation_id = rd_i32()
    member_id     = rd_str()
    if header.api_version >= 3:
        _group_instance_id = rd_str()   # nullable, ignore

    assignment_count = rd_i32()
    received_assignments: dict[str, bytes] = {}
    for _ in range(assignment_count):
        mid      = rd_str()
        metadata = rd_bytes()
        received_assignments[mid] = metadata

    # ── Store assignments if this is the leader sending them ──────────────
    grp = GROUP_STATE.get(group_id)
    if grp is None:
        log.warning("SyncGroup: unknown group %r — returning error", group_id)
        return build_sync_group_response(
            correlation_id=header.correlation_id,
            api_version=header.api_version,
            error_code=15,   # COORDINATOR_NOT_AVAILABLE
            member_assignment_bytes=b"",
        )

    if received_assignments:
        # This is the leader: store the raw assignment bytes per member.
        # Each value is a ConsumerProtocolAssignment blob as sent by the client.
        grp["assignments"] = received_assignments
        log.info(
            "SyncGroup: leader %r stored assignments for %d member(s) in group %r",
            member_id, len(received_assignments), group_id,
        )

    # ── Return this member's assignment ───────────────────────────────────
    assignments = grp.get("assignments", {})
    my_assignment_bytes = assignments.get(member_id, b"")

    if not my_assignment_bytes:
        # Fallback: no assignment stored yet (shouldn't happen in single-member
        # case, but be safe). Give the member an empty assignment.
        my_assignment_bytes = encode_member_assignment({})
        log.warning(
            "SyncGroup: no assignment found for member %r in group %r — "
            "returning empty assignment",
            member_id, group_id,
        )
    else:
        log.info(
            "SyncGroup ← group=%r generation=%d member=%r "
            "assignment=%d bytes",
            group_id, generation_id, member_id, len(my_assignment_bytes),
        )

    return build_sync_group_response(
        correlation_id=header.correlation_id,
        api_version=header.api_version,
        error_code=0,
        member_assignment_bytes=my_assignment_bytes,
    )


def _handle_produce(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Handle a Produce request:
      1. Parse the request body (topic, partition, RecordBatch bytes).
      2. Log RecordBatch header + decoded record values.
      3. Write the raw RecordBatch bytes to MinIO.
      4. Return a framed Produce response with error_code=0 and the
         assigned base_offset.

    Returns the framed response bytes, or None on unrecoverable parse failure.
    """
    try:
        partitions = parse_produce_request(payload, header.header_size, header.api_version)
    except Exception as exc:
        log.warning("Produce — parse error: %s", exc)
        return None

    response_results: list[tuple[str, int, int, int]] = []   # (topic, part, err, offset)

    for p in partitions:
        # ── Log RecordBatch header and decoded records ────────────────────────
        batch_info   = "  (no record set)"
        records_info = ""
        records_count = 0

        if p["record_set"]:
            try:
                b = parse_record_batch_header(p["record_set"])
                records_count = b["records_count"]
                batch_info = (
                    f"\n"
                    f"  ┌─ RecordBatch ──────────────────────────────────────────────────\n"
                    f"  │  magic={b['magic']}  compression={b['compression']}  "
                    f"records_count={b['records_count']}\n"
                    f"  │  base_offset={b['base_offset']}  "
                    f"base_timestamp={b['base_timestamp']}\n"
                    f"  │  producer_id={b['producer_id']}  "
                    f"producer_epoch={b['producer_epoch']}  "
                    f"base_sequence={b['base_sequence']}\n"
                    f"  │  crc={b['crc']}  is_transactional={b['is_transactional']}\n"
                    f"  └───────────────────────────────────────────────────────────"
                )
            except Exception as exc:
                batch_info = f"  (RecordBatch parse error: {exc})"

            try:
                records = parse_records_in_batch(p["record_set"])
                for i, r in enumerate(records):
                    key_s = (
                        "<null>"
                        if r["key"] is None
                        else repr(r["key"].decode("utf-8", errors="replace"))
                    )
                    val_s = (
                        "<null>"
                        if r["value"] is None
                        else repr(r["value"].decode("utf-8", errors="replace"))
                    )
                    hdr_s = (
                        "  (no headers)"
                        if not r["headers"]
                        else "".join(
                            f"\n      header: {k!r} = {v!r}" for k, v in r["headers"]
                        )
                    )
                    records_info += (
                        f"\n  record[{i}]  key={key_s}  value={val_s}{hdr_s}"
                    )
            except Exception as exc:
                records_info = f"\n  (record decode error: {exc})"

        log.info(
            "Produce →  topic=%r  partition=%d  "
            "acks=%d  record_set=%d bytes%s%s",
            p["topic"], p["partition"], p["acks"],
            p["record_set_size"], batch_info, records_info,
        )

        # ── Write the RecordBatch to MinIO ───────────────────────────────────
        # We write the raw bytes exactly as we received them — no parsing,
        # no re-serialisation, CRC is preserved end-to-end.
        error_code  = 0
        base_offset = 0

        if p["record_set"]:
            try:
                base_offset = write_batch(
                    topic=p["topic"],
                    partition=p["partition"],
                    record_set=p["record_set"],
                    records_count=records_count,
                )
                log.info(
                    "Produce →  ack  topic=%r  partition=%d  base_offset=%d",
                    p["topic"], p["partition"], base_offset,
                )
                # ── Wake any Fetch coroutines long-polling this partition ────
                # This is the produce-side of our purgatory: instead of making
                # the consumer wait up to max_wait_ms, we signal immediately.
                waiter_key = (p["topic"], p["partition"])
                if waiter_key in FETCH_WAITERS:
                    FETCH_WAITERS[waiter_key].set()
            except Exception as exc:
                log.error(
                    "Produce →  MinIO write failed for topic=%r partition=%d: %s",
                    p["topic"], p["partition"], exc,
                )
                error_code = 5   # LEADER_NOT_AVAILABLE — generic storage error

        response_results.append((p["topic"], p["partition"], error_code, base_offset))

    return build_produce_response(
        correlation_id=header.correlation_id,
        results=response_results,
        api_version=header.api_version,
    )


# ---------------------------------------------------------------------------
# Connection handler — one coroutine per accepted TCP connection
# ---------------------------------------------------------------------------
async def handle_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    peer = writer.get_extra_info("peername")
    log.info("── new connection from %s:%d ──", *peer)

    frame_index = 0
    try:
        while True:
            # ── Step 1: read the 4-byte length prefix ──────────────────────
            try:
                raw_len = await _read_exact(reader, LENGTH_PREFIX_BYTES)
            except asyncio.IncompleteReadError:
                log.info("client %s:%d closed the connection", *peer)
                break

            (payload_length,) = struct.unpack(">I", raw_len)
            log.debug(
                "frame #%d — length prefix: %s → payload_length=%d",
                frame_index,
                raw_len.hex(" "),
                payload_length,
            )

            # Guard against absurdly large frames (simple sanity check)
            MAX_FRAME = 100 * 1024 * 1024  # 100 MiB
            if payload_length > MAX_FRAME:
                log.error(
                    "frame #%d claims %d bytes — exceeds limit (%d). "
                    "Dropping connection.",
                    frame_index,
                    payload_length,
                    MAX_FRAME,
                )
                break

            # ── Step 2: read exactly payload_length bytes ──────────────────
            payload = await _read_exact(reader, payload_length)

            # ── Step 3: log as hex ─────────────────────────────────────────
            log.info(
                "frame #%d received (%d bytes):\n%s",
                frame_index,
                payload_length,
                _hex_dump(payload),
            )

            # ── Step 4: parse the request header ────────────────────────────
            try:
                header = parse_request_header(payload)
                log.info("frame #%d parsed header:\n%s", frame_index, header.pretty())
            except ParseError as exc:
                log.warning("frame #%d header parse failed: %s — skipping", frame_index, exc)
                frame_index += 1
                continue

            # ── Step 5: dispatch and send response ───────────────────────────
            response = await dispatch(header, payload)

            if response is not None:
                writer.write(response)
                await writer.drain()   # flush the OS send buffer
                log.info(
                    "frame #%d → sent %s response (%d bytes, corr_id=%d)",
                    frame_index,
                    header.api_name,
                    len(response),
                    header.correlation_id,
                )
            else:
                log.warning(
                    "frame #%d — no handler for %s (api_key=%d), not responding",
                    frame_index,
                    header.api_name,
                    header.api_key,
                )

            frame_index += 1

    except Exception as exc:  # pragma: no cover — unexpected errors
        log.exception("unexpected error handling %s:%d: %s", *peer, exc)
    finally:
        log.info("closing connection to %s:%d", *peer)
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main() -> None:
    # ── Load persisted committed offsets from MinIO before accepting connections
    global COMMITTED_OFFSETS
    log.info("Loading committed offsets from MinIO …")
    try:
        COMMITTED_OFFSETS = load_committed_offsets()
    except Exception as exc:
        log.warning("Could not load committed offsets from MinIO: %s", exc)
        COMMITTED_OFFSETS = {}

    server = await asyncio.start_server(
        handle_connection,
        HOST,
        PORT,
        reuse_address=True,
    )
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    log.info("diskless-kafka raw listener running on %s", addrs)
    log.info("waiting for connections …")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("server stopped.")
