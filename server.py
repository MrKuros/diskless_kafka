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
    build_api_versions_response,
    build_find_coordinator_response,
    build_metadata_response,
    build_produce_response,
    parse_metadata_request_topics,
    parse_produce_request,
    parse_record_batch_header,
    parse_records_in_batch,
    parse_request_header,
)
from storage import write_batch

HOST = "0.0.0.0"
PORT = 9092

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

def dispatch(header: RequestHeader, payload: bytes) -> bytes | None:
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
        topics = parse_metadata_request_topics(payload, header.header_size)
        return build_metadata_response(
            correlation_id=header.correlation_id,
            topics=topics,
            api_version=header.api_version,
        )

    if header.api_key == 0:   # Produce
        return _handle_produce(header, payload)

    if header.api_key == 1:   # Fetch
        return _handle_fetch(header, payload)

    if header.api_key == 2:   # ListOffsets
        return _handle_list_offsets(header, payload)

    if header.api_key == 10:  # FindCoordinator
        return _handle_find_coordinator(header, payload)

    return None

# ---------------------------------------------------------------------------
# Fetch handler — read batch from MinIO and send response
# ---------------------------------------------------------------------------

def _handle_fetch(header: RequestHeader, payload: bytes) -> bytes | None:
    """
    Handle a Fetch request:
      1. Parse request body to get list of (topic, partition, fetch_offset).
      2. Call read_batch() for each partition to get bytes + high_watermark.
      3. Build and return the Fetch response.
    """
    from protocol import parse_fetch_request, build_fetch_response
    from storage import read_batch

    try:
        fetches = parse_fetch_request(payload, header.header_size, header.api_version)
    except Exception as exc:
        log.warning("Fetch — parse error: %s", exc)
        return None

    results = []
    for f in fetches:
        topic = f["topic"]
        partition = f["partition"]
        fetch_offset = f["fetch_offset"]

        try:
            batch_bytes, hw = read_batch(topic, partition, fetch_offset)
            error_code = 0  # NONE
        except Exception as exc:
            log.error("Fetch → failed to read %s/%d: %s", topic, partition, exc)
            batch_bytes = None
            hw = 0
            error_code = 5  # LEADER_NOT_AVAILABLE

        log.info(
            "Fetch ←  topic=%r  partition=%d  fetch_offset=%d  hw=%d  bytes=%d",
            topic, partition, fetch_offset, hw, len(batch_bytes) if batch_bytes else 0
        )
        results.append((topic, partition, error_code, hw, batch_bytes))

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
            response = dispatch(header, payload)

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
