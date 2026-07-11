"""Self-contained checks for the codec, parsers, coordinator and dispatch.

Runnable directly (``python tests/test_broker.py``) or via pytest.  Uses fake
store/control-plane objects so it needs no MinIO or Postgres.
"""
import asyncio
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from broker import Broker
from codec import BinaryReader, BinaryWriter, frame
from config import BrokerAddress, Settings
from coordinator import (
    FetchPurgatory, GroupCoordinator, OffsetStore,
)
from errors import ApiKey, ErrorCode
from handlers import dispatch
from protocol import (
    JoinGroupRequest, SyncGroupRequest, parse_join_group, parse_produce,
    parse_request_header,
)


def _payload(api_key, version, corr_id=1, client_id="c", body=b""):
    cid = client_id.encode()
    head = struct.pack(">hhih", api_key, version, corr_id, len(cid)) + cid
    return head + body


# ── Codec ────────────────────────────────────────────────────────────────────

def test_codec_roundtrip():
    w = (BinaryWriter().i8(-5).i16(-300).i32(70000).i64(2**40)
         .string("héllo").blob(b"\x00\x01\x02").null_string().null_blob())
    r = BinaryReader(w.getvalue())
    assert r.i8() == -5
    assert r.i16() == -300
    assert r.i32() == 70000
    assert r.i64() == 2**40
    assert r.string() == "héllo"
    assert r.blob() == b"\x00\x01\x02"
    assert r.string() is None      # null_string
    assert r.blob() == b""         # null_blob (-1 → empty)


def test_frame_prefix():
    body = b"abcd"
    framed = frame(body)
    assert struct.unpack(">I", framed[:4])[0] == len(body)
    assert framed[4:] == body


# ── Request parsers (build a body, parse it back) ────────────────────────────

def test_parse_produce_roundtrip():
    body = (BinaryWriter()
            .null_string()      # transactional_id (v3+)
            .i16(1)             # acks
            .i32(100)           # timeout_ms
            .i32(1)             # topic count
            .string("orders").i32(1)   # 1 partition
            .i32(2).blob(b"RECORDBATCH"))  # partition=2, record_set
    payload = _payload(ApiKey.PRODUCE, 7, body=body.getvalue())
    req = parse_produce(payload, parse_request_header(payload))
    assert req.acks == 1 and req.timeout_ms == 100
    assert len(req.partitions) == 1
    p = req.partitions[0]
    assert (p.topic, p.partition, p.record_set) == ("orders", 2, b"RECORDBATCH")


def test_parse_join_group_roundtrip():
    body = (BinaryWriter()
            .string("g1").i32(30000)   # group_id, session_timeout
            .i32(60000)                # rebalance_timeout (v1+)
            .string("")                # member_id (empty → fresh join)
            .string("consumer")        # protocol_type
            .i32(1).string("range").blob(b"META"))  # one protocol
    payload = _payload(ApiKey.JOIN_GROUP, 1, body=body.getvalue())
    req = parse_join_group(payload, parse_request_header(payload))
    assert req.group_id == "g1" and req.member_id == ""
    assert req.protocols == [("range", b"META")]


# ── Coordinator state machine ────────────────────────────────────────────────

def test_group_lifecycle():
    gc = GroupCoordinator()
    join = JoinGroupRequest("g", 30000, "", "consumer", [("range", b"m")])

    first = gc.join(join, client_id="clientA")
    assert first.generation_id == 1
    assert first.leader_id == first.member_id            # first member leads
    assert first.members == [(first.member_id, b"m")]    # leader sees itself

    mid = first.member_id
    assert gc.heartbeat("g", mid, 1) == ErrorCode.NONE
    assert gc.heartbeat("g", mid, 99) == ErrorCode.ILLEGAL_GENERATION
    assert gc.heartbeat("g", "ghost", 1) == ErrorCode.UNKNOWN_MEMBER_ID
    assert gc.heartbeat("missing", mid, 1) == ErrorCode.UNKNOWN_MEMBER_ID

    sync = SyncGroupRequest("g", 1, mid, {mid: b"ASSIGNED"})
    err, assignment = gc.sync(sync)
    assert err == ErrorCode.NONE and assignment == b"ASSIGNED"

    err, _ = gc.sync(SyncGroupRequest("unknown", 1, mid, {}))
    assert err == ErrorCode.COORDINATOR_NOT_AVAILABLE


# ── OffsetStore with a fake object store ─────────────────────────────────────

class _FakeStore:
    def __init__(self):
        self.persisted = {}
        self.batches = {}

    # OffsetStore surface
    def load_committed_offsets(self):
        return {}

    def commit_offset(self, group, topic, partition, offset):
        self.persisted[(group, topic, partition)] = offset

    # ObjectStore surface used by handlers
    def get_topic_config(self):
        return {"orders": {"partitions": 1}}

    def write_batch(self, topic, partition, record_set, count):
        self.batches[(topic, partition)] = record_set
        return 0

    def read_batch(self, topic, partition, fetch_offset):
        return None, 0

    def high_watermark(self, topic, partition):
        return 0


class _FakeControl:
    def partition_leaders(self):
        return {"orders": {0: 1}}


def test_offset_store():
    store = _FakeStore()
    offsets = OffsetStore(store)
    assert offsets.fetch("g", "t", 0) == -1        # unknown → -1
    offsets.commit("g", "t", 0, 42)
    assert offsets.fetch("g", "t", 0) == 42
    assert store.persisted[("g", "t", 0)] == 42    # written through


# ── Handler dispatch smoke test ──────────────────────────────────────────────

def _fake_broker():
    settings = Settings(cluster=[BrokerAddress(1, "localhost", 9092)])
    store = _FakeStore()
    return Broker(
        settings=settings, store=store, control=_FakeControl(),
        coordinator=GroupCoordinator(), offsets=OffsetStore(store),
        purgatory=FetchPurgatory(),
    )


def _run(coro):
    return asyncio.run(coro)


def _corr(resp):
    return struct.unpack(">i", resp[4:8])[0]  # payload starts after length prefix


def test_dispatch_smoke():
    broker = _fake_broker()

    # ApiVersions
    p = _payload(ApiKey.API_VERSIONS, 0, corr_id=7)
    resp = _run(dispatch(broker, parse_request_header(p), p))
    assert resp and _corr(resp) == 7

    # Metadata (async handler, hits fake control plane)
    p = _payload(ApiKey.METADATA, 1, corr_id=8, body=struct.pack(">i", 0))
    resp = _run(dispatch(broker, parse_request_header(p), p))
    assert resp and _corr(resp) == 8

    # Produce → writes to the fake store
    body = (BinaryWriter().null_string().i16(1).i32(0)
            .i32(1).string("orders").i32(1).i32(0).blob(b"BATCH")).getvalue()
    p = _payload(ApiKey.PRODUCE, 7, corr_id=9, body=body)
    resp = _run(dispatch(broker, parse_request_header(p), p))
    assert resp and _corr(resp) == 9
    assert broker.store.batches[("orders", 0)] == b"BATCH"

    # Unknown API → None
    p = _payload(99, 0, corr_id=10)
    assert _run(dispatch(broker, parse_request_header(p), p)) is None


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"ok  {t.__name__}")
    print(f"\n{len(tests)} tests passed")
