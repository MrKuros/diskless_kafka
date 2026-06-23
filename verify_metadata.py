import struct
from protocol import build_metadata_response, BROKER_NODE_ID

# ── verify v1 with empty topics ───────────────────────────────────────────────
frame = build_metadata_response(correlation_id=3, topics=[], api_version=1)
print(f"v1 empty-topics frame: {len(frame)} bytes")
pos = 0
(length,)      = struct.unpack_from(">I", frame, pos); pos += 4
(corr_id,)     = struct.unpack_from(">i", frame, pos); pos += 4
(n_brokers,)   = struct.unpack_from(">i", frame, pos); pos += 4
(node_id,)     = struct.unpack_from(">i", frame, pos); pos += 4
(host_len,)    = struct.unpack_from(">h", frame, pos); pos += 2
host           = frame[pos:pos+host_len]; pos += host_len
(port,)        = struct.unpack_from(">i", frame, pos); pos += 4
(rack_len,)    = struct.unpack_from(">h", frame, pos); pos += 2
(ctrl_id,)     = struct.unpack_from(">i", frame, pos); pos += 4
(n_topics,)    = struct.unpack_from(">i", frame, pos); pos += 4
print(f"  corr_id={corr_id}  brokers={n_brokers}  node_id={node_id}  host={host}  port={port}")
print(f"  rack_len={rack_len} (should be -1=NULL)  controller_id={ctrl_id}  topics={n_topics}")
assert pos == len(frame), f"leftover: {len(frame)-pos} bytes"
print("  OK v1 empty-topics frame verified")
print()

# ── verify v1 with test-topic ─────────────────────────────────────────────────
frame2 = build_metadata_response(correlation_id=1, topics=["test-topic"], api_version=1)
print(f"v1 single-topic frame: {len(frame2)} bytes")
pos = 0
(length,)      = struct.unpack_from(">I", frame2, pos); pos += 4
(corr_id,)     = struct.unpack_from(">i", frame2, pos); pos += 4
(n_brokers,)   = struct.unpack_from(">i", frame2, pos); pos += 4
(node_id,)     = struct.unpack_from(">i", frame2, pos); pos += 4
(host_len,)    = struct.unpack_from(">h", frame2, pos); pos += 2
host           = frame2[pos:pos+host_len]; pos += host_len
(port,)        = struct.unpack_from(">i", frame2, pos); pos += 4
(rack_len,)    = struct.unpack_from(">h", frame2, pos); pos += 2
(ctrl_id,)     = struct.unpack_from(">i", frame2, pos); pos += 4
(n_topics,)    = struct.unpack_from(">i", frame2, pos); pos += 4
(t_err,)       = struct.unpack_from(">h", frame2, pos); pos += 2
(t_namelen,)   = struct.unpack_from(">h", frame2, pos); pos += 2
t_name         = frame2[pos:pos+t_namelen]; pos += t_namelen
(is_internal,) = struct.unpack_from(">?", frame2, pos); pos += 1
(n_parts,)     = struct.unpack_from(">i", frame2, pos); pos += 4
(p_err,)       = struct.unpack_from(">h", frame2, pos); pos += 2
(p_id,)        = struct.unpack_from(">i", frame2, pos); pos += 4
(leader,)      = struct.unpack_from(">i", frame2, pos); pos += 4
(n_reps,)      = struct.unpack_from(">i", frame2, pos); pos += 4
(rep0,)        = struct.unpack_from(">i", frame2, pos); pos += 4
(n_isr,)       = struct.unpack_from(">i", frame2, pos); pos += 4
(isr0,)        = struct.unpack_from(">i", frame2, pos); pos += 4
print(f"  corr_id={corr_id}  controller_id={ctrl_id}  topics={n_topics}")
print(f"  topic={t_name}  err={t_err}  is_internal={is_internal}")
print(f"  partition id={p_id}  leader={leader}  replicas=[{rep0}]  isr=[{isr0}]")
assert rack_len == -1,        f"rack_len should be -1, got {rack_len}"
assert ctrl_id == BROKER_NODE_ID, f"controller_id should be {BROKER_NODE_ID}"
assert t_err == 0,            "topic error should be 0"
assert p_err == 0,            "partition error should be 0"
assert leader == BROKER_NODE_ID
assert is_internal == False
assert pos == len(frame2),    f"leftover: {len(frame2)-pos} bytes"
print("  OK v1 single-topic frame verified  (all fields in correct positions)")
print()
print("All assertions passed.")
