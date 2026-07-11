"""In-memory broker state: consumer groups, committed offsets, fetch waiters.

These three classes replace the module-level ``GROUP_STATE`` /
``COMMITTED_OFFSETS`` / ``FETCH_WAITERS`` dicts.  Encapsulating them keeps the
group-membership state machine and offset bookkeeping in one testable place and
off the global namespace.  A single asyncio event loop drives all of this, so
no locking is needed.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field

from errors import ErrorCode
from protocol import (
    JoinGroupRequest,
    SyncGroupRequest,
    encode_member_assignment,
)
from storage import ObjectStore

log = logging.getLogger("kafka.coordinator")


# ── Consumer group coordination ──────────────────────────────────────────────

@dataclass
class _Group:
    generation_id: int
    leader_id: str
    protocol: str
    members: dict[str, bytes] = field(default_factory=dict)  # member_id -> metadata
    assignments: dict[str, bytes] = field(default_factory=dict)  # member_id -> assignment


@dataclass
class JoinResult:
    error_code: int
    generation_id: int
    protocol_name: str
    leader_id: str
    member_id: str
    members: list[tuple[str, bytes]]  # populated for the leader only


class GroupCoordinator:
    """Minimal single-broker consumer-group state machine.

    Simplifications vs. real Kafka: the first member to join is elected leader
    and gets generation 1 immediately (no rebalance barrier), and there is no
    session-timeout expiry — liveness is only checked on heartbeat.
    """

    def __init__(self) -> None:
        self._groups: dict[str, _Group] = {}

    def join(self, req: JoinGroupRequest, client_id: str | None) -> JoinResult:
        member_id = req.member_id
        if not member_id:
            member_id = f"{client_id}-{uuid.uuid4()}"
            log.info("JoinGroup: assigned member_id=%r", member_id)

        protocol, metadata = req.protocols[0] if req.protocols else ("range", b"")

        group = self._groups.get(req.group_id)
        if group is None:
            group = _Group(generation_id=1, leader_id=member_id, protocol=protocol)
            self._groups[req.group_id] = group
            log.info("JoinGroup: new group %r generation=1 leader=%r",
                     req.group_id, member_id)
        else:
            group.generation_id += 1
            log.info("JoinGroup: group %r rebalancing generation=%d",
                     req.group_id, group.generation_id)

        group.members[member_id] = metadata

        # Only the leader receives the member list; followers wait for SyncGroup.
        members = list(group.members.items()) if member_id == group.leader_id else []
        return JoinResult(
            error_code=ErrorCode.NONE,
            generation_id=group.generation_id,
            protocol_name=group.protocol,
            leader_id=group.leader_id,
            member_id=member_id,
            members=members,
        )

    def heartbeat(self, group_id: str, member_id: str, generation_id: int) -> int:
        group = self._groups.get(group_id)
        if group is None or member_id not in group.members:
            log.warning("Heartbeat: unknown member %r in group %r", member_id, group_id)
            return ErrorCode.UNKNOWN_MEMBER_ID
        if generation_id != group.generation_id:
            log.warning("Heartbeat: stale generation %d (current %d) for %r",
                        generation_id, group.generation_id, member_id)
            return ErrorCode.ILLEGAL_GENERATION
        return ErrorCode.NONE

    def sync(self, req: SyncGroupRequest) -> tuple[int, bytes]:
        group = self._groups.get(req.group_id)
        if group is None:
            log.warning("SyncGroup: unknown group %r", req.group_id)
            return ErrorCode.COORDINATOR_NOT_AVAILABLE, b""

        # The leader carries every member's assignment; store them.
        if req.assignments:
            group.assignments = req.assignments
            log.info("SyncGroup: leader %r stored %d assignment(s) in group %r",
                     req.member_id, len(req.assignments), req.group_id)

        assignment = group.assignments.get(req.member_id, b"")
        if not assignment:
            assignment = encode_member_assignment({})
            log.warning("SyncGroup: no assignment for %r in group %r — empty",
                        req.member_id, req.group_id)
        return ErrorCode.NONE, assignment


# ── Committed offsets ────────────────────────────────────────────────────────

class OffsetStore:
    """In-memory committed offsets, write-through to the object store.

    ``-1`` is returned for an unknown offset — the client's ``auto_offset_reset``
    then decides between earliest and latest.
    """

    def __init__(self, store: ObjectStore) -> None:
        self._store = store
        self._offsets: dict[tuple[str, str, int], int] = {}

    def load(self) -> None:
        """Pre-populate from the object store so consumers resume after restart."""
        self._offsets = self._store.load_committed_offsets()

    def commit(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        self._offsets[(group_id, topic, partition)] = offset
        try:
            self._store.commit_offset(group_id, topic, partition, offset)
        except Exception as exc:  # persistence is best-effort; memory is source of truth
            log.warning("OffsetCommit: persist failed for %s/%s/%d: %s",
                        group_id, topic, partition, exc)

    def fetch(self, group_id: str, topic: str, partition: int) -> int:
        return self._offsets.get((group_id, topic, partition), -1)


# ── Fetch long-poll ("purgatory") ────────────────────────────────────────────

class FetchPurgatory:
    """Per-partition wakeup events for long-polling Fetch requests.

    A parked Fetch waits on the partition's event; a Produce to that partition
    ``notify``-s it, waking the consumer within ~1ms instead of after the full
    ``max_wait_ms``.  A scale-down of Kafka's DelayedOperation purgatory.
    """

    def __init__(self) -> None:
        self._waiters: dict[tuple[str, int], asyncio.Event] = {}

    def waiter(self, topic: str, partition: int) -> asyncio.Event:
        return self._waiters.setdefault((topic, partition), asyncio.Event())

    def notify(self, topic: str, partition: int) -> None:
        event = self._waiters.get((topic, partition))
        if event is not None:
            event.set()
