"""Request handlers and the dispatch registry.

Each API is handled by one function registered against its :class:`ApiKey` via
``@handler`` — a small Command/registry pattern that replaces the old
``if api_key == N`` ladder.  A handler takes a :class:`RequestContext`, parses
the body with :mod:`protocol`, drives the broker, and returns a
framed response (or ``None`` for an unhandled API).  Handlers may be sync or
async; :func:`dispatch` awaits the async ones transparently.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections import OrderedDict
from dataclasses import dataclass
from typing import Awaitable, Callable

import protocol
from broker import Broker
from errors import ApiKey, ErrorCode
from protocol import RequestHeader

log = logging.getLogger("kafka.handlers")


@dataclass
class RequestContext:
    """Everything a handler needs: the parsed header, raw payload and broker."""

    header: RequestHeader
    payload: bytes
    broker: Broker

    @property
    def version(self) -> int:
        return self.header.api_version

    @property
    def correlation_id(self) -> int:
        return self.header.correlation_id


Handler = Callable[[RequestContext], "bytes | None | Awaitable[bytes | None]"]
_REGISTRY: dict[int, Handler] = {}


def handler(api_key: ApiKey) -> Callable[[Handler], Handler]:
    """Register *func* as the handler for *api_key*."""
    def register(func: Handler) -> Handler:
        _REGISTRY[api_key] = func
        return func
    return register


async def dispatch(broker: Broker, header: RequestHeader, payload: bytes) -> bytes | None:
    """Route a request to its registered handler, or ``None`` if unhandled."""
    func = _REGISTRY.get(header.api_key)
    if func is None:
        return None
    result = func(RequestContext(header, payload, broker))
    if inspect.isawaitable(result):
        result = await result
    return result


# ── Handshake & metadata ─────────────────────────────────────────────────────

@handler(ApiKey.API_VERSIONS)
def _api_versions(ctx: RequestContext) -> bytes:
    return protocol.build_api_versions_response(ctx.correlation_id, ctx.version)


@handler(ApiKey.METADATA)
async def _metadata(ctx: RequestContext) -> bytes:
    topics = protocol.parse_metadata_topics(ctx.payload, ctx.header)
    topic_config = ctx.broker.store.get_topic_config()
    leaders = await asyncio.to_thread(ctx.broker.control.partition_leaders)
    return protocol.build_metadata_response(
        ctx.correlation_id, topics, ctx.broker.settings.cluster,
        ctx.version, topic_config, leaders,
    )


# ── Produce / Fetch / ListOffsets ────────────────────────────────────────────

@handler(ApiKey.PRODUCE)
def _produce(ctx: RequestContext) -> bytes:
    request = protocol.parse_produce(ctx.payload, ctx.header)
    results: list[tuple[str, int, int, int]] = []

    for p in request.partitions:
        error_code = ErrorCode.NONE
        base_offset = 0
        if p.record_set:
            try:
                count = protocol.records_count(p.record_set)
            except Exception:
                count = 0
            try:
                base_offset = ctx.broker.store.write_batch(
                    p.topic, p.partition, p.record_set, count)
                ctx.broker.purgatory.notify(p.topic, p.partition)
                log.info("Produce → %s/%d base_offset=%d (%d records)",
                         p.topic, p.partition, base_offset, count)
            except Exception as exc:
                log.error("Produce → write failed for %s/%d: %s",
                          p.topic, p.partition, exc)
                error_code = ErrorCode.LEADER_NOT_AVAILABLE
        results.append((p.topic, p.partition, error_code, base_offset))

    return protocol.build_produce_response(ctx.correlation_id, results, ctx.version)


# 50ms poll: fine enough to feel instant, coarse enough not to spin.
_FETCH_POLL_INTERVAL_S = 0.050


@handler(ApiKey.FETCH)
async def _fetch(ctx: RequestContext) -> bytes:
    request = protocol.parse_fetch(ctx.payload, ctx.header)
    targets = request.targets
    deadline = asyncio.get_event_loop().time() + request.max_wait_ms / 1000.0

    # Long-poll: hold the response until data arrives or max_wait_ms elapses.
    while True:
        results = _read_targets(ctx.broker, targets)
        if any(batch for *_, batch in results):
            break

        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0 or not targets:
            break

        # Park on the first partition's waiter; a Produce there wakes us early.
        event = ctx.broker.purgatory.waiter(targets[0].topic, targets[0].partition)
        try:
            await asyncio.wait_for(event.wait(), timeout=min(_FETCH_POLL_INTERVAL_S, remaining))
            event.clear()
        except asyncio.TimeoutError:
            pass  # re-check the watermark

    return protocol.build_fetch_response(ctx.correlation_id, results, ctx.version)


def _read_targets(broker: Broker, targets) -> list[tuple[str, int, int, int, bytes | None]]:
    results = []
    for t in targets:
        try:
            batch_bytes, hw = broker.store.read_batch(t.topic, t.partition, t.fetch_offset)
            error_code = ErrorCode.NONE
        except Exception as exc:
            log.error("Fetch → read failed for %s/%d: %s", t.topic, t.partition, exc)
            batch_bytes, hw, error_code = None, 0, ErrorCode.LEADER_NOT_AVAILABLE
        results.append((t.topic, t.partition, error_code, hw, batch_bytes))
    return results


@handler(ApiKey.LIST_OFFSETS)
def _list_offsets(ctx: RequestContext) -> bytes:
    targets = protocol.parse_list_offsets(ctx.payload, ctx.header)
    results = []
    for t in targets:
        hw = ctx.broker.store.high_watermark(t.topic, t.partition)
        offset = 0 if t.timestamp == -2 else hw  # earliest=0, else latest (hw)
        results.append((t.topic, t.partition, ErrorCode.NONE, t.timestamp, offset))
    return protocol.build_list_offsets_response(ctx.correlation_id, results, ctx.version)


# ── Consumer group coordination ──────────────────────────────────────────────

@handler(ApiKey.FIND_COORDINATOR)
def _find_coordinator(ctx: RequestContext) -> bytes:
    req = protocol.parse_find_coordinator(ctx.payload, ctx.header)
    log.info("FindCoordinator ← group=%r → self (broker 1)", req.key)
    # Single-broker cluster: we are always the coordinator.
    return protocol.build_find_coordinator_response(
        ctx.correlation_id, ctx.version,
        error_code=ErrorCode.NONE, coordinator_id=1, host="localhost", port=9092,
    )


@handler(ApiKey.JOIN_GROUP)
def _join_group(ctx: RequestContext) -> bytes:
    req = protocol.parse_join_group(ctx.payload, ctx.header)
    result = ctx.broker.coordinator.join(req, ctx.header.client_id)
    return protocol.build_join_group_response(
        ctx.correlation_id, ctx.version, result.error_code, result.generation_id,
        result.protocol_name, result.leader_id, result.member_id, result.members,
    )


@handler(ApiKey.SYNC_GROUP)
def _sync_group(ctx: RequestContext) -> bytes:
    req = protocol.parse_sync_group(ctx.payload, ctx.header)
    error_code, assignment = ctx.broker.coordinator.sync(req)
    return protocol.build_sync_group_response(
        ctx.correlation_id, ctx.version, error_code, assignment)


@handler(ApiKey.HEARTBEAT)
def _heartbeat(ctx: RequestContext) -> bytes:
    req = protocol.parse_heartbeat(ctx.payload, ctx.header)
    error_code = ctx.broker.coordinator.heartbeat(
        req.group_id, req.member_id, req.generation_id)
    return protocol.build_heartbeat_response(ctx.correlation_id, ctx.version, error_code)


@handler(ApiKey.LEAVE_GROUP)
def _leave_group(ctx: RequestContext) -> bytes:
    log.info("LeaveGroup ← received")
    return protocol.build_leave_group_response(ctx.correlation_id, ctx.version)


# ── Offset commit / fetch ────────────────────────────────────────────────────

@handler(ApiKey.OFFSET_COMMIT)
def _offset_commit(ctx: RequestContext) -> bytes:
    req = protocol.parse_offset_commit(ctx.payload, ctx.header)

    by_topic: "OrderedDict[str, list[int]]" = OrderedDict()
    for entry in req.entries:
        ctx.broker.offsets.commit(req.group_id, entry.topic, entry.partition, entry.offset)
        by_topic.setdefault(entry.topic, []).append(entry.partition)
        log.info("OffsetCommit ← group=%r %s/%d offset=%d",
                 req.group_id, entry.topic, entry.partition, entry.offset)

    return protocol.build_offset_commit_response(
        ctx.correlation_id, ctx.version, list(by_topic.items()))


@handler(ApiKey.OFFSET_FETCH)
def _offset_fetch(ctx: RequestContext) -> bytes:
    req = protocol.parse_offset_fetch(ctx.payload, ctx.header)

    by_topic: "OrderedDict[str, list[tuple[int, int]]]" = OrderedDict()
    for topic, partition in req.targets:
        committed = ctx.broker.offsets.fetch(req.group_id, topic, partition)
        by_topic.setdefault(topic, []).append((partition, committed))
        log.info("OffsetFetch ← group=%r %s/%d → %d",
                 req.group_id, topic, partition, committed)

    return protocol.build_offset_fetch_response(
        ctx.correlation_id, ctx.version, list(by_topic.items()))
