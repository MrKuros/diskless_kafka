"""Protocol enumerations: API keys and error codes.

Replacing the magic numbers (``api_key == 18``, ``error_code = 25``) that were
sprinkled through the handlers with named enums makes the dispatch table and
the handler logic self-documenting.
"""

from __future__ import annotations

from enum import IntEnum


class ApiKey(IntEnum):
    """Kafka request API keys.

    Only the subset this broker recognises is enumerated; ``name_for`` still
    resolves unknown keys to a readable ``Unknown(N)`` label for logging.
    """

    PRODUCE = 0
    FETCH = 1
    LIST_OFFSETS = 2
    METADATA = 3
    OFFSET_COMMIT = 8
    OFFSET_FETCH = 9
    FIND_COORDINATOR = 10
    JOIN_GROUP = 11
    HEARTBEAT = 12
    LEAVE_GROUP = 13
    SYNC_GROUP = 14
    API_VERSIONS = 18

    @classmethod
    def name_for(cls, key: int) -> str:
        """Human-readable name for *key*, or ``Unknown(N)`` if unrecognised."""
        try:
            return _API_KEY_LABELS[cls(key)]
        except ValueError:
            return _EXTRA_API_KEY_LABELS.get(key, f"Unknown({key})")


# Pretty labels (CamelCase per the Kafka protocol spec) for the keys we handle.
_API_KEY_LABELS: dict[ApiKey, str] = {
    ApiKey.PRODUCE: "Produce",
    ApiKey.FETCH: "Fetch",
    ApiKey.LIST_OFFSETS: "ListOffsets",
    ApiKey.METADATA: "Metadata",
    ApiKey.OFFSET_COMMIT: "OffsetCommit",
    ApiKey.OFFSET_FETCH: "OffsetFetch",
    ApiKey.FIND_COORDINATOR: "FindCoordinator",
    ApiKey.JOIN_GROUP: "JoinGroup",
    ApiKey.HEARTBEAT: "Heartbeat",
    ApiKey.LEAVE_GROUP: "LeaveGroup",
    ApiKey.SYNC_GROUP: "SyncGroup",
    ApiKey.API_VERSIONS: "ApiVersions",
}

# Keys we don't handle but still want to name in logs / ApiVersions negotiation.
_EXTRA_API_KEY_LABELS: dict[int, str] = {
    4: "LeaderAndIsr", 5: "StopReplica", 6: "UpdateMetadata",
    7: "ControlledShutdown", 15: "DescribeGroups", 16: "ListGroups",
    17: "SaslHandshake", 19: "CreateTopics", 20: "DeleteTopics",
    36: "SaslAuthenticate", 37: "CreatePartitions",
}


class ErrorCode(IntEnum):
    """Kafka error codes returned in response bodies."""

    NONE = 0
    LEADER_NOT_AVAILABLE = 5
    COORDINATOR_NOT_AVAILABLE = 15
    ILLEGAL_GENERATION = 22
    UNKNOWN_MEMBER_ID = 25
    REBALANCE_IN_PROGRESS = 27
    UNSUPPORTED_VERSION = 35


class ParseError(ValueError):
    """Raised when a request payload is too short or malformed."""


# (api_key, min_version, max_version) ranges advertised to clients.
#
# Rule: only advertise versions we can actually decode.  kafka-python infers
# the broker version from these ranges — advertising Metadata v5 makes it
# detect Kafka >= 1.0.0 and send RecordBatch (magic=2) Produce requests.
SUPPORTED_APIS: list[tuple[int, int, int]] = [
    (ApiKey.PRODUCE, 0, 7),
    (ApiKey.FETCH, 0, 4),
    (ApiKey.LIST_OFFSETS, 0, 2),
    (ApiKey.METADATA, 0, 5),
    (ApiKey.OFFSET_COMMIT, 0, 2),
    (ApiKey.OFFSET_FETCH, 0, 1),
    (ApiKey.FIND_COORDINATOR, 0, 0),
    (ApiKey.JOIN_GROUP, 0, 1),
    (ApiKey.HEARTBEAT, 0, 1),
    (ApiKey.SYNC_GROUP, 0, 1),
    (ApiKey.API_VERSIONS, 0, 0),
]
