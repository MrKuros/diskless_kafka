"""Runtime configuration, resolved once from the environment.

All broker components read their settings from a single immutable
:class:`Settings` instance rather than scattering ``os.getenv`` calls across
modules.  This keeps the knobs discoverable in one place and makes tests able
to construct a broker with an explicit config instead of monkeypatching env
vars.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _brokers_from_env(raw: str, fallback: "BrokerAddress") -> list["BrokerAddress"]:
    """Parse ``CLUSTER_BROKERS`` ("id:host:port,id:host:port") into addresses.

    Falls back to a single-node cluster containing *fallback* if the value is
    empty or malformed.
    """
    brokers: list[BrokerAddress] = []
    for entry in raw.split(","):
        parts = entry.split(":")
        if len(parts) != 3:
            return [fallback]
        node_id, host, port = parts
        brokers.append(BrokerAddress(int(node_id), host, int(port)))
    return brokers or [fallback]


@dataclass(frozen=True)
class BrokerAddress:
    """Identity of a broker as advertised in Metadata responses."""

    node_id: int
    host: str
    port: int


@dataclass(frozen=True)
class Settings:
    """Immutable snapshot of every environment-driven knob."""

    # Listener
    host: str = "0.0.0.0"
    port: int = 9092

    # This broker's identity
    node_id: int = 1
    advertised_host: str = "localhost"
    advertised_port: int = 9092
    cluster: list[BrokerAddress] = field(default_factory=list)

    # Object store (MinIO / S3)
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "diskless-kafka"
    minio_secure: bool = False

    # Control plane (PostgreSQL)
    postgres_dsn: str = "postgresql://kafka:kafka@localhost:5432/diskless_kafka"

    # Failover loop
    heartbeat_interval_s: float = 3.0
    dead_broker_timeout_s: int = 10

    @classmethod
    def from_env(cls) -> "Settings":
        node_id = int(os.getenv("BROKER_NODE_ID", "1"))
        advertised_host = os.getenv("BROKER_HOST", "localhost")
        advertised_port = int(os.getenv("BROKER_PORT", "9092"))
        self_addr = BrokerAddress(node_id, advertised_host, advertised_port)

        return cls(
            host=os.getenv("HOST", "0.0.0.0"),
            port=int(os.getenv("PORT", "9092")),
            node_id=node_id,
            advertised_host=advertised_host,
            advertised_port=advertised_port,
            cluster=_brokers_from_env(
                os.getenv("CLUSTER_BROKERS", "1:localhost:9092"), self_addr
            ),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            minio_bucket=os.getenv("MINIO_BUCKET", "diskless-kafka"),
            minio_secure=os.getenv("MINIO_SECURE", "0").lower() in ("1", "true"),
            postgres_dsn=os.getenv(
                "POSTGRES_DSN",
                "postgresql://kafka:kafka@localhost:5432/diskless_kafka",
            ),
        )

    @property
    def cluster_size(self) -> int:
        return len(self.cluster) or 1
