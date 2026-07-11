"""Control plane (PostgreSQL): broker liveness and partition leadership.

Postgres is the small, strongly-consistent coordination layer — the equivalent
of ZooKeeper/KRaft in real Kafka.  Brokers heartbeat into ``broker_health`` and
race to claim rows in ``partition_leaders``; a dead broker's partitions are
freed and re-claimed by the survivors.

The object store holds the (large) data; this holds the (tiny) metadata that
needs a consensus authority.
"""

from __future__ import annotations

import logging

import psycopg2
from psycopg2.extras import DictCursor

from config import Settings

log = logging.getLogger("kafka.control_plane")


class ControlPlane:
    """Thin, synchronous wrapper over the coordination tables."""

    def __init__(self, settings: Settings) -> None:
        self._dsn = settings.postgres_dsn

    def _connect(self):
        return psycopg2.connect(self._dsn, cursor_factory=DictCursor)

    def init_schema(self) -> None:
        """Create the coordination tables if they don't exist."""
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS broker_health (
                        broker_id INT PRIMARY KEY,
                        last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS partition_leaders (
                        topic VARCHAR NOT NULL,
                        partition INT NOT NULL,
                        leader_id INT,
                        PRIMARY KEY (topic, partition)
                    )
                """)
                conn.commit()
            log.info("PostgreSQL: schema initialized")
        except Exception as exc:
            log.error("PostgreSQL init error: %s", exc)

    def heartbeat(self, broker_id: int) -> None:
        """Record that *broker_id* is alive right now."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO broker_health (broker_id, last_heartbeat)
                VALUES (%s, NOW())
                ON CONFLICT (broker_id) DO UPDATE SET last_heartbeat = NOW()
            """, (broker_id,))
            conn.commit()

    def reap_dead_brokers(self, timeout_s: int) -> None:
        """Free partitions of, and remove, brokers silent for > *timeout_s*."""
        stale = "%s * INTERVAL '1 second'"
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"""
                UPDATE partition_leaders SET leader_id = NULL
                WHERE leader_id IN (
                    SELECT broker_id FROM broker_health
                    WHERE last_heartbeat < NOW() - {stale}
                )
            """, (timeout_s,))
            if cur.rowcount:
                log.info("PostgreSQL: usurped %d partition(s) from dead brokers", cur.rowcount)

            cur.execute(f"""
                DELETE FROM broker_health WHERE last_heartbeat < NOW() - {stale}
            """, (timeout_s,))
            if cur.rowcount:
                log.info("PostgreSQL: removed %d dead broker(s)", cur.rowcount)
            conn.commit()

    def claim_partition(
        self, topic: str, partition: int, broker_id: int, preferred_broker_id: int
    ) -> bool:
        """Claim leadership for a partition.

        The preferred broker seeds the row with itself; other brokers seed it as
        leaderless. Any broker can then take a NULL leadership or refresh its own.
        """
        with self._connect() as conn, conn.cursor() as cur:
            initial_leader = broker_id if broker_id == preferred_broker_id else None
            cur.execute("""
                INSERT INTO partition_leaders (topic, partition, leader_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (topic, partition) DO NOTHING
            """, (topic, partition, initial_leader))
            cur.execute("""
                UPDATE partition_leaders SET leader_id = %s
                WHERE topic = %s AND partition = %s
                  AND (leader_id IS NULL OR leader_id = %s)
            """, (broker_id, topic, partition, broker_id))
            claimed = cur.rowcount > 0
            if claimed:
                conn.commit()
            return claimed

    def partition_leaders(self) -> dict[str, dict[int, int]]:
        """Current leadership as ``{topic: {partition: leader_id}}``."""
        leaders: dict[str, dict[int, int]] = {}
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT topic, partition, leader_id FROM partition_leaders "
                "WHERE leader_id IS NOT NULL"
            )
            for row in cur.fetchall():
                leaders.setdefault(row["topic"], {})[row["partition"]] = row["leader_id"]
        return leaders
