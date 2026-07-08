"""
diskless_kafka/db.py
────────────────────
PostgreSQL database interactions for the diskless broker.
Manages the control plane, including broker health heartbeats, 
partition leadership election, and broker usurpation during failovers.
"""
import os
import psycopg2
from psycopg2.extras import DictCursor
import logging

log = logging.getLogger("kafka.db")

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://kafka:kafka@localhost:5432/diskless_kafka")

def get_connection():
    return psycopg2.connect(POSTGRES_DSN, cursor_factory=DictCursor)

def init_db():
    """Create tables if they don't exist."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
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
            log.info("PostgreSQL: database initialized")
    except Exception as e:
        log.error("PostgreSQL init error: %s", e)


def heartbeat(broker_id: int):
    """Upsert heartbeat for the broker."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO broker_health (broker_id, last_heartbeat) 
                VALUES (%s, NOW())
                ON CONFLICT (broker_id) DO UPDATE 
                SET last_heartbeat = NOW()
            """, (broker_id,))
        conn.commit()

def usurp_dead_brokers(timeout_sec: int = 10):
    """
    Remove brokers whose heartbeat is older than timeout_sec.
    Their partitions will be left with no leader, or we can just 
    clear their leadership right away.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # 1. Clear partition leaders for dead brokers
            cur.execute(f"""
                UPDATE partition_leaders 
                SET leader_id = NULL 
                WHERE leader_id IN (
                    SELECT broker_id FROM broker_health 
                    WHERE last_heartbeat < NOW() - INTERVAL '{timeout_sec} seconds'
                )
            """)
            cleared = cur.rowcount
            if cleared > 0:
                log.info(f"PostgreSQL: usurped {cleared} partition(s) from dead brokers")
            
            # 2. Remove dead brokers from health table
            cur.execute(f"""
                DELETE FROM broker_health
                WHERE last_heartbeat < NOW() - INTERVAL '{timeout_sec} seconds'
            """)
            removed = cur.rowcount
            if removed > 0:
                log.info(f"PostgreSQL: removed {removed} dead broker(s)")
        conn.commit()

def claim_partition(topic: str, partition: int, broker_id: int, preferred_broker_id: int) -> bool:
    """
    Try to claim leadership for a partition.
    Any broker can initialize the row. If it's the preferred broker, it claims it immediately.
    If it's not preferred, it initializes it as NULL.
    Then, any broker can claim it if it's NULL or if they are already the leader.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # 1. Initialize row if missing
            initial_leader = broker_id if broker_id == preferred_broker_id else None
            cur.execute("""
                INSERT INTO partition_leaders (topic, partition, leader_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (topic, partition) DO NOTHING
            """, (topic, partition, initial_leader))
            
            # 2. Try to update if leader_id is NULL or if we are the current leader
            cur.execute("""
                UPDATE partition_leaders 
                SET leader_id = %s
                WHERE topic = %s AND partition = %s AND (leader_id IS NULL OR leader_id = %s)
            """, (broker_id, topic, partition, broker_id))
            
            success = cur.rowcount > 0
            if success:
                conn.commit()
                # log.info(f"PostgreSQL: Broker {broker_id} claimed/maintained {topic}/{partition}")
            return success


def get_partition_leaders() -> dict[str, dict[int, int]]:
    """
    Returns a nested dict: { topic: { partition: leader_id } }
    """
    leaders = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT topic, partition, leader_id FROM partition_leaders WHERE leader_id IS NOT NULL")
            for row in cur.fetchall():
                t = row['topic']
                p = row['partition']
                l = row['leader_id']
                if t not in leaders:
                    leaders[t] = {}
                leaders[t][p] = l
    return leaders
