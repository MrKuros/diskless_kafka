"""Asyncio TCP server: frame the socket, dispatch requests, run failover.

One coroutine per connection reads length-prefixed Kafka frames, hands each to
the handler registry, and writes the framed response back.  A background task
maintains this broker's liveness and partition leadership in the control plane.
"""

from __future__ import annotations

import asyncio
import logging
import struct

import handlers  # noqa: F401 — importing populates the handler registry
from broker import Broker
from config import Settings
from errors import ParseError
from handlers import dispatch
from protocol import parse_request_header

log = logging.getLogger("kafka.server")

_LENGTH_PREFIX_BYTES = 4
_MAX_FRAME_BYTES = 100 * 1024 * 1024  # 100 MiB sanity cap


class BrokerServer:
    def __init__(self, settings: Settings, broker: Broker) -> None:
        self._settings = settings
        self._broker = broker

    async def serve(self) -> None:
        await asyncio.to_thread(self._broker.control.init_schema)
        self._broker.store.get_topic_config()  # warm the cache

        log.info("Loading committed offsets …")
        try:
            await asyncio.to_thread(self._broker.offsets.load)
        except Exception as exc:
            log.warning("Could not load committed offsets: %s", exc)

        server = await asyncio.start_server(
            self._handle_connection, self._settings.host, self._settings.port,
            reuse_address=True,
        )
        addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
        log.info("diskless-kafka listening on %s", addrs)

        async with server:
            failover = asyncio.create_task(self._failover_loop())
            try:
                await server.serve_forever()
            finally:
                failover.cancel()

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        log.info("connection open %s", peer)
        try:
            while True:
                try:
                    raw_len = await reader.readexactly(_LENGTH_PREFIX_BYTES)
                except asyncio.IncompleteReadError:
                    break  # client closed cleanly

                (length,) = struct.unpack(">I", raw_len)
                if length > _MAX_FRAME_BYTES:
                    log.error("frame of %d bytes exceeds cap — dropping connection", length)
                    break

                payload = await reader.readexactly(length)
                try:
                    header = parse_request_header(payload)
                except ParseError as exc:
                    log.warning("header parse failed: %s — skipping frame", exc)
                    continue

                response = await dispatch(self._broker, header, payload)
                if response is None:
                    log.warning("no handler for %s", header.summary())
                    continue

                writer.write(response)
                await writer.drain()
                log.debug("→ %s (%d bytes)", header.summary(), len(response))
        except Exception:
            log.exception("error handling connection %s", peer)
        finally:
            log.info("connection close %s", peer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _failover_loop(self) -> None:
        """Maintain heartbeat and (re)claim leaderless preferred partitions."""
        s = self._settings
        while True:
            try:
                await asyncio.to_thread(self._broker.control.heartbeat, s.node_id)
                await asyncio.to_thread(
                    self._broker.control.reap_dead_brokers, s.dead_broker_timeout_s)

                for topic, cfg in self._broker.store.get_topic_config().items():
                    for part_id in range(cfg.get("partitions", 1)):
                        preferred = (part_id % s.cluster_size) + 1
                        await asyncio.to_thread(
                            self._broker.control.claim_partition,
                            topic, part_id, s.node_id, preferred)
            except Exception as exc:
                log.error("failover loop error: %s", exc)
            await asyncio.sleep(s.heartbeat_interval_s)


async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    settings = Settings.from_env()
    server = BrokerServer(settings, Broker.create(settings))
    await server.serve()


def main() -> None:
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        log.info("server stopped")


if __name__ == "__main__":
    main()
