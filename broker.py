"""The :class:`Broker` aggregate — one object wiring the data plane, control
plane and in-memory state together.

Handlers receive a Broker and reach every subsystem through it, so there are no
globals and a test can assemble a broker from fakes.
"""

from __future__ import annotations

from dataclasses import dataclass

from config import Settings
from control_plane import ControlPlane
from coordinator import FetchPurgatory, GroupCoordinator, OffsetStore
from storage import ObjectStore


@dataclass
class Broker:
    settings: Settings
    store: ObjectStore
    control: ControlPlane
    coordinator: GroupCoordinator
    offsets: OffsetStore
    purgatory: FetchPurgatory

    @classmethod
    def create(cls, settings: Settings) -> "Broker":
        store = ObjectStore(settings)
        return cls(
            settings=settings,
            store=store,
            control=ControlPlane(settings),
            coordinator=GroupCoordinator(),
            offsets=OffsetStore(store),
            purgatory=FetchPurgatory(),
        )
