from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from litestar.events import BaseEventEmitterBackend, EventListener
from typing_extensions import Self

from litestar_events._queue import QueuedEmitterMixin, QueuePayload, require

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_events.codec import Codec
    from litestar_events.config import EventsRole

__all__ = ["MemoryEventEmitter"]


logger = logging.getLogger(__name__)


class MemoryEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """An in-process emitter for tests and local development.

    Events never leave the process, so it cannot connect a separate producer
    and worker: ``litestar events run`` rejects it.
    """

    process_local = True
    durable = False

    def __init__(
        self,
        listeners: Sequence[EventListener],
        *,
        role: EventsRole | str | None = None,
        codec: Codec | None = None,
    ) -> None:
        super().__init__(listeners)
        self._init_role(role)
        self._init_codec(codec)
        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)
        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        self._publish_queue = asyncio.Queue()
        if self._consumes:
            self._consumer_task = asyncio.create_task(self._consumer_loop())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._consumer_task is not None:
            self._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer_task
        self._close_queue()

    async def _consumer_loop(self) -> None:
        queue = require(self._publish_queue, "publish queue")
        while True:
            event_id, body = await queue.get()
            try:
                event = self._codec.decode(body)
            except Exception:
                logger.exception("Dropping unparseable event %s", event_id)
                continue
            for listener in self._by_event.get(event_id, []):
                try:
                    await self._codec.invoke(listener.fn, event_id, event)
                except Exception:
                    logger.exception(
                        "Listener %s failed for event %s",
                        getattr(listener.fn, "__name__", repr(listener.fn)),
                        event_id,
                    )
