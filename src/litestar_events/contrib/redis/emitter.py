from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from litestar.events import BaseEventEmitterBackend, EventListener
from typing_extensions import Self

from litestar_events._queue import QueuedEmitterMixin, QueuePayload, require

if TYPE_CHECKING:
    from collections.abc import Sequence

    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub

__all__ = ["RedisEventEmitter"]


logger = logging.getLogger(__name__)


class RedisEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by Redis Pub/Sub.

    Delivery semantics:
      - At-most-once, fire-and-forget. Events published while no subscriber
        is connected are lost; Redis Pub/Sub has no buffer or persistence.
      - Listener exceptions are caught per-listener; siblings still complete.
      - Every app instance receives every event (broadcast/fanout).

    Channel layout:
      - One Redis channel per registered event id: ``f"{channel_prefix}{event_id}"``.
      - Exact ``SUBSCRIBE`` (not ``PSUBSCRIBE``): the set of channels is the
        closed set of event ids attached to ``listeners`` at app construction.
        Inspect with ``PUBSUB CHANNELS '<prefix>*'`` in ``redis-cli``.

    Not suitable for:
      - durability across restarts (use the rabbit backend),
      - work-queue / single-consumer semantics (use the rabbit backend with
        a shared ``queue_name``),
      - external producers emitting arbitrary event ids (use ``litestar-channels``).
    """

    def __init__(
        self,
        listeners: Sequence[EventListener],
        *,
        redis_url: str = "redis://localhost:6379/0",
        channel_prefix: str = "litestar.events:",
    ) -> None:
        super().__init__(listeners)
        self._redis_url = redis_url
        self._channel_prefix = channel_prefix

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._client: Redis | None = None
        self._pubsub: PubSub | None = None
        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    def _channel(self, event_id: str) -> str:
        return f"{self._channel_prefix}{event_id}"

    async def __aenter__(self) -> Self:
        from redis.asyncio import Redis

        self._client = Redis.from_url(self._redis_url)
        self._pubsub = self._client.pubsub()

        channels = [self._channel(event_id) for event_id in self._by_event]
        if channels:
            await self._pubsub.subscribe(*channels)
            self._consumer_task = asyncio.create_task(self._consumer_loop())

        self._publish_queue = asyncio.Queue()
        self._publisher_task = asyncio.create_task(self._publisher_loop())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        for task in (self._publisher_task, self._consumer_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._close_queue()
        if self._pubsub is not None:
            await self._pubsub.aclose()
        if self._client is not None:
            await self._client.aclose()

    async def _publisher_loop(self) -> None:
        queue = require(self._publish_queue, "publish queue")
        client = require(self._client, "Redis client")
        while True:
            event_id, args, kwargs = await queue.get()
            try:
                body = json.dumps({"args": list(args), "kwargs": kwargs})
                await client.publish(self._channel(event_id), body)
            except Exception:
                logger.exception("Failed to publish event %s", event_id)

    async def _consumer_loop(self) -> None:
        pubsub = require(self._pubsub, "Redis pubsub")
        prefix_len = len(self._channel_prefix)
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            try:
                channel = message["channel"]
                if isinstance(channel, bytes):
                    channel = channel.decode()
                event_id = channel[prefix_len:]

                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode()
                payload = json.loads(data)
                args = payload.get("args", [])
                kwargs = payload.get("kwargs", {})
            except Exception:
                logger.exception("Dropping unparseable Redis message")
                continue

            listeners = self._by_event.get(event_id, [])
            if not listeners:
                continue

            async def _run_one(listener: EventListener) -> None:
                try:
                    await listener.fn(*args, **kwargs)
                except Exception:
                    logger.exception(
                        "Listener %s failed for event %s",
                        getattr(listener.fn, "__name__", repr(listener.fn)),
                        event_id,
                    )

            await asyncio.gather(
                *(_run_one(listener) for listener in listeners), return_exceptions=True
            )
