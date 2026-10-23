from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Self

from litestar.events import BaseEventEmitterBackend, EventListener

from litestar_events._queue import QueuedEmitterMixin, QueuePayload, require

if TYPE_CHECKING:
    from collections.abc import Sequence

    import zmq.asyncio

    from litestar_events.codec import Codec
    from litestar_events.config import EventsRole

__all__ = ["ZeroMQEventEmitter"]


logger = logging.getLogger(__name__)


class ZeroMQEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by ZeroMQ PUB/SUB sockets.

    ZeroMQ is *brokerless*: there is no server to operate. Each emitter binds a
    ``PUB`` socket and connects a ``SUB`` socket to one or more peer addresses.

    Delivery semantics:
      - At-most-once, fire-and-forget. There is no persistence, no acks, and no
        buffering for absent subscribers. Events published before a subscriber
        has finished connecting are silently dropped (ZeroMQ's "slow joiner"
        problem) -- hence ``subscribe_warmup``.
      - Listener exceptions are caught per-listener; siblings still complete.
      - Broadcast: every connected SUB peer receives every event it subscribed
        to. There is no work-queue / single-consumer mode.

    Topology:
      - The ``event_id`` is the ZeroMQ subscription topic (first message frame);
        the second frame is the ``{"args": [...], "kwargs": {...}}`` JSON body.
      - ``pub_address``: where this instance binds its PUB socket.
      - ``connect_addresses``: peer PUB addresses this instance's SUB connects
        to. Defaults to ``[pub_address]`` (single-process / single-node, the
        common dev case). For cross-process fanout, point each instance's SUB at
        every peer's ``pub_address``.

    Not suitable for:
      - durability across restarts (use the rabbit / sqs / pubsub backends),
      - guaranteed delivery (ZeroMQ drops on full queues / absent peers),
      - dynamic peer discovery (addresses are static configuration).
    """

    durable = False

    def __init__(
        self,
        listeners: Sequence[EventListener],
        *,
        role: EventsRole | str | None = None,
        codec: Codec | None = None,
        pub_address: str = "tcp://127.0.0.1:5557",
        connect_addresses: Sequence[str] | None = None,
        subscribe_warmup: float = 0.2,
    ) -> None:
        super().__init__(listeners)
        self._init_role(role)
        self._init_codec(codec)
        self._pub_address = pub_address
        self._connect_addresses = (
            list(connect_addresses) if connect_addresses is not None else [pub_address]
        )
        self._subscribe_warmup = subscribe_warmup

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._ctx: zmq.asyncio.Context | None = None
        self._pub: zmq.asyncio.Socket | None = None
        self._sub: zmq.asyncio.Socket | None = None
        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        import zmq.asyncio

        import zmq

        self._ctx = zmq.asyncio.Context()
        if self._publishes:
            self._pub = self._ctx.socket(zmq.PUB)
            self._pub.bind(self._pub_address)

        if self._by_event and self._consumes:
            self._sub = self._ctx.socket(zmq.SUB)
            for addr in self._connect_addresses:
                self._sub.connect(addr)
            for event_id in self._by_event:
                self._sub.setsockopt(zmq.SUBSCRIBE, event_id.encode())
            self._consumer_task = self._watch(
                asyncio.create_task(self._consumer_loop())
            )

        # PUB/SUB slow-joiner: give subscriptions time to propagate before the
        # first emit, otherwise early events are dropped.
        await asyncio.sleep(self._subscribe_warmup)

        if self._publishes:
            self._publish_queue = asyncio.Queue()
            self._publisher_task = asyncio.create_task(self._publisher_loop())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self._flush_publisher()
        for task in (self._publisher_task, self._consumer_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._close_queue()
        if self._ctx is not None:
            self._ctx.destroy(linger=0)

    async def _publisher_loop(self) -> None:
        queue = require(self._publish_queue, "publish queue")
        pub = require(self._pub, "ZMQ publisher socket")
        while True:
            event_id, body, _ = await queue.get()
            try:
                await pub.send_multipart([event_id.encode(), body])
            except Exception:
                logger.exception("Failed to publish event %s", event_id)
            finally:
                queue.task_done()

    async def _consumer_loop(self) -> None:
        sub = require(self._sub, "ZMQ subscriber socket")
        while True:
            try:
                frames = await sub.recv_multipart()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("ZMQ receive failed")
                continue
            try:
                event_id = frames[0].decode()
                event = self._codec.decode(frames[1])
            except Exception:
                logger.exception("Dropping unparseable ZMQ message")
                continue

            listeners = self._by_event.get(event_id, [])
            if not listeners:
                continue

            async def _run_one(listener: EventListener) -> None:
                try:
                    await self._codec.invoke(listener.fn, event_id, event)
                except Exception:
                    logger.exception(
                        "Listener %s failed for event %s",
                        getattr(listener.fn, "__name__", repr(listener.fn)),
                        event_id,
                    )

            await asyncio.gather(
                *(_run_one(listener) for listener in listeners), return_exceptions=True
            )
