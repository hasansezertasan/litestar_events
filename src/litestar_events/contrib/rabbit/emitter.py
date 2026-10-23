from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Self

from litestar.events import BaseEventEmitterBackend, EventListener

from litestar_events._queue import (
    CORRELATION_HEADER,
    QueuedEmitterMixin,
    QueuePayload,
    require,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    import aio_pika
    from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection

    from litestar_events.codec import Codec
    from litestar_events.config import EventsRole

__all__ = ["RabbitEventEmitter"]


logger = logging.getLogger(__name__)


class RabbitEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by RabbitMQ via aio-pika.

    Delivery semantics:
      - At-least-once. Messages are acked only after all matching listeners run.
      - Listener exceptions are caught per-listener; siblings still complete.
      - Unparseable messages are rejected to the configured DLX (no requeue loop).

    Topology:
      - One durable topic exchange (``exchange_name``).
      - One consumer queue per backend instance.
        * ``queue_name=None`` (default): server-named, exclusive queue.
          Every app instance gets its own copy of every event -> fanout semantics.
        * ``queue_name="..."``: durable shared queue.
          Competing consumers across instances -> work-queue semantics.
      - One DLX (``dlx_name``) bound to a dead-letter queue for poison messages.
    """

    def __init__(
        self,
        listeners: Sequence[EventListener],
        *,
        role: EventsRole | str | None = None,
        codec: Codec | None = None,
        amqp_url: str = "amqp://guest:guest@localhost/",
        exchange_name: str = "litestar.events",
        queue_name: str | None = None,
        dlx_name: str = "litestar.events.dlx",
        prefetch: int = 32,
    ) -> None:
        super().__init__(listeners)
        self._init_role(role)
        self._init_codec(codec)
        # An anonymous queue/subscription/group dies with the process, so a
        # restarted or late worker misses what was emitted meanwhile.
        self.durable = queue_name is not None
        self._amqp_url = amqp_url
        self._exchange_name = exchange_name
        self._queue_name = queue_name
        self._dlx_name = dlx_name
        self._prefetch = prefetch

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._connection: AbstractRobustConnection | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None
        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._publisher_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        import aio_pika

        self._connection = await aio_pika.connect_robust(self._amqp_url)

        pub_channel = await self._connection.channel(publisher_confirms=True)
        self._exchange = await pub_channel.declare_exchange(
            self._exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )

        dlx = await pub_channel.declare_exchange(
            self._dlx_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        dl_queue = await pub_channel.declare_queue(
            f"{self._exchange_name}.dead", durable=True
        )
        await dl_queue.bind(dlx, routing_key="#")

        if self._consumes:
            await self._start_consumer(self._connection)

        if self._publishes:
            self._publish_queue = asyncio.Queue()
            self._publisher_task = asyncio.create_task(self._publisher_loop())
        return self

    async def _start_consumer(self, connection: AbstractRobustConnection) -> None:
        sub_channel = await connection.channel()
        await sub_channel.set_qos(prefetch_count=self._prefetch)
        queue = await sub_channel.declare_queue(
            self._queue_name or "",
            durable=bool(self._queue_name),
            exclusive=self._queue_name is None,
            arguments={"x-dead-letter-exchange": self._dlx_name},
        )
        for event_id in self._by_event:
            await queue.bind(require(self._exchange, "exchange"), routing_key=event_id)

        await queue.consume(self._on_message)

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self._flush_publisher()
        if self._publisher_task is not None:
            self._publisher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._publisher_task
        self._close_queue()
        if self._connection is not None:
            await self._connection.close()

    async def _publisher_loop(self) -> None:
        import aio_pika

        queue = require(self._publish_queue, "publish queue")
        exchange = require(self._exchange, "RabbitMQ exchange")
        while True:
            event_id, body, meta = await queue.get()
            try:
                await exchange.publish(
                    aio_pika.Message(
                        body=body,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        content_type=self._codec.content_type,
                        headers={
                            k: v for k, v in meta.items() if k != CORRELATION_HEADER
                        },
                        correlation_id=meta.get(CORRELATION_HEADER),
                    ),
                    routing_key=event_id,
                )
            except Exception:
                logger.exception("Failed to publish event %s", event_id)
            finally:
                queue.task_done()

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        try:
            event = self._codec.decode(message.body)
        except Exception:
            logger.exception("Rejecting unparseable message to DLX")
            await message.reject(requeue=False)
            return

        event_id = message.routing_key or ""
        listeners = self._by_event.get(event_id, [])
        if not listeners:
            logger.info("No listeners for event %s; acking and dropping", event_id)
            await message.ack()
            return

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
        await message.ack()
