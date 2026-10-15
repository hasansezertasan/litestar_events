from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING

from litestar.events import BaseEventEmitterBackend, EventListener
from typing_extensions import Self

from litestar_events._queue import QueuedEmitterMixin, QueuePayload, require

if TYPE_CHECKING:
    from collections.abc import Sequence

    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    from litestar_events.codec import Codec
    from litestar_events.config import EventsRole

__all__ = ["KafkaEventEmitter"]


logger = logging.getLogger(__name__)

_VALID_KAFKA_TOPIC = re.compile(r"^[A-Za-z0-9._-]{1,249}$")


def _validate_topic(topic: str) -> None:
    if not _VALID_KAFKA_TOPIC.match(topic):
        msg = (
            f"Invalid Kafka topic {topic!r}. Topics must match "
            "[A-Za-z0-9._-] and be 1..249 chars."
        )
        raise ValueError(msg)


class KafkaEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by ``aiokafka`` (pure-Python Kafka).

    Delivery semantics:
      - At-least-once with consumer-group offsets. Auto-commit is disabled;
        offsets are committed only after listener dispatch returns, so a
        worker crash mid-dispatch causes the broker to redeliver.
      - Listener exceptions are caught per-listener; siblings still complete.
        A listener that raises does not block the commit — the contract is
        at-least-once at the transport layer, not at the listener layer.
      - ``group_id`` controls fanout vs work-queue:
          * Unique per instance (default, random UUID): every app instance
            sees every event (broadcast/fanout).
          * Shared across instances: exactly one instance handles each event
            (work-queue semantics).

    Topic layout:
      - One Kafka topic per registered event id: ``f"{topic_prefix}{event_id}"``.
      - Validated at ``__aenter__`` against Kafka's topic-name rules.
      - Topic auto-creation depends on broker config
        (``auto.create.topics.enable``). For production, pre-create topics
        with explicit partitions/replication.

    Pairs with ``ConfluentEventEmitter`` (``confluent-kafka``) as a faster
    C-backed alternative. Same semantics, same wire protocol.
    """

    def __init__(
        self,
        listeners: Sequence[EventListener],
        *,
        role: EventsRole | str | None = None,
        codec: Codec | None = None,
        bootstrap_servers: str = "localhost:9092",
        group_id: str | None = None,
        topic_prefix: str = "litestar.events.",
        auto_offset_reset: str = "latest",
    ) -> None:
        super().__init__(listeners)
        self._init_role(role)
        self._init_codec(codec)
        # An anonymous queue/subscription/group dies with the process, so a
        # restarted or late worker misses what was emitted meanwhile.
        self.durable = group_id is not None
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id or f"litestar-events-{uuid.uuid4()}"
        self._topic_prefix = topic_prefix
        self._auto_offset_reset = auto_offset_reset

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    def _topic(self, event_id: str) -> str:
        return f"{self._topic_prefix}{event_id}"

    async def __aenter__(self) -> Self:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

        for event_id in self._by_event:
            try:
                _validate_topic(self._topic(event_id))
            except ValueError as exc:
                msg = f"event_id {event_id!r} produces an invalid Kafka topic. {exc}"
                raise ValueError(msg) from exc

        if self._publishes:
            self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
            await self._producer.start()

        if self._by_event and self._consumes:
            topics = [self._topic(event_id) for event_id in self._by_event]
            self._consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                auto_offset_reset=self._auto_offset_reset,
                enable_auto_commit=False,
            )
            await self._consumer.start()
            self._consumer_task = asyncio.create_task(self._consumer_loop())

        if self._publishes:
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

        if self._consumer is not None:
            await self._consumer.stop()
        if self._producer is not None:
            await self._producer.stop()

    async def _publisher_loop(self) -> None:
        queue = require(self._publish_queue, "publish queue")
        producer = require(self._producer, "Kafka producer")
        while True:
            event_id, body = await queue.get()
            try:
                await producer.send_and_wait(
                    self._topic(event_id),
                    body,
                    headers=[("content-type", self._codec.content_type.encode())],
                )
            except Exception:
                logger.exception("Failed to publish event %s", event_id)

    async def _consumer_loop(self) -> None:
        consumer = require(self._consumer, "Kafka consumer")
        prefix_len = len(self._topic_prefix)
        async for msg in consumer:
            topic = msg.topic
            event_id = topic[prefix_len:]
            value = msg.value
            if value is None:  # tombstone / empty record
                logger.debug("Skipping tombstone record on topic %s", topic)
                continue
            try:
                event = self._codec.decode(value)
            except Exception:
                logger.exception("Dropping unparseable message on topic %s", topic)
                continue

            listeners = self._by_event.get(event_id, [])
            if not listeners:
                logger.info("No listeners for event %s; dropping", event_id)
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

            try:
                await consumer.commit()
            except Exception:
                logger.exception(
                    "Failed to commit offset for event %s; message may redeliver",
                    event_id,
                )
