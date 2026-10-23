from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import logging
import re
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Self

from litestar.events import BaseEventEmitterBackend, EventListener

from litestar_events._queue import (
    QueuedEmitterMixin,
    QueuePayload,
    native_headers,
    require,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from confluent_kafka import Consumer, Producer

    from litestar_events.codec import Codec
    from litestar_events.config import EventsRole

__all__ = ["ConfluentEventEmitter"]


logger = logging.getLogger(__name__)

_VALID_KAFKA_TOPIC = re.compile(r"^[A-Za-z0-9._-]{1,249}$")


def _validate_topic(topic: str) -> None:
    if not _VALID_KAFKA_TOPIC.match(topic):
        msg = (
            f"Invalid Kafka topic {topic!r}. Topics must match "
            "[A-Za-z0-9._-] and be 1..249 chars."
        )
        raise ValueError(msg)


class ConfluentEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by ``confluent-kafka`` (librdkafka).

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

    Async bridge:
      - ``confluent-kafka`` is a sync C-extension. The producer and consumer
        each run on a dedicated single-worker ``ThreadPoolExecutor`` to
        guarantee ``poll()`` and ``flush()``/``close()`` never run
        concurrently against the same handle (which segfaults librdkafka).
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
        producer_config: dict[str, Any] | None = None,
        consumer_config: dict[str, Any] | None = None,
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
        self._producer_config = producer_config or {}
        self._consumer_config = consumer_config or {}

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._producer: Producer | None = None
        self._consumer: Consumer | None = None
        self._producer_pool: concurrent.futures.ThreadPoolExecutor | None = None
        self._consumer_pool: concurrent.futures.ThreadPoolExecutor | None = None
        self._stopping = False
        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._producer_poll_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    def _topic(self, event_id: str) -> str:
        return f"{self._topic_prefix}{event_id}"

    async def __aenter__(self) -> Self:
        from confluent_kafka import Consumer, Producer

        for event_id in self._by_event:
            try:
                _validate_topic(self._topic(event_id))
            except ValueError as exc:
                msg = f"event_id {event_id!r} produces an invalid Kafka topic. {exc}"
                raise ValueError(msg) from exc

        if self._publishes:
            self._producer_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="confluent-producer"
            )
            self._producer = Producer({
                "bootstrap.servers": self._bootstrap_servers,
                **self._producer_config,
            })

        if self._by_event and self._consumes:
            self._consumer_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="confluent-consumer"
            )
            self._consumer = Consumer({
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": self._group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
                **self._consumer_config,
            })
            await self._run_consumer(
                self._consumer.subscribe,
                [self._topic(event_id) for event_id in self._by_event],
            )
            self._consumer_task = self._watch(
                asyncio.create_task(self._consumer_loop())
            )

        if self._publishes:
            self._publish_queue = asyncio.Queue()
            self._publisher_task = asyncio.create_task(self._publisher_loop())
            self._producer_poll_task = asyncio.create_task(self._producer_poll_loop())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._stopping = True

        await self._flush_publisher()
        if self._publisher_task is not None:
            self._publisher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._publisher_task
        self._close_queue()

        if self._producer_poll_task is not None:
            self._producer_poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._producer_poll_task

        if self._consumer_task is not None:
            try:
                await self._consumer_task
            except Exception:
                logger.exception("Consumer task raised on shutdown")

        if self._producer is not None and self._producer_pool is not None:
            await self._run_producer(self._producer.flush, 5.0)
        if self._consumer is not None and self._consumer_pool is not None:
            await self._run_consumer(self._consumer.close)

        if self._producer_pool is not None:
            self._producer_pool.shutdown(wait=True)
        if self._consumer_pool is not None:
            self._consumer_pool.shutdown(wait=True)

    async def _run_producer(self, fn: Callable[..., Any], *args: Any) -> Any:
        pool = require(self._producer_pool, "producer thread pool")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(pool, fn, *args)

    async def _run_consumer(self, fn: Callable[..., Any], *args: Any) -> Any:
        pool = require(self._consumer_pool, "consumer thread pool")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(pool, fn, *args)

    async def _producer_poll_loop(self) -> None:
        producer = require(self._producer, "Kafka producer")
        while not self._stopping:
            await asyncio.sleep(0.1)
            try:
                await self._run_producer(producer.poll, 0)
            except Exception:
                logger.exception("Producer poll failed")

    async def _publisher_loop(self) -> None:
        queue = require(self._publish_queue, "publish queue")
        producer = require(self._producer, "Kafka producer")
        while True:
            event_id, body, meta = await queue.get()
            try:
                headers: list[tuple[str, str | bytes | None]] = [
                    (key, value.encode())
                    for key, value in native_headers(
                        meta, self._codec.content_type
                    ).items()
                ]
                try:
                    producer.produce(self._topic(event_id), value=body, headers=headers)
                except BufferError:
                    await self._run_producer(producer.poll, 1.0)
                    producer.produce(self._topic(event_id), value=body, headers=headers)
            except Exception:
                logger.exception("Failed to publish event %s", event_id)
            finally:
                queue.task_done()

    async def _consumer_loop(self) -> None:
        from confluent_kafka import KafkaError, KafkaException

        consumer = require(self._consumer, "Kafka consumer")
        prefix_len = len(self._topic_prefix)
        while not self._stopping:
            try:
                msg = await self._run_consumer(consumer.poll, 1.0)
            except KafkaException:
                logger.exception("Consumer poll raised")
                continue
            if msg is None:
                continue
            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:  # pylint: disable=protected-access  # confluent-kafka exposes the EOF marker only as a protected member
                    continue  # benign end-of-partition marker
                if err.fatal():
                    # Unrecoverable: stop consuming rather than spin the loop
                    # re-logging the same error every poll.
                    logger.critical("Fatal Kafka consumer error; stopping: %s", err)
                    break
                logger.error("Consumer error: %s", err)
                continue

            topic = msg.topic() or ""
            event_id = topic[prefix_len:]
            try:
                event = self._codec.decode(msg.value())
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
                await self._run_consumer(
                    lambda m: consumer.commit(message=m, asynchronous=False), msg
                )
            except Exception:
                logger.exception(
                    "Failed to commit offset for event %s; message may redeliver",
                    event_id,
                )
