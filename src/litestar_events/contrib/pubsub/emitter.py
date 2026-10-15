from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING
from uuid import uuid4

from litestar.events import BaseEventEmitterBackend, EventListener
from typing_extensions import Self

from litestar_events._queue import QueuedEmitterMixin, QueuePayload, require

if TYPE_CHECKING:
    from collections.abc import Sequence

    from gcloud.aio.pubsub import PublisherClient, SubscriberClient, SubscriberMessage

    from litestar_events.codec import Codec
    from litestar_events.config import EventsRole

__all__ = ["PubSubEventEmitter"]


logger = logging.getLogger(__name__)


class PubSubEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by GCP Pub/Sub via gcloud-aio.

    Delivery semantics:
      - At-least-once + durable. A message is acked only after all matching
        in-process listeners run (the subscribe handler returns), so a crash
        mid-dispatch leaves the message to be redelivered.
      - Listener exceptions are caught per-listener; siblings still complete.
        The handler never re-raises, so a permanently-failing listener does not
        cause infinite redelivery (matching the other durable backends).
      - Unparseable or empty (no-data) messages are acked and dropped with a
        logged warning/error.

    Topology:
      - A single topic (``topic_id``) carries every event id. The originating
        ``event_id`` travels in the ``event_id`` message attribute; the body is
        the same ``{"args": [...], "kwargs": {...}}`` JSON contract used by the
        other backends.
      - ``subscription_name=None`` (default): a unique per-instance subscription
        is created (and deleted on exit). Pub/Sub delivers every message to
        every subscription, so each app instance sees every event -> broadcast
        fanout.
      - ``subscription_name="..."``: a shared, durable subscription. Instances
        sharing it are competing consumers -> work-queue semantics.

    Credentials:
      - Production uses the standard GCP credential chain
        (``GOOGLE_APPLICATION_CREDENTIALS`` etc.).
      - ``emulator_host`` (``host:port``) targets a local Pub/Sub emulator; it
        is passed as an explicit ``api_root`` to each client so gcloud-aio skips
        auth, without touching the process-wide ``PUBSUB_EMULATOR_HOST``. Mainly
        for dev and tests.
    """

    def __init__(
        self,
        listeners: Sequence[EventListener],
        *,
        role: EventsRole | str | None = None,
        codec: Codec | None = None,
        project_id: str,
        topic_id: str = "litestar-events",
        subscription_name: str | None = None,
        create_resources: bool = True,
        emulator_host: str | None = None,
        ack_deadline: float | None = None,
    ) -> None:
        super().__init__(listeners)
        self._init_role(role)
        self._init_codec(codec)
        # An anonymous queue/subscription/group dies with the process, so a
        # restarted or late worker misses what was emitted meanwhile.
        self.durable = subscription_name is not None
        self._project_id = project_id
        self._topic_id = topic_id
        self._subscription_name = subscription_name
        self._create_resources = create_resources
        self._emulator_host = emulator_host
        self._ack_deadline = ack_deadline

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._publisher: PublisherClient | None = None
        self._subscriber: SubscriberClient | None = None
        self._topic = ""
        self._subscription = ""
        self._owns_subscription = subscription_name is None
        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    async def _ensure_topic(self) -> None:
        from aiohttp import ClientResponseError

        publisher = require(self._publisher, "Pub/Sub publisher")
        try:
            await publisher.create_topic(self._topic)
        except ClientResponseError as exc:
            if exc.status != 409:  # 409 ALREADY_EXISTS is expected
                raise

    async def _ensure_subscription(self) -> None:
        from aiohttp import ClientResponseError

        subscriber = require(self._subscriber, "Pub/Sub subscriber")
        try:
            await subscriber.create_subscription(self._subscription, self._topic)
        except ClientResponseError as exc:
            if exc.status != 409:
                raise

    async def __aenter__(self) -> Self:
        from gcloud.aio.pubsub import PublisherClient, SubscriberClient

        api_root = f"http://{self._emulator_host}/v1" if self._emulator_host else None

        self._publisher = PublisherClient(api_root=api_root)
        self._topic = f"projects/{self._project_id}/topics/{self._topic_id}"
        if self._create_resources:
            await self._ensure_topic()

        sub_id = self._subscription_name or f"{self._topic_id}-{uuid4().hex}"
        self._subscription = f"projects/{self._project_id}/subscriptions/{sub_id}"

        if self._publishes:
            self._publish_queue = asyncio.Queue()
            self._publisher_task = asyncio.create_task(self._publisher_loop())

        if self._by_event and self._consumes:
            self._subscriber = SubscriberClient(api_root=api_root)
            if self._create_resources:
                await self._ensure_subscription()
            self._consumer_task = asyncio.create_task(self._run_subscribe())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        for task in (self._publisher_task, self._consumer_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._close_queue()

        if (
            self._owns_subscription
            and self._create_resources
            and self._subscriber is not None
        ):
            try:
                await self._subscriber.delete_subscription(self._subscription)
            except Exception:
                logger.exception(
                    "Failed to delete per-instance subscription %s", self._subscription
                )

        if self._subscriber is not None:
            await self._subscriber.close()
        if self._publisher is not None:
            await self._publisher.close()

    async def _publisher_loop(self) -> None:
        from gcloud.aio.pubsub import PubsubMessage

        queue = require(self._publish_queue, "publish queue")
        publisher = require(self._publisher, "Pub/Sub publisher")
        while True:
            event_id, body = await queue.get()
            try:
                await publisher.publish(
                    self._topic,
                    [
                        PubsubMessage(
                            body,
                            event_id=event_id,
                            content_type=self._codec.content_type,
                        )
                    ],
                )
            except Exception:
                logger.exception("Failed to publish event %s", event_id)

    async def _run_subscribe(self) -> None:
        from gcloud.aio.pubsub import subscribe

        subscriber = require(self._subscriber, "Pub/Sub subscriber")
        await subscribe(
            self._subscription,
            self._handle_message,
            subscriber,
            num_producers=1,
            max_messages_per_producer=10,
            ack_deadline=self._ack_deadline,
        )

    async def _handle_message(self, message: SubscriberMessage) -> None:
        event_id = (message.attributes or {}).get("event_id", "")
        # gcloud-aio's ``subscribe`` acks a message once its handler returns
        # normally, so every early ``return`` below both drops *and* acks the
        # message -- a no-data or unparseable message is not redelivered.
        if message.data is None:
            logger.warning(
                "Dropping Pub/Sub message with no data (event_id=%s)", event_id
            )
            return
        try:
            event = self._codec.decode(message.data)
        except Exception:
            logger.exception("Dropping unparseable Pub/Sub message")
            return

        listeners = self._by_event.get(event_id, [])
        if not listeners:
            logger.info("No listeners for event %s; acking and dropping", event_id)
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
