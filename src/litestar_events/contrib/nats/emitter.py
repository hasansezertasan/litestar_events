from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING

from litestar.events import BaseEventEmitterBackend, EventListener
from typing_extensions import Self

from litestar_events._queue import (
    QueuedEmitterMixin,
    QueuePayload,
    native_headers,
    require,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from nats.aio.client import Client as NATSClient
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription

    from litestar_events.codec import Codec
    from litestar_events.config import EventsRole

__all__ = ["NATSEventEmitter"]


logger = logging.getLogger(__name__)

_VALID_SUBJECT_TOKEN = re.compile(r"^[^\s*>.]+$")


def _validate_subject(subject: str) -> None:
    if not subject:
        msg = "NATS subject must not be empty."
        raise ValueError(msg)
    tokens = subject.split(".")
    for token in tokens:
        if not _VALID_SUBJECT_TOKEN.match(token):
            msg = (
                f"Invalid NATS subject {subject!r}: tokens must be non-empty "
                "and must not contain whitespace, '*', or '>'."
            )
            raise ValueError(msg)


class NATSEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by core NATS pub/sub.

    Delivery semantics:
      - At-most-once, fire-and-forget. Events published to a subject with no
        active subscriber are dropped. Core NATS has no buffer or persistence;
        for durability use JetStream (not implemented by this backend).
      - Listener exceptions are caught per-listener; siblings still complete.
      - Every app instance receives every event (broadcast/fanout).

    Subject layout:
      - One NATS subject per registered event id:
        ``f"{subject_prefix}{event_id}"``.
      - Default prefix ``"litestar.events."`` follows NATS dot-separated
        token convention.
      - Exact ``SUB`` (no wildcards): the set of subjects is the closed set
        of event ids attached to ``listeners`` at app construction.
      - Inspect from the NATS CLI with ``nats sub 'litestar.events.>'``.

    Subject validation:
      - The combination of prefix + event id is validated at ``__aenter__``.
        Tokens (between dots) must not be empty or contain whitespace,
        ``*``, or ``>`` (which are reserved as NATS wildcards).
      - Litestar's canonical snake_case event ids always pass.

    Not suitable for:
      - durability across restarts (use the rabbit backend or NATS JetStream),
      - work-queue / single-consumer semantics (use the rabbit backend with
        a shared ``queue_name``; NATS queue groups are not currently exposed
        by this backend).
    """

    durable = False

    def __init__(
        self,
        listeners: Sequence[EventListener],
        *,
        role: EventsRole | str | None = None,
        codec: Codec | None = None,
        servers: str | Sequence[str] = "nats://localhost:4222",
        subject_prefix: str = "litestar.events.",
    ) -> None:
        super().__init__(listeners)
        self._init_role(role)
        self._init_codec(codec)
        self._servers = servers
        self._subject_prefix = subject_prefix

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._client: NATSClient | None = None
        self._subscriptions: list[Subscription] = []
        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._publisher_task: asyncio.Task[None] | None = None

    def _subject(self, event_id: str) -> str:
        return f"{self._subject_prefix}{event_id}"

    async def __aenter__(self) -> Self:
        import nats

        for event_id in self._by_event:
            try:
                _validate_subject(self._subject(event_id))
            except ValueError as exc:
                msg = f"event_id {event_id!r} produces an invalid NATS subject. {exc}"
                raise ValueError(msg) from exc

        servers = (
            self._servers if isinstance(self._servers, str) else list(self._servers)
        )
        self._client = await nats.connect(servers=servers)

        if self._consumes:
            for event_id in self._by_event:
                subject = self._subject(event_id)
                sub = await self._client.subscribe(
                    subject, cb=self._make_callback(event_id)
                )
                self._subscriptions.append(sub)

        if self._publishes:
            self._publish_queue = asyncio.Queue()
            self._publisher_task = asyncio.create_task(self._publisher_loop())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._publisher_task is not None:
            self._publisher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._publisher_task
        self._close_queue()

        for sub in self._subscriptions:
            try:
                await sub.unsubscribe()
            except Exception:
                logger.exception("Failed to unsubscribe from %s", sub.subject)

        if self._client is not None:
            await self._client.drain()
            await self._client.close()

    async def _publisher_loop(self) -> None:
        queue = require(self._publish_queue, "publish queue")
        client = require(self._client, "NATS client")
        while True:
            event_id, body, meta = await queue.get()
            try:
                await client.publish(
                    self._subject(event_id),
                    body,
                    headers=native_headers(
                        meta, self._codec.content_type, key="Content-Type"
                    ),
                )
            except Exception:
                logger.exception("Failed to publish event %s", event_id)

    def _make_callback(self, event_id: str) -> Callable[[Msg], Awaitable[None]]:
        listeners = self._by_event[event_id]

        async def _callback(msg: Msg) -> None:
            try:
                event = self._codec.decode(msg.data)
            except Exception:
                logger.exception(
                    "Dropping unparseable message on subject %s", msg.subject
                )
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

        return _callback
