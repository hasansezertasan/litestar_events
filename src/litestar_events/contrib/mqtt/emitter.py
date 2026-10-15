from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from litestar.events import BaseEventEmitterBackend, EventListener
from typing_extensions import Self

from litestar_events._queue import (
    QueuedEmitterMixin,
    QueuePayload,
    native_headers,
    require,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    import aiomqtt

    from litestar_events.codec import Codec
    from litestar_events.config import EventsRole

__all__ = ["MQTTEventEmitter"]


logger = logging.getLogger(__name__)

_INVALID_TOPIC = re.compile(r"[+#]")


def _validate_topic(topic: str) -> None:
    if not topic:
        msg = "MQTT topic must not be empty."
        raise ValueError(msg)
    if _INVALID_TOPIC.search(topic):
        msg = (
            f"Invalid MQTT topic {topic!r}: must not contain '+' or '#' "
            "(those are MQTT wildcards)."
        )
        raise ValueError(msg)
    if "\x00" in topic:
        msg = f"MQTT topic {topic!r} must not contain NUL."
        raise ValueError(msg)


class MQTTEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by MQTT.

    Delivery semantics:
      - At-most-once by default (QoS 0). Increase ``qos`` for at-least-once
        (QoS 1) or exactly-once (QoS 2) at the cost of throughput.
      - Listener exceptions are caught per-listener; siblings still complete.
      - Every connected app instance receives every event (broadcast/fanout).
        For work-queue semantics, use the rabbit backend.
      - Content type and event headers travel as MQTT 5 properties only when
        ``protocol`` is ``aiomqtt.ProtocolVersion.V5``; below v5 there is no
        per-message metadata.

    Topic layout:
      - One MQTT topic per registered event id: ``f"{topic_prefix}{event_id}"``.
      - Default prefix ``"litestar/events/"`` follows MQTT slash-separated
        topic convention.
      - Exact subscribe (no wildcards): the set of topics is the closed set
        of event ids attached to ``listeners`` at app construction.

    Topic validation:
      - Topics must not contain ``+`` or ``#`` (MQTT wildcards) or NUL.
      - Validated at ``__aenter__``; invalid combinations raise immediately.

    Not suitable for:
      - durability across restarts (use the rabbit backend; MQTT brokers
        offer retained messages but those aren't a substitute for a queue),
      - work-queue / single-consumer semantics (use the rabbit backend with
        a shared ``queue_name``).
    """

    durable = False

    def __init__(  # pylint: disable=too-many-arguments  # one keyword knob per MQTT feature; keyword-only past `listeners`
        self,
        listeners: Sequence[EventListener],
        *,
        role: EventsRole | str | None = None,
        codec: Codec | None = None,
        hostname: str = "localhost",
        port: int = 1883,
        username: str | None = None,
        password: str | None = None,
        topic_prefix: str = "litestar/events/",
        qos: int = 0,
        client_id: str | None = None,
        protocol: aiomqtt.ProtocolVersion | None = None,
    ) -> None:
        super().__init__(listeners)
        self._init_role(role)
        self._init_codec(codec)
        self._hostname = hostname
        self._port = port
        self._username = username
        self._password = password
        self._topic_prefix = topic_prefix
        self._qos = qos
        self._client_id = client_id
        self._protocol = protocol

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._client: aiomqtt.Client | None = None
        self._client_cm: Any = None
        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    def _topic(self, event_id: str) -> str:
        return f"{self._topic_prefix}{event_id}"

    async def __aenter__(self) -> Self:
        import aiomqtt

        for event_id in self._by_event:
            try:
                _validate_topic(self._topic(event_id))
            except ValueError as exc:
                msg = f"event_id {event_id!r} produces an invalid MQTT topic. {exc}"
                raise ValueError(msg) from exc

        self._client_cm = aiomqtt.Client(
            hostname=self._hostname,
            port=self._port,
            username=self._username,
            password=self._password,
            identifier=self._client_id,
            protocol=self._protocol,
        )
        self._client = await self._client_cm.__aenter__()
        # Guard (not assert) the external CM contract: stripped asserts under
        # `python -O` would defer a None here to an opaque AttributeError on the
        # subscribe/publish call below.
        if self._client is None:
            msg = "aiomqtt client context manager returned no client"
            raise RuntimeError(msg)

        if self._consumes:
            for event_id in self._by_event:
                await self._client.subscribe(self._topic(event_id), qos=self._qos)

        if self._by_event and self._consumes:
            self._consumer_task = self._watch(
                asyncio.create_task(self._consumer_loop())
            )

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
        if self._client_cm is not None:
            await self._client_cm.__aexit__(exc_type, exc, tb)

    async def _publisher_loop(self) -> None:
        queue = require(self._publish_queue, "publish queue")
        client = require(self._client, "MQTT client")
        while True:
            event_id, body, meta = await queue.get()
            try:
                await client.publish(
                    self._topic(event_id),
                    payload=body,
                    qos=self._qos,
                    properties=self._properties(meta),
                )
            except Exception:
                logger.exception("Failed to publish event %s", event_id)
            finally:
                queue.task_done()

    def _properties(self, meta: dict[str, str]) -> Any:
        """MQTT 5 content type and user properties; ``None`` below v5."""
        import aiomqtt
        from paho.mqtt.packettypes import PacketTypes
        from paho.mqtt.properties import Properties

        if self._protocol != aiomqtt.ProtocolVersion.V5:
            return None
        properties = Properties(PacketTypes.PUBLISH)
        properties.ContentType = self._codec.content_type
        properties.UserProperty = [
            (key, value)
            for key, value in native_headers(meta, self._codec.content_type).items()
            if key != "content-type"
        ]
        return properties

    async def _consumer_loop(self) -> None:
        client = require(self._client, "MQTT client")
        prefix_len = len(self._topic_prefix)
        async for message in client.messages:
            topic = str(message.topic)
            event_id = topic[prefix_len:]
            try:
                data = message.payload
                if isinstance(data, (bytes, bytearray)):
                    data = data.decode()
                event = self._codec.decode(data)
            except Exception:
                logger.exception("Dropping unparseable MQTT message on %s", topic)
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
