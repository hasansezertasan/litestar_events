"""Unit tests for the null-payload guards in the consumer paths.

Two backends grew a defensive ``is None`` guard before ``json.loads``:

* kafka skips tombstone / empty records (``msg.value is None``) so a null
  record cannot crash the consumer loop with ``json.loads(None)``;
* pub/sub drops (with a warning) messages whose ``data is None``.

Both are exercised here in isolation -- no broker -- by driving the
consumer/handler directly with hand-built message stand-ins.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

from litestar.events import listener


class _FakeKafkaConsumer:
    """Async-iterable stand-in for ``AIOKafkaConsumer`` over fixed messages."""

    def __init__(self, messages: list[Any]) -> None:
        self._messages = messages
        self.commits = 0

    async def __aiter__(self):
        for msg in self._messages:
            yield msg

    async def commit(self) -> None:
        self.commits += 1


async def test_kafka_consumer_skips_tombstone() -> None:
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def lstn(*args: Any) -> None:
        received.append(args)

    from litestar_events.contrib.kafka.emitter import KafkaEventEmitter

    emitter = KafkaEventEmitter([lstn])
    topic = emitter._topic("evt")
    consumer = _FakeKafkaConsumer([
        SimpleNamespace(topic=topic, value=None),  # tombstone: must skip
        SimpleNamespace(
            topic=topic, value=json.dumps({"args": [1], "kwargs": {}}).encode()
        ),
    ])
    emitter._consumer = cast("Any", consumer)

    await emitter._consumer_loop()

    # Only the real record reached the listener; the tombstone was skipped
    # before json.loads (which would otherwise raise on None).
    assert received == [(1,)]
    # commit runs only after a dispatched message, not for the tombstone.
    assert consumer.commits == 1


async def test_pubsub_drops_message_with_no_data() -> None:
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def lstn(*args: Any) -> None:
        received.append(args)

    from litestar_events.contrib.pubsub.emitter import PubSubEventEmitter

    emitter = PubSubEventEmitter([lstn], project_id="test")

    await emitter._handle_message(
        cast("Any", SimpleNamespace(data=None, attributes={"event_id": "evt"}))
    )
    assert received == []  # no-data message dropped, listener never invoked

    await emitter._handle_message(
        cast(
            "Any",
            SimpleNamespace(
                data=json.dumps({"args": [1], "kwargs": {}}).encode(),
                attributes={"event_id": "evt"},
            ),
        )
    )
    assert received == [(1,)]  # a real message still dispatches
