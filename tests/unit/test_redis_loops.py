"""Mocked-transport unit tests for the Redis emitter (no broker needed).

Covers ``__aenter__``/``__aexit__`` lifecycle, the publisher loop (success and
failure), and the consumer loop (valid dispatch, non-message frames, bytes/str
payloads, unparseable payloads, unknown events, and listener isolation) by
driving the emitter with in-memory fakes for ``redis.asyncio``.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, cast

import redis.asyncio as redis_asyncio
from dirty_equals import IsStr
from litestar.events import EventListener, listener


def _raw_listener(event_id: str) -> EventListener:
    """Cover the emitter's per-listener guard with a raising ``fn``.

    Litestar wraps ``fn`` in its own error handler, so a normally-decorated
    failing listener never reaches the emitter's per-listener ``except``
    guard. Overwriting ``.fn`` with the raw coroutine covers that guard (and
    proves sibling isolation at the emitter layer).
    """

    async def _boom(*args: Any, **kwargs: Any) -> None:
        msg = "raw listener boom"
        raise RuntimeError(msg)

    evt_listener = listener(event_id)(_boom)
    evt_listener.fn = _boom
    return evt_listener


class _FakePubSub:
    """Stand-in for ``redis.asyncio.client.PubSub`` over scripted messages."""

    def __init__(self, messages: list[dict[str, Any]] | None = None) -> None:
        self._messages = list(messages or [])
        self.subscribed: list[str] = []
        self.aclosed = False

    async def subscribe(self, *channels: str) -> None:
        self.subscribed.extend(channels)

    async def aclose(self) -> None:
        self.aclosed = True

    async def listen(self):
        for message in self._messages:
            yield message


class _FakeRedis:
    """Stand-in for ``redis.asyncio.Redis`` recording publishes."""

    def __init__(self, pubsub: _FakePubSub | None = None) -> None:
        self.pubsub_obj = pubsub or _FakePubSub()
        self.published: list[tuple[str, str]] = []
        self.aclosed = False
        self.publish_error: Exception | None = None

    async def ping(self) -> bool:
        return True

    def pubsub(self) -> _FakePubSub:
        return self.pubsub_obj

    async def publish(self, channel: str, body: str) -> None:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((channel, body))

    async def aclose(self) -> None:
        self.aclosed = True


def _patch_redis(monkeypatch: Any, fake: _FakeRedis) -> None:
    monkeypatch.setattr(redis_asyncio.Redis, "from_url", lambda *args: fake)


def _emitter(*listeners: Any, **kwargs: Any):
    from litestar_events.contrib.redis.emitter import RedisEventEmitter

    return RedisEventEmitter(listeners, **kwargs)


def test_channel_naming() -> None:
    emitter = _emitter()
    assert emitter._channel("user_registered") == "litestar.events:user_registered"


async def test_enter_subscribes_and_exit_closes(monkeypatch: Any) -> None:
    @listener("user_registered")
    async def lstn(**_: Any) -> None:
        pass

    fake = _FakeRedis()
    _patch_redis(monkeypatch, fake)
    emitter = _emitter(lstn)

    async with emitter:
        assert fake.pubsub_obj.subscribed == ["litestar.events:user_registered"]
        assert emitter._consumer_task is not None
        assert emitter._publisher_task is not None
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.sleep(0.1)

    assert fake.pubsub_obj.aclosed is True
    assert fake.aclosed is True
    assert emitter._publish_queue is None  # teardown drops the queue
    assert len(fake.published) == 1
    channel, body = fake.published[0]
    assert channel == "litestar.events:user_registered"
    assert json.loads(body) == {
        "v": 1,
        "cid": IsStr(),
        "args": [],
        "kwargs": {"email": "ada@example.com"},
    }


async def test_enter_without_listeners_skips_consumer(monkeypatch: Any) -> None:
    fake = _FakeRedis()
    _patch_redis(monkeypatch, fake)
    emitter = _emitter()

    async with emitter:
        assert fake.pubsub_obj.subscribed == []
        assert emitter._consumer_task is None
        assert emitter._publisher_task is not None

    assert fake.aclosed is True


async def test_publisher_loop_logs_failure(monkeypatch: Any) -> None:
    fake = _FakeRedis()
    fake.publish_error = RuntimeError("boom")
    _patch_redis(monkeypatch, fake)
    emitter = _emitter()

    async with emitter:
        emitter.emit("user_registered", 1)
        await asyncio.sleep(0.1)

    assert fake.published == []  # publish raised; loop logged and continued


async def test_consumer_loop_dispatches_and_isolates() -> None:
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    from litestar_events.contrib.redis.emitter import RedisEventEmitter

    emitter = RedisEventEmitter([_raw_listener("evt"), good])
    prefix = "litestar.events:"

    def msg(event_id: str, payload: Any) -> dict[str, Any]:
        body = payload if isinstance(payload, str) else json.dumps(payload)
        return {"type": "message", "channel": f"{prefix}{event_id}", "data": body}

    emitter._pubsub = cast(
        "Any",
        _FakePubSub([
            {"type": "subscribe", "channel": f"{prefix}evt", "data": 1},  # skipped
            msg("evt", {"args": [1], "kwargs": {}}),  # str channel/data
            {
                "type": "message",  # bytes channel/data
                "channel": f"{prefix}evt".encode(),
                "data": json.dumps({"args": [2], "kwargs": {}}).encode(),
            },
            msg("evt", "not-json{{{"),
            msg("unknown", {"args": [3], "kwargs": {}}),
        ]),
    )

    await emitter._consumer_loop()

    # Both valid messages reached the good listener despite the failing
    # sibling; unparseable and unknown-event messages were dropped.
    assert received == [(1,), (2,)]


async def test_exit_without_enter_is_safe() -> None:
    from litestar_events.contrib.redis.emitter import RedisEventEmitter

    emitter = RedisEventEmitter([])
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None
