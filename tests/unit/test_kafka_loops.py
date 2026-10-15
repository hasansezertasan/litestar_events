"""Mocked-transport unit tests for the Kafka (aiokafka) emitter.

No broker needed: ``aiokafka`` producer/consumer classes are stubbed with
in-memory fakes covering ``__aenter__`` validation and lifecycle,
``__aexit__`` teardown, the publisher loop, and every consumer-loop branch
(valid dispatch, tombstone skip, unparseable payload, unknown event, listener
isolation, and commit failure).
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import aiokafka
import pytest
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


class _FakeProducer:
    instances: ClassVar[list[_FakeProducer]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.sent: list[tuple[str, bytes]] = []
        self.headers: list[Any] = []
        self.started = False
        self.stopped = False
        self.send_error: Exception | None = None
        _FakeProducer.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(self, topic: str, body: bytes, headers: Any = None) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append((topic, body))
        self.headers.append(headers)


class _FakeConsumer:
    instances: ClassVar[list[_FakeConsumer]] = []

    def __init__(self, *topics: str, **kwargs: Any) -> None:
        self.topics = topics
        self.kwargs = kwargs
        self.messages: list[Any] = []
        self.commits = 0
        self.commit_error: Exception | None = None
        self.started = False
        self.stopped = False
        _FakeConsumer.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def __aiter__(self):
        async def _gen():
            for message in self.messages:
                yield message

        return _gen()

    async def commit(self) -> None:
        if self.commit_error is not None:
            err, self.commit_error = self.commit_error, None
            raise err
        self.commits += 1


def _patch_aiokafka(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeProducer.instances.clear()
    _FakeConsumer.instances.clear()
    monkeypatch.setattr(aiokafka, "AIOKafkaProducer", _FakeProducer)
    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", _FakeConsumer)


def _emitter(*listeners: Any, **kwargs: Any):
    from litestar_events.contrib.kafka.emitter import KafkaEventEmitter

    return KafkaEventEmitter(listeners, **kwargs)


def _record(topic: str, *args: Any, **kwargs: Any) -> Any:
    return SimpleNamespace(
        topic=topic, value=json.dumps({"args": list(args), "kwargs": kwargs}).encode()
    )


async def test_enter_starts_clients_and_exit_stops_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("user_registered")
    async def lstn(**_: Any) -> None:
        pass

    _patch_aiokafka(monkeypatch)
    emitter = _emitter(lstn, group_id="test-group")

    async with emitter:
        assert _FakeProducer.instances[0].started is True
        assert _FakeConsumer.instances[0].started is True
        assert _FakeConsumer.instances[0].topics == ("litestar.events.user_registered",)
        assert emitter._consumer_task is not None
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.sleep(0.1)

    assert _FakeProducer.instances[0].stopped is True
    assert _FakeConsumer.instances[0].stopped is True
    assert emitter._publish_queue is None
    assert _FakeProducer.instances[0].headers == [
        [("content-type", b"application/json")]
    ]
    topic, body = _FakeProducer.instances[0].sent[0]
    assert topic == "litestar.events.user_registered"
    assert json.loads(body) == {
        "v": 1,
        "cid": IsStr(),
        "args": [],
        "kwargs": {"email": "ada@example.com"},
    }


async def test_enter_without_listeners_skips_consumer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_aiokafka(monkeypatch)
    emitter = _emitter()

    async with emitter:
        assert _FakeConsumer.instances == []
        assert emitter._consumer_task is None

    assert _FakeProducer.instances[0].stopped is True


async def test_enter_rejects_invalid_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    @listener("bad id")
    async def lstn(**_: Any) -> None:
        pass

    _patch_aiokafka(monkeypatch)
    emitter = _emitter(lstn)
    with pytest.raises(ValueError, match="invalid Kafka topic"):
        async with emitter:
            pass  # pragma: no cover - enter raises before the body


async def test_publisher_loop_logs_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_aiokafka(monkeypatch)
    emitter = _emitter()
    async with emitter:
        _FakeProducer.instances[0].send_error = RuntimeError("boom")
        emitter.emit("user_registered", 1)
        await asyncio.sleep(0.1)
    assert _FakeProducer.instances[0].sent == []


async def test_consumer_loop_full_branch_coverage() -> None:
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    emitter = _emitter(_raw_listener("evt"), good)
    topic = emitter._topic("evt")
    consumer = _FakeConsumer(topic)
    consumer.commit_error = RuntimeError("commit boom")
    consumer.messages = [
        _record(topic, 1),  # valid: dispatches (first commit fails -> logged)
        SimpleNamespace(topic=topic, value=b"not-json{{{"),  # dropped
        _record("litestar.events.unknown", 2),  # no listeners -> dropped
        _record(topic, 2),  # valid: dispatches and commits
    ]
    emitter._consumer = cast("Any", consumer)

    await emitter._consumer_loop()

    assert received == [(1,), (2,)]
    # One-shot commit failure: the first dispatch logged it, the second
    # committed.
    assert consumer.commits == 1


async def test_exit_without_enter_is_safe() -> None:
    emitter = _emitter()
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None
