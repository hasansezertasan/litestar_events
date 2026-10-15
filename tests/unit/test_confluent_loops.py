"""Mocked-transport unit tests for the Confluent (librdkafka) emitter.

The existing ``test_confluent_consumer.py`` pins consumer-error
classification; these tests cover the rest without a broker: ``__aenter__``
validation and lifecycle, ``__aexit__`` teardown, the publisher loop
(including the ``BufferError`` poll-and-retry path), the producer poll loop,
and the consumer loop's dispatch branches (valid, unparseable, unknown event,
listener isolation, commit failure, poll exception).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, cast

import confluent_kafka
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
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.produced: list[tuple[str, bytes]] = []
        self.headers: list[Any] = []
        self.polls: list[float] = []
        self.flushed: list[float] = []
        self.produce_error: Exception | None = None
        self.produce_failures = 0

    def produce(self, topic: str, value: bytes, headers: Any = None) -> None:
        if self.produce_error is not None and self.produce_failures == 0:
            self.produce_failures += 1
            raise self.produce_error
        self.produced.append((topic, value))
        self.headers.append(headers)

    def poll(self, timeout: float) -> None:
        self.polls.append(timeout)

    def flush(self, timeout: float) -> None:
        self.flushed.append(timeout)


class _FakeMsg:
    def __init__(self, topic: str, payload: Any, *, raw: bool = False) -> None:
        self._topic = topic
        self._payload = payload
        self._raw = raw

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return self._topic

    def value(self) -> Any:
        if self._raw:
            return self._payload
        return json.dumps(self._payload).encode()


class _FakeConsumer:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.subscribed: list[list[str]] = []
        self.poll_results: list[Any] = []
        self.poll_error: Exception | None = None
        self.commits = 0
        self.commit_async: list[bool] = []
        self.commit_error: Exception | None = None
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed.append(topics)

    def poll(self, timeout: float) -> Any:
        if self.poll_error is not None:
            err, self.poll_error = self.poll_error, None
            raise err
        if self.poll_results:
            return self.poll_results.pop(0)
        return None

    def commit(self, *, message: Any = None, asynchronous: bool = False) -> None:
        if self.commit_error is not None:
            err, self.commit_error = self.commit_error, None
            raise err
        self.commits += 1
        self.commit_async.append(asynchronous)

    def close(self) -> None:
        self.closed = True


def _patch_confluent(
    monkeypatch: pytest.MonkeyPatch, producer: _FakeProducer, consumer: _FakeConsumer
) -> None:
    monkeypatch.setattr(confluent_kafka, "Producer", lambda config: producer)
    monkeypatch.setattr(confluent_kafka, "Consumer", lambda config: consumer)


def _emitter(*listeners: Any, **kwargs: Any):
    from litestar_events.contrib.confluent.emitter import ConfluentEventEmitter

    return ConfluentEventEmitter(listeners, **kwargs)


async def test_enter_and_exit_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    @listener("user_registered")
    async def lstn(**_: Any) -> None:
        pass

    producer = _FakeProducer({})
    consumer = _FakeConsumer({})
    _patch_confluent(monkeypatch, producer, consumer)
    emitter = _emitter(lstn, group_id="test-group")
    # Consumer loop would spin on None polls; stop it right away.
    emitter._stopping = True

    async with emitter:
        assert consumer.subscribed == [["litestar.events.user_registered"]]
        assert emitter._consumer_task is not None
        assert emitter._publisher_task is not None
        assert emitter._producer_poll_task is not None
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.sleep(0.1)

    assert producer.flushed == [5.0]
    assert consumer.closed is True
    assert emitter._publish_queue is None
    assert producer.headers[0] == [("content-type", b"application/json")]
    topic, body = producer.produced[0]
    assert topic == "litestar.events.user_registered"
    assert json.loads(body) == {
        "v": 1,
        "cid": IsStr(),
        "args": [],
        "kwargs": {"email": "ada@example.com"},
    }


async def test_enter_rejects_invalid_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    @listener("bad id")
    async def lstn(**_: Any) -> None:
        pass

    _patch_confluent(monkeypatch, _FakeProducer({}), _FakeConsumer({}))
    emitter = _emitter(lstn)
    with pytest.raises(ValueError, match="invalid Kafka topic"):
        async with emitter:
            pass  # pragma: no cover - enter raises before the body


async def test_publisher_retries_on_buffer_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = _FakeProducer({})
    producer.produce_error = BufferError("queue full")
    _patch_confluent(monkeypatch, producer, _FakeConsumer({}))
    emitter = _emitter()
    emitter._stopping = True

    async with emitter:
        emitter.emit("evt", 1)
        await asyncio.sleep(0.2)

    # First produce raised BufferError (poll to free space, then retry).
    assert producer.polls != []
    assert len(producer.produced) == 1


async def test_publisher_logs_generic_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    producer = _FakeProducer({})
    producer.produce_error = RuntimeError("boom")

    # BufferError triggers one retry; anything else is logged. Force the
    # non-retry path by making every produce raise a generic error through
    # the stubbed producer below.
    class _AlwaysFails(_FakeProducer):
        def produce(self, topic: str, value: bytes, headers: Any = None) -> None:
            msg = "boom"
            raise RuntimeError(msg)

    failing = _AlwaysFails({})
    _patch_confluent(monkeypatch, failing, _FakeConsumer({}))
    emitter = _emitter()
    emitter._stopping = True

    async with emitter:
        emitter.emit("evt", 1)
        await asyncio.sleep(0.1)

    assert failing.produced == []


async def test_producer_poll_loop_logs_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    producer = _FakeProducer({})
    _patch_confluent(monkeypatch, producer, _FakeConsumer({}))
    emitter = _emitter()

    original_run_producer = emitter._run_producer
    failed = False

    async def flaky(fn: Any, *args: Any) -> Any:
        nonlocal failed
        if not failed:
            failed = True
            emitter._stopping = True
            msg = "poll boom"
            raise RuntimeError(msg)
        return await original_run_producer(fn, *args)

    monkeypatch.setattr(emitter, "_run_producer", flaky)
    async with emitter:
        await emitter._producer_poll_loop()  # one failing poll, then stopping

    assert failed is True


async def test_consumer_loop_dispatch_branches() -> None:
    from confluent_kafka import KafkaException

    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    from litestar_events.contrib.confluent.emitter import ConfluentEventEmitter

    emitter = ConfluentEventEmitter([_raw_listener("evt"), good], group_id="test-group")
    topic = emitter._topic("evt")
    consumer = _FakeConsumer({})
    # One-shot poll failure first (covers the KafkaException-continue branch
    # through the real executor path), then the dispatch script.
    consumer.poll_error = KafkaException("boom")
    consumer.poll_results = [
        _FakeMsg(topic, {"args": [1], "kwargs": {}}),
        _FakeMsg(topic, b"not-json{{{", raw=True),
        _FakeMsg("litestar.events.unknown", {"args": [2], "kwargs": {}}),
        _FakeMsg(topic, {"args": [2], "kwargs": {}}),
    ]
    # One-shot commit failure: the first dispatch logs it, the second commits.
    consumer.commit_error = RuntimeError("commit boom")
    emitter._consumer = cast("Any", consumer)

    import concurrent.futures

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    emitter._consumer_pool = cast("Any", pool)
    try:
        real_poll = consumer.poll

        def stopping_poll(timeout: float) -> Any:
            if not consumer.poll_results and consumer.poll_error is None:
                emitter._stopping = True
            return real_poll(timeout)

        cast("Any", consumer).poll = stopping_poll
        await emitter._consumer_loop()
    finally:
        pool.shutdown(wait=True)

    assert received == [(1,), (2,)]
    assert consumer.commits == 1
    # Offsets commit synchronously (the emitter passes asynchronous=False).
    assert consumer.commit_async == [False]


async def test_exit_without_enter_is_safe() -> None:
    emitter = _emitter()
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None


async def test_exit_logs_consumer_task_failure() -> None:
    emitter = _emitter()

    async def _boom() -> None:
        msg = "consumer boom"
        raise RuntimeError(msg)

    emitter._consumer_task = asyncio.create_task(_boom())
    await asyncio.sleep(0)  # let the task fail before __aexit__ awaits it
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None
