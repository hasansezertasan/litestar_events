"""Mocked-transport unit tests for the RabbitMQ emitter (no broker needed).

Stubs ``aio_pika.connect_robust`` with in-memory fakes covering
``__aenter__`` topology setup (exchange, DLX, queue, bindings, consume),
``__aexit__`` teardown, the publisher loop (success and failure), and the
message callback (valid dispatch, unparseable reject, unknown-event ack,
listener isolation).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import aio_pika
import pytest
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


class _FakeMessage:
    """Stand-in for ``aio_pika.abc.AbstractIncomingMessage``."""

    def __init__(self, body: bytes, routing_key: str) -> None:
        self.body = body
        self.routing_key = routing_key
        self.acked = False
        self.rejected: list[bool | None] = []

    async def ack(self) -> None:
        self.acked = True

    async def reject(self, *, requeue: bool | None = None) -> None:
        self.rejected.append(requeue)


class _FakeQueue:
    def __init__(self) -> None:
        self.bindings: list[tuple[Any, str]] = []
        self.callback: Any = None

    async def bind(self, exchange: Any, routing_key: str) -> None:
        self.bindings.append((exchange, routing_key))

    async def consume(self, callback: Any) -> None:
        self.callback = callback


class _FakeExchange:
    def __init__(self, name: str) -> None:
        self.name = name
        self.published: list[tuple[Any, str]] = []
        self.publish_error: Exception | None = None

    async def publish(self, message: Any, routing_key: str) -> None:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((message, routing_key))


class _FakeChannel:
    def __init__(self) -> None:
        self.exchanges: dict[str, _FakeExchange] = {}
        self.declared_kinds: list[Any] = []
        self.queue = _FakeQueue()
        self.declared_queues: list[tuple[str, bool, bool, dict[str, Any]]] = []
        self.qos: list[int] = []

    async def declare_exchange(
        self, name: str, kind: Any, *, durable: bool = True
    ) -> _FakeExchange:
        exchange = _FakeExchange(name)
        self.exchanges[name] = exchange
        self.declared_kinds.append(kind)
        return exchange

    async def declare_queue(
        self,
        name: str,
        *,
        durable: bool = False,
        exclusive: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> _FakeQueue:
        self.declared_queues.append((name, durable, exclusive, arguments or {}))
        return self.queue

    async def set_qos(self, prefetch_count: int = 0) -> None:
        self.qos.append(prefetch_count)


class _FakeConnection:
    def __init__(self) -> None:
        self.channels: list[_FakeChannel] = []
        self.channel_confirms: list[bool] = []
        self.closed = False

    async def channel(self, *, publisher_confirms: bool = False) -> _FakeChannel:
        channel = _FakeChannel()
        self.channels.append(channel)
        self.channel_confirms.append(publisher_confirms)
        return channel

    async def close(self) -> None:
        self.closed = True


def _patch_rabbit(monkeypatch: pytest.MonkeyPatch, connection: _FakeConnection) -> None:
    async def fake_connect_robust(url: str) -> _FakeConnection:
        return connection

    monkeypatch.setattr(aio_pika, "connect_robust", fake_connect_robust)


def _emitter(*listeners: Any, **kwargs: Any):
    from litestar_events.contrib.rabbit.emitter import RabbitEventEmitter

    return RabbitEventEmitter(listeners, **kwargs)


def _incoming(event_id: str, payload: Any) -> _FakeMessage:
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return _FakeMessage(body, event_id)


async def test_enter_declares_topology_and_exit_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("user_registered")
    async def lstn(**_: Any) -> None:
        pass

    connection = _FakeConnection()
    _patch_rabbit(monkeypatch, connection)
    emitter = _emitter(lstn)

    async with emitter:
        assert set(connection.channels[0].exchanges) == {
            "litestar.events",
            "litestar.events.dlx",
        }
        # Both exchanges are durable topic exchanges; the publishing channel
        # uses confirms while the subscription channel does not.
        assert connection.channels[0].declared_kinds == [
            aio_pika.ExchangeType.TOPIC,
            aio_pika.ExchangeType.TOPIC,
        ]
        assert connection.channel_confirms == [True, False]
        sub_channel = connection.channels[1]
        assert sub_channel.qos == [32]
        assert sub_channel.queue.bindings[0][1] == "user_registered"
        assert sub_channel.queue.callback is not None
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.sleep(0.1)

    assert connection.closed is True
    assert emitter._publish_queue is None
    exchange = connection.channels[0].exchanges["litestar.events"]
    (message, routing_key), *_ = exchange.published
    assert routing_key == "user_registered"
    assert json.loads(message.body) == {
        "args": [],
        "kwargs": {"email": "ada@example.com"},
    }


async def test_enter_exclusive_queue_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    _patch_rabbit(monkeypatch, connection)
    emitter = _emitter()

    async with emitter:
        ((name, durable, exclusive, args), *_) = connection.channels[1].declared_queues

    assert (name, durable, exclusive) == ("", False, True)
    assert args == {"x-dead-letter-exchange": "litestar.events.dlx"}


async def test_publisher_loop_logs_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _FakeConnection()
    _patch_rabbit(monkeypatch, connection)
    emitter = _emitter()

    async with emitter:
        connection.channels[0].exchanges[
            "litestar.events"
        ].publish_error = RuntimeError("boom")
        emitter.emit("evt", 1)
        await asyncio.sleep(0.1)

    assert connection.channels[0].exchanges["litestar.events"].published == []


async def test_on_message_full_branch_coverage() -> None:
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    emitter = _emitter(_raw_listener("evt"), good)

    valid = _incoming("evt", {"args": [1], "kwargs": {}})
    await emitter._on_message(valid)  # type: ignore[arg-type]
    assert valid.acked is True

    poison = _incoming("evt", b"not-json{{{")
    await emitter._on_message(poison)  # type: ignore[arg-type]
    assert poison.rejected == [False]
    assert poison.acked is False

    unknown = _incoming("unknown", {"args": [2], "kwargs": {}})
    await emitter._on_message(unknown)  # type: ignore[arg-type]
    assert unknown.acked is True

    assert received == [(1,)]


async def test_exit_without_enter_is_safe() -> None:
    emitter = _emitter()
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None
