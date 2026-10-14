"""Mocked-transport unit tests for the ZeroMQ emitter (no sockets needed).

Stubs ``zmq.asyncio.Context`` with in-memory fakes covering ``__aenter__``
(bind/connect/subscribe/warmup), ``__aexit__`` teardown, the publisher loop
(success and failure), and the consumer loop (valid dispatch, unparseable
frames, unknown events, listener isolation, receive failure).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any, cast

import pytest
import zmq.asyncio
from litestar.events import EventListener, listener

import zmq


class _FakeSocket:
    def __init__(self, recv_script: list[Any] | None = None) -> None:
        self._recv_script = list(recv_script or [])
        self.bound: list[str] = []
        self.connected: list[str] = []
        self.subscriptions: list[tuple[int, bytes]] = []
        self.sent: list[list[bytes]] = []
        self.send_error: Exception | None = None

    def bind(self, address: str) -> None:
        self.bound.append(address)

    def connect(self, address: str) -> None:
        self.connected.append(address)

    def setsockopt(self, opt: int, value: bytes) -> None:
        self.subscriptions.append((opt, value))

    async def send_multipart(self, frames: list[bytes]) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(frames)

    async def recv_multipart(self) -> list[bytes]:
        if self._recv_script:
            result = self._recv_script.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        await asyncio.Event().wait()  # hang until the task is cancelled
        msg = "unreachable"  # pragma: no cover - cancellation always raises above
        raise AssertionError(msg)  # pragma: no cover - see above


class _FakeContext:
    instance: _FakeContext | None = None

    def __init__(self, sub_script: list[Any] | None = None) -> None:
        self.pub = _FakeSocket()
        self.sub = _FakeSocket(sub_script)
        self.sockets: list[_FakeSocket] = []
        self.socket_kinds: list[int] = []
        self.destroyed: list[int] = []
        _FakeContext.instance = self

    def socket(self, kind: int) -> _FakeSocket:
        sock = self.pub if not self.sockets else self.sub
        self.sockets.append(sock)
        self.socket_kinds.append(kind)
        return sock

    def destroy(self, linger: int = 0) -> None:
        self.destroyed.append(linger)


def _patch_zmq(monkeypatch: pytest.MonkeyPatch, ctx: _FakeContext) -> None:
    monkeypatch.setattr(zmq.asyncio, "Context", lambda: ctx)


def _emitter(*listeners: Any, **kwargs: Any):
    from litestar_events.contrib.zmq.emitter import ZeroMQEventEmitter

    return ZeroMQEventEmitter(listeners, subscribe_warmup=0, **kwargs)


def _frames(event_id: str, payload: Any) -> list[bytes]:
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return [event_id.encode(), body]


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


async def test_enter_binds_and_exit_destroys(monkeypatch: pytest.MonkeyPatch) -> None:
    @listener("user_registered")
    async def lstn(**_: Any) -> None:
        pass

    ctx = _FakeContext()
    _patch_zmq(monkeypatch, ctx)
    emitter = _emitter(lstn)

    async with emitter:
        assert ctx.pub.bound == ["tcp://127.0.0.1:5557"]
        assert ctx.sub.connected == ["tcp://127.0.0.1:5557"]
        assert ctx.sub.subscriptions == [(zmq.SUBSCRIBE, b"user_registered")]
        assert ctx.socket_kinds == [zmq.PUB, zmq.SUB]
        assert emitter._consumer_task is not None
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.sleep(0.1)

    assert ctx.destroyed == [0]
    assert emitter._publish_queue is None
    (frames,) = ctx.pub.sent
    assert frames[0] == b"user_registered"
    assert json.loads(frames[1]) == {"args": [], "kwargs": {"email": "ada@example.com"}}


async def test_enter_without_listeners_skips_subscriber(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _FakeContext()
    _patch_zmq(monkeypatch, ctx)
    emitter = _emitter()

    async with emitter:
        assert ctx.pub.bound == ["tcp://127.0.0.1:5557"]
        assert len(ctx.sockets) == 1  # PUB only, no SUB
        assert ctx.socket_kinds == [zmq.PUB]
        assert emitter._consumer_task is None

    assert ctx.destroyed == [0]


async def test_publisher_loop_logs_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _FakeContext()
    ctx.pub.send_error = RuntimeError("boom")
    _patch_zmq(monkeypatch, ctx)
    emitter = _emitter()

    async with emitter:
        emitter.emit("evt", 1)
        await asyncio.sleep(0.1)

    assert ctx.pub.sent == []


async def test_consumer_loop_full_branch_coverage() -> None:
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    from litestar_events.contrib.zmq.emitter import ZeroMQEventEmitter

    emitter = ZeroMQEventEmitter([_raw_listener("evt"), good], subscribe_warmup=0)
    emitter._sub = cast(
        "Any",
        _FakeSocket([
            _frames("evt", {"args": [1], "kwargs": {}}),
            RuntimeError("recv boom"),  # receive failure: logged, loop continues
            [b"evt"],  # malformed: missing body frame -> dropped
            _frames("evt", b"not-json{{{"),
            _frames("unknown", {"args": [2], "kwargs": {}}),
        ]),
    )

    task = asyncio.create_task(emitter._consumer_loop())
    await asyncio.sleep(0.1)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert received == [(1,)]


async def test_exit_without_enter_is_safe() -> None:
    emitter = _emitter()
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None
