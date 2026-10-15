"""Mocked-transport unit tests for the NATS emitter (no broker needed).

Stubs ``nats.connect`` with an in-memory fake covering ``__aenter__``
validation and subscription, ``__aexit__`` teardown (including unsubscribe
failure), the publisher loop (success and failure), and the per-event
callback (valid dispatch, unparseable payload, listener isolation).
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, cast

import pytest
from dirty_equals import IsStr
from litestar.events import EventListener, listener

import nats


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


class _FakeSubscription:
    def __init__(self, subject: str) -> None:
        self.subject = subject
        self.unsubscribe_error: Exception | None = None

    async def unsubscribe(self) -> None:
        if self.unsubscribe_error is not None:
            raise self.unsubscribe_error


class _FakeClient:
    def __init__(self) -> None:
        self.subscriptions: list[tuple[str, Any]] = []
        self.created_subs: list[_FakeSubscription] = []
        self.published: list[tuple[str, bytes]] = []
        self.headers: list[Any] = []
        self.publish_error: Exception | None = None
        self.drained = False
        self.closed = False

    async def subscribe(self, subject: str, cb: Any) -> _FakeSubscription:
        self.subscriptions.append((subject, cb))
        sub = _FakeSubscription(subject)
        self.created_subs.append(sub)
        return sub

    async def publish(self, subject: str, body: bytes, headers: Any = None) -> None:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((subject, body))
        self.headers.append(headers)

    async def drain(self) -> None:
        self.drained = True

    async def close(self) -> None:
        self.closed = True


def _patch_nats(monkeypatch: pytest.MonkeyPatch, client: _FakeClient) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    async def fake_connect(*, servers: Any) -> _FakeClient:
        captured["servers"] = servers
        return client

    monkeypatch.setattr(nats, "connect", fake_connect)
    return captured


def _emitter(*listeners: Any, **kwargs: Any):
    from litestar_events.contrib.nats.emitter import NATSEventEmitter

    return NATSEventEmitter(listeners, **kwargs)


async def test_enter_subscribes_and_exit_drains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("user_registered")
    async def lstn(**_: Any) -> None:
        pass

    client = _FakeClient()
    captured = _patch_nats(monkeypatch, client)
    emitter = _emitter(lstn)

    async with emitter:
        assert captured["servers"] == "nats://localhost:4222"
        assert [s for s, _ in client.subscriptions] == [
            "litestar.events.user_registered"
        ]
        assert len(emitter._subscriptions) == 1
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.sleep(0.1)

    assert client.drained is True
    assert client.closed is True
    assert emitter._publish_queue is None
    assert client.headers == [{"Content-Type": "application/json"}]
    subject, body = client.published[0]
    assert subject == "litestar.events.user_registered"
    assert json.loads(body) == {
        "v": 1,
        "cid": IsStr(),
        "args": [],
        "kwargs": {"email": "ada@example.com"},
    }


async def test_enter_rejects_invalid_subject(monkeypatch: pytest.MonkeyPatch) -> None:
    @listener("bad subject")
    async def lstn(**_: Any) -> None:
        pass

    _patch_nats(monkeypatch, _FakeClient())
    emitter = _emitter(lstn)
    with pytest.raises(ValueError, match="invalid NATS subject"):
        async with emitter:
            pass  # pragma: no cover - enter raises before the body


async def test_exit_logs_unsubscribe_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    @listener("evt")
    async def lstn(**_: Any) -> None:
        pass

    client = _FakeClient()
    _patch_nats(monkeypatch, client)
    emitter = _emitter(lstn)

    async with emitter:
        for sub in client.created_subs:
            sub.unsubscribe_error = RuntimeError("boom")

    # Unsubscribe raised; __aexit__ logged it and still drained/closed.
    assert client.drained is True
    assert client.closed is True


async def test_publisher_loop_logs_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeClient()
    client.publish_error = RuntimeError("boom")
    _patch_nats(monkeypatch, client)
    emitter = _emitter()

    async with emitter:
        emitter.emit("evt", 1)
        await asyncio.sleep(0.1)

    assert client.published == []


async def test_callback_dispatch_and_isolation() -> None:
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    emitter = _emitter(_raw_listener("evt"), good)
    callback = emitter._make_callback("evt")

    await callback(
        cast(
            "Any",
            SimpleNamespace(
                subject="litestar.events.evt",
                data=json.dumps({"args": [1], "kwargs": {}}).encode(),
            ),
        )
    )
    await callback(
        cast("Any", SimpleNamespace(subject="litestar.events.evt", data=b"not-json{{{"))
    )

    assert received == [(1,)]


async def test_exit_without_enter_is_safe() -> None:
    emitter = _emitter()
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None
