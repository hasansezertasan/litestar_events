"""Mocked-transport unit tests for the MQTT emitter (no broker needed).

Stubs ``aiomqtt.Client`` with an in-memory fake covering ``__aenter__``
validation and subscription, ``__aexit__`` teardown, the publisher loop
(success and failure), and the consumer loop (valid dispatch, unparseable
payloads, unknown events, listener isolation, bytes/bytearray/str payloads).
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, cast

import aiomqtt
import pytest
from dirty_equals import IsBytes, IsStr
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


class _FakeClient:
    """Stand-in for ``aiomqtt.Client`` with a scriptable message stream."""

    def __init__(self, messages: list[Any] | None = None) -> None:
        self._messages = list(messages or [])
        self.subscribed: list[tuple[str, int]] = []
        self.published: list[tuple[str, str, int]] = []
        self.properties: list[Any] = []
        self.publish_error: Exception | None = None

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        self.subscribed.append((topic, qos))

    async def publish(
        self, topic: str, payload: str, qos: int = 0, properties: Any = None
    ) -> None:
        self.properties.append(properties)
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((topic, payload, qos))

    @property
    def messages(self):
        messages = self._messages

        async def _gen():
            for message in messages:
                yield message

        return _gen()


class _FakeClientCM:
    """Stand-in for the ``aiomqtt.Client(...)`` constructor (an async CM)."""

    client: _FakeClient | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.kwargs = kwargs

    async def __aenter__(self) -> _FakeClient:
        assert _FakeClientCM.client is not None
        return _FakeClientCM.client

    async def __aexit__(self, *exc: object) -> None:
        return None


def _patch_mqtt(monkeypatch: pytest.MonkeyPatch, client: _FakeClient) -> None:
    _FakeClientCM.client = client
    monkeypatch.setattr(aiomqtt, "Client", _FakeClientCM)


def _emitter(*listeners: Any, **kwargs: Any):
    from litestar_events.contrib.mqtt.emitter import MQTTEventEmitter

    return MQTTEventEmitter(listeners, **kwargs)


def _message(topic: str, payload: Any) -> Any:
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return SimpleNamespace(topic=topic, payload=body)


async def test_enter_subscribes_and_exit_tears_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("user_registered")
    async def lstn(**_: Any) -> None:
        pass

    client = _FakeClient()
    _patch_mqtt(monkeypatch, client)
    emitter = _emitter(lstn, qos=1)

    async with emitter:
        assert client.subscribed == [("litestar/events/user_registered", 1)]
        assert emitter._consumer_task is not None
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.sleep(0.1)

    assert emitter._publish_queue is None
    assert client.published == [
        (
            "litestar/events/user_registered",
            IsBytes(
                regex=rb'^\{"v":1,"cid":"[0-9a-f]{32}","args":\[\],"kwargs":\{"email":"ada@example.com"\}\}$'
            ),
            1,
        )
    ]


async def test_v5_publishes_content_type_and_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from litestar_events.context import event_headers

    client = _FakeClient()
    _patch_mqtt(monkeypatch, client)
    emitter = _emitter(protocol=aiomqtt.ProtocolVersion.V5)

    async with emitter:
        with event_headers({"tenant": "acme", "Content-Type": "text/plain"}):
            emitter.emit("ping")
        await asyncio.sleep(0.1)

    (properties,) = client.properties
    assert properties.ContentType == "application/json"
    assert properties.UserProperty == [("tenant", "acme"), ("correlation-id", IsStr())]


async def test_below_v5_publishes_no_properties(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient()
    _patch_mqtt(monkeypatch, client)
    emitter = _emitter()

    async with emitter:
        emitter.emit("ping")
        await asyncio.sleep(0.1)

    assert client.properties == [None]


async def test_enter_rejects_invalid_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    @listener("bad+id")
    async def lstn(**_: Any) -> None:
        pass

    _patch_mqtt(monkeypatch, _FakeClient())
    emitter = _emitter(lstn)
    with pytest.raises(ValueError, match="invalid MQTT topic"):
        async with emitter:
            pass  # pragma: no cover - enter raises before the body


async def test_publisher_loop_logs_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeClient()
    client.publish_error = RuntimeError("boom")
    _patch_mqtt(monkeypatch, client)
    emitter = _emitter()

    async with emitter:
        emitter.emit("user_registered", 1)
        await asyncio.sleep(0.1)

    assert client.published == []


async def test_consumer_loop_full_branch_coverage() -> None:
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    emitter = _emitter(_raw_listener("evt"), good)
    prefix = "litestar/events/"
    emitter._client = cast(
        "Any",
        _FakeClient([
            _message(f"{prefix}evt", {"args": [1], "kwargs": {}}),
            SimpleNamespace(  # bytes payload
                topic=f"{prefix}evt",
                payload=json.dumps({"args": [2], "kwargs": {}}).encode(),
            ),
            SimpleNamespace(  # bytearray payload
                topic=f"{prefix}evt",
                payload=bytearray(json.dumps({"args": [3], "kwargs": {}}).encode()),
            ),
            _message(f"{prefix}evt", "not-json{{{"),
            _message(f"{prefix}unknown", {"args": [4], "kwargs": {}}),
        ]),
    )

    await emitter._consumer_loop()

    assert received == [(1,), (2,), (3,)]


async def test_exit_without_enter_is_safe() -> None:
    emitter = _emitter()
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None
