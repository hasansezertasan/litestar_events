"""Mocked-transport unit tests for the GCP Pub/Sub emitter (no emulator needed).

Stubs the ``gcloud.aio.pubsub`` clients and ``subscribe`` helper with
in-memory fakes covering topic/subscription bootstrap (including the 409
idempotency path), ``__aenter__``/``__aexit__`` lifecycle (owned vs shared
subscriptions), the publisher loop, and the message handler (valid dispatch,
no-data drop, unparseable drop, unknown event, listener isolation).
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import gcloud.aio.pubsub as gcloud_pubsub
import pytest
from aiohttp import ClientResponseError
from litestar.events import EventListener, listener


def _conflict(status: int = 409) -> ClientResponseError:
    return ClientResponseError(MagicMock(), MagicMock(), status=status)


class _FakePublisher:
    def __init__(self) -> None:
        self.created_topics: list[str] = []
        self.published: list[tuple[str, list[Any]]] = []
        self.publish_error: Exception | None = None
        self.create_error: Exception | None = None
        self.closed = False

    async def create_topic(self, topic: str) -> None:
        if self.create_error is not None:
            raise self.create_error
        self.created_topics.append(topic)

    async def publish(self, topic: str, messages: list[Any]) -> None:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((topic, messages))

    async def close(self) -> None:
        self.closed = True


class _FakeSubscriber:
    def __init__(self) -> None:
        self.created_subscriptions: list[tuple[str, str]] = []
        self.deleted: list[str] = []
        self.create_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.closed = False

    async def create_subscription(self, subscription: str, topic: str) -> None:
        if self.create_error is not None:
            raise self.create_error
        self.created_subscriptions.append((subscription, topic))

    async def delete_subscription(self, subscription: str) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(subscription)

    async def close(self) -> None:
        self.closed = True


def _patch_pubsub(
    monkeypatch: pytest.MonkeyPatch,
    publisher: _FakePublisher,
    subscriber: _FakeSubscriber,
    to_handle: list[Any] | None = None,
) -> None:
    monkeypatch.setattr(
        gcloud_pubsub, "PublisherClient", lambda *args, **kwargs: publisher
    )
    monkeypatch.setattr(
        gcloud_pubsub, "SubscriberClient", lambda *args, **kwargs: subscriber
    )

    async def fake_subscribe(
        subscription: str, handler: Any, *args: Any, **kwargs: Any
    ) -> None:
        for message in to_handle or []:
            await handler(message)

    monkeypatch.setattr(gcloud_pubsub, "subscribe", fake_subscribe)


def _emitter(*listeners: Any, **kwargs: Any):
    from litestar_events.contrib.pubsub.emitter import PubSubEventEmitter

    return PubSubEventEmitter(listeners, project_id="test-project", **kwargs)


def _message(event_id: str, payload: Any) -> Any:
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return SimpleNamespace(data=body.encode(), attributes={"event_id": event_id})


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


async def test_enter_creates_resources_and_exit_deletes_owned_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("user_registered")
    async def lstn(**_: Any) -> None:
        pass

    publisher = _FakePublisher()
    subscriber = _FakeSubscriber()
    _patch_pubsub(monkeypatch, publisher, subscriber)
    emitter = _emitter(lstn)

    async with emitter:
        assert publisher.created_topics == [
            "projects/test-project/topics/litestar-events"
        ]
        assert len(subscriber.created_subscriptions) == 1
        subscription, topic = subscriber.created_subscriptions[0]
        assert subscription.startswith("projects/test-project/subscriptions/")
        assert topic == "projects/test-project/topics/litestar-events"
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.sleep(0.1)

    assert subscriber.deleted == [subscription]
    assert publisher.closed is True
    assert subscriber.closed is True
    assert emitter._publish_queue is None
    assert len(publisher.published) == 1
    topic, messages = publisher.published[0]
    assert topic == "projects/test-project/topics/litestar-events"
    (message,) = messages
    assert message.attributes == {"event_id": "user_registered"}


async def test_enter_tolerates_existing_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("evt")
    async def lstn(**_: Any) -> None:
        pass

    publisher = _FakePublisher()
    publisher.create_error = _conflict(409)
    subscriber = _FakeSubscriber()
    subscriber.create_error = _conflict(409)
    _patch_pubsub(monkeypatch, publisher, subscriber)
    emitter = _emitter(lstn)

    async with emitter:
        pass

    assert publisher.created_topics == []  # 409 swallowed, not re-raised


async def test_enter_propagates_unexpected_bootstrap_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("evt")
    async def lstn(**_: Any) -> None:
        pass

    publisher = _FakePublisher()
    publisher.create_error = _conflict(500)
    _patch_pubsub(monkeypatch, publisher, _FakeSubscriber())
    emitter = _emitter(lstn)
    with pytest.raises(ClientResponseError):
        async with emitter:
            pass  # pragma: no cover - enter raises before the body


async def test_enter_propagates_unexpected_subscription_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("evt")
    async def lstn(**_: Any) -> None:
        pass

    subscriber = _FakeSubscriber()
    subscriber.create_error = _conflict(500)
    _patch_pubsub(monkeypatch, _FakePublisher(), subscriber)
    emitter = _emitter(lstn)
    with pytest.raises(ClientResponseError):
        async with emitter:
            pass  # pragma: no cover - enter raises before the body


async def test_enter_without_create_resources_skips_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("evt")
    async def lstn(**_: Any) -> None:
        pass

    publisher = _FakePublisher()
    subscriber = _FakeSubscriber()
    _patch_pubsub(monkeypatch, publisher, subscriber)
    emitter = _emitter(lstn, create_resources=False)

    async with emitter:
        assert publisher.created_topics == []
        assert subscriber.created_subscriptions == []

    # Nothing was created, so there is nothing owned to delete.
    assert subscriber.deleted == []


async def test_shared_subscription_not_deleted_on_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("evt")
    async def lstn(**_: Any) -> None:
        pass

    publisher = _FakePublisher()
    subscriber = _FakeSubscriber()
    _patch_pubsub(monkeypatch, publisher, subscriber)
    emitter = _emitter(lstn, subscription_name="shared-sub")

    async with emitter:
        pass

    assert subscriber.created_subscriptions[0][0].endswith("/shared-sub")
    assert subscriber.deleted == []  # shared subscriptions are kept
    assert subscriber.closed is True


async def test_exit_logs_delete_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    @listener("evt")
    async def lstn(**_: Any) -> None:
        pass

    publisher = _FakePublisher()
    subscriber = _FakeSubscriber()
    subscriber.delete_error = RuntimeError("boom")
    _patch_pubsub(monkeypatch, publisher, subscriber)
    emitter = _emitter(lstn)

    async with emitter:
        pass

    assert subscriber.closed is True  # exit continued past the delete failure


async def test_publisher_loop_logs_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    publisher = _FakePublisher()
    publisher.publish_error = RuntimeError("boom")
    _patch_pubsub(monkeypatch, publisher, _FakeSubscriber())
    emitter = _emitter()

    async with emitter:
        emitter.emit("evt", 1)
        await asyncio.sleep(0.1)

    assert publisher.published == []


async def test_subscribe_drives_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    publisher = _FakePublisher()
    subscriber = _FakeSubscriber()
    _patch_pubsub(
        monkeypatch,
        publisher,
        subscriber,
        to_handle=[
            _message("evt", {"args": [1], "kwargs": {}}),
            SimpleNamespace(data=None, attributes={"event_id": "evt"}),
            SimpleNamespace(data=b"not-json{{{", attributes={"event_id": "evt"}),
            _message("unknown", {"args": [2], "kwargs": {}}),
        ],
    )
    # Bypass __aenter__ (which would call the stubbed subscribe with no
    # messages) and drive _run_subscribe directly against scripted messages.
    from litestar_events.contrib.pubsub.emitter import PubSubEventEmitter

    emitter = PubSubEventEmitter(
        [_raw_listener("evt"), good], project_id="test-project"
    )
    emitter._subscriber = cast("Any", subscriber)

    await emitter._run_subscribe()

    assert received == [(1,)]


async def test_exit_without_enter_is_safe() -> None:
    emitter = _emitter()
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None
