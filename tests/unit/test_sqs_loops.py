"""Mocked-transport unit tests for the SQS emitter (no LocalStack needed).

Stubs the ``aiobotocore`` session/clients with in-memory fakes covering queue
URL resolution (existing queue, created queue, missing without create),
``__aenter__``/``__aexit__`` lifecycle, the publisher loop (success and
failure), the consumer loop (dispatch, receive failure backoff, empty polls),
the message handler (valid, unparseable, unknown event, listener isolation),
and delete failure.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Self, cast

import aiobotocore.session
import pytest
from dirty_equals import IsStr
from litestar.events import EventListener, listener


class _QueueDoesNotExistError(Exception):
    pass


class _FakeClient:
    """Stand-in for an aiobotocore SQS client (also its own async CM)."""

    def __init__(self, existing_queues: set[str] | None = None) -> None:
        self.queues = dict.fromkeys(existing_queues or set(), "")
        self.created: list[str] = []
        self.sent: list[dict[str, Any]] = []
        self.send_error: Exception | None = None
        self.receives: list[dict[str, Any] | Exception] = []
        self.deleted: list[str] = []
        self.delete_error: Exception | None = None
        self.exceptions = type(
            "exceptions", (), {"QueueDoesNotExist": _QueueDoesNotExistError}
        )()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def get_queue_url(self, **kwargs: Any) -> dict[str, str]:
        queue_name: str = kwargs["QueueName"]
        if queue_name not in self.queues:
            raise _QueueDoesNotExistError(queue_name)
        return {"QueueUrl": f"https://sqs.test/{queue_name}"}

    async def create_queue(self, **kwargs: Any) -> dict[str, str]:
        queue_name: str = kwargs["QueueName"]
        self.created.append(queue_name)
        url = f"https://sqs.test/{queue_name}"
        self.queues[queue_name] = url
        return {"QueueUrl": url}

    async def send_message(self, **kwargs: Any) -> dict[str, str]:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(kwargs)
        return {"MessageId": "mid"}

    async def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        # Yield control every poll: the real client awaits network I/O, and
        # without this the consumer's tight empty-poll loop would starve the
        # event loop (the test's own sleep/cancel would never fire).
        await asyncio.sleep(0)
        if not self.receives:
            return {}
        result = self.receives.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def delete_message(self, **kwargs: Any) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        receipt: str = kwargs["ReceiptHandle"]
        self.deleted.append(receipt)


class _FakeSession:
    def __init__(self, pub: _FakeClient, sub: _FakeClient) -> None:
        self._clients = [pub, sub]

    def create_client(self, *args: Any, **kwargs: Any) -> _FakeClient:
        return self._clients.pop(0)


def _patch_session(
    monkeypatch: pytest.MonkeyPatch, pub: _FakeClient, sub: _FakeClient
) -> None:
    monkeypatch.setattr(
        aiobotocore.session, "get_session", lambda: _FakeSession(pub, sub)
    )


def _emitter(*listeners: Any, **kwargs: Any):
    from litestar_events.contrib.sqs.emitter import SQSEventEmitter

    return SQSEventEmitter(listeners, **kwargs)


def _message(event_id: str, payload: Any, receipt: str = "rh") -> dict[str, Any]:
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return {
        "ReceiptHandle": receipt,
        "Body": body,
        "MessageAttributes": {"event_id": {"StringValue": event_id}},
    }


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


async def test_enter_resolves_and_creates_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @listener("user_registered")
    async def lstn(**_: Any) -> None:
        pass

    pub, sub = _FakeClient(), _FakeClient()
    _patch_session(monkeypatch, pub, sub)
    emitter = _emitter(lstn, queue_name="events")

    async with emitter:
        assert emitter._queue_url == "https://sqs.test/events"
        assert pub.created == ["events"]
        assert emitter._consumer_task is not None
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.sleep(0.1)

    assert emitter._publish_queue is None
    (sent,) = pub.sent
    assert sent["QueueUrl"] == "https://sqs.test/events"
    assert json.loads(sent["MessageBody"]) == {
        "v": 1,
        "cid": IsStr(),
        "args": [],
        "kwargs": {"email": "ada@example.com"},
    }
    assert sent["MessageAttributes"] == {
        "event_id": {"DataType": "String", "StringValue": "user_registered"},
        "content_type": {"DataType": "String", "StringValue": "application/json"},
        "correlation_id": {"DataType": "String", "StringValue": IsStr()},
    }


async def test_enter_uses_existing_queue_url(monkeypatch: pytest.MonkeyPatch) -> None:
    pub, sub = _FakeClient({"events"}), _FakeClient()
    _patch_session(monkeypatch, pub, sub)
    emitter = _emitter(queue_name="events")

    async with emitter:
        assert emitter._consumer_task is None  # no listeners -> no consumer
        assert pub.created == []

    assert emitter._queue_url == "https://sqs.test/events"


async def test_enter_missing_queue_without_create_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_session(monkeypatch, _FakeClient(), _FakeClient())
    emitter = _emitter(queue_name="missing", create_queue=False)
    with pytest.raises(_QueueDoesNotExistError):
        async with emitter:
            pass  # pragma: no cover - enter raises before the body


async def test_publisher_loop_logs_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    pub, sub = _FakeClient({"events"}), _FakeClient()
    pub.send_error = RuntimeError("boom")
    _patch_session(monkeypatch, pub, sub)
    emitter = _emitter(queue_name="events")

    async with emitter:
        emitter.emit("evt", 1)
        await asyncio.sleep(0.1)

    assert pub.sent == []


async def test_consumer_loop_dispatches_and_backs_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    pub, sub = _FakeClient({"events"}), _FakeClient({"events"})
    sub.receives = [
        RuntimeError("receive boom"),  # backed off, then continued
        {"Messages": [_message("evt", {"args": [1], "kwargs": {}})]},
        {},  # empty poll
    ]
    _patch_session(monkeypatch, pub, sub)
    emitter = _emitter(
        _raw_listener("evt"), good, queue_name="events", wait_time_seconds=0
    )

    async with emitter:
        await asyncio.sleep(1.5)  # covers the 1s receive-failure backoff

    assert received == [(1,)]
    assert sub.deleted == ["rh"]


async def test_handle_message_branches() -> None:
    from litestar_events.contrib.sqs.emitter import SQSEventEmitter

    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    emitter = SQSEventEmitter(
        [_raw_listener("evt"), good], queue_url="https://sqs.test/events"
    )
    sub = _FakeClient({"events"})
    emitter._sub_client = cast("Any", sub)

    await emitter._handle_message(_message("evt", {"args": [1], "kwargs": {}}))
    await emitter._handle_message(_message("evt", "not-json{{{", receipt="rh-bad"))
    await emitter._handle_message(_message("unknown", {"args": [2], "kwargs": {}}))

    assert received == [(1,)]
    assert sub.deleted == ["rh", "rh-bad", "rh"]


async def test_delete_failure_is_logged() -> None:
    from litestar_events.contrib.sqs.emitter import SQSEventEmitter

    @listener("evt")
    async def good(**_: Any) -> None:
        pass

    emitter = SQSEventEmitter([good], queue_url="https://sqs.test/events")
    sub = _FakeClient({"events"})
    sub.delete_error = RuntimeError("boom")
    emitter._sub_client = cast("Any", sub)

    # Delete raises; the handler logs instead of propagating.
    await emitter._handle_message(_message("evt", {"args": [], "kwargs": {}}))
    assert sub.deleted == []


async def test_enter_with_preset_queue_url_skips_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pub, sub = _FakeClient(), _FakeClient()
    _patch_session(monkeypatch, pub, sub)
    emitter = _emitter(queue_url="https://sqs.test/preset")

    async with emitter:
        assert emitter._queue_url == "https://sqs.test/preset"
        assert pub.created == []

    assert emitter._publish_queue is None


async def test_exit_without_enter_is_safe() -> None:
    emitter = _emitter(queue_url="https://sqs.test/preset")
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None
