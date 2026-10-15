"""Lifecycle, threading and configuration behavior found in the idiom review."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from litestar import Litestar, Request, get
from litestar.config.app import AppConfig
from litestar.events import listener
from litestar.exceptions import ImproperlyConfiguredException
from litestar.testing import AsyncTestClient
from typing_extensions import Self

from litestar_events import _cli
from litestar_events._queue import QueuedEmitterMixin, RoleAwareEmitter
from litestar_events.config import EventsConfig, EventsRole
from litestar_events.contrib.memory import MemoryEventEmitter
from litestar_events.middleware import RequestIdMiddleware
from litestar_events.plugin import EventsPlugin


async def test_sync_handler_can_emit() -> None:
    got = asyncio.Event()
    seen: list[int] = []

    @listener("ping")
    async def on_ping(n: int) -> None:
        seen.append(n)
        got.set()

    @get("/ping", sync_to_thread=True)
    def ping(request: Request[Any, Any, Any]) -> str:
        request.app.emit("ping", 7)  # runs in a worker thread
        return "ok"

    app = Litestar(
        route_handlers=[ping],
        listeners=[on_ping],
        plugins=[EventsPlugin(EventsConfig(backend=MemoryEventEmitter))],
    )
    async with AsyncTestClient(app) as client:
        assert (await client.get("/ping")).status_code == 200
        await asyncio.wait_for(got.wait(), timeout=5)
    assert seen == [7]


async def test_lifespan_emits_and_shuts_down_cleanly() -> None:
    got = asyncio.Event()

    @listener("ping")
    async def on_ping() -> None:
        got.set()

    app = Litestar(
        listeners=[on_ping],
        plugins=[EventsPlugin(EventsConfig(backend=MemoryEventEmitter))],
    )
    async with AsyncTestClient(app) as client:
        client.app.emit("ping")
        await asyncio.wait_for(got.wait(), timeout=5)
    with pytest.raises(RuntimeError, match="outside its async context"):
        app.emit("ping")


async def test_producer_shutdown_waits_for_local_listeners() -> None:
    finished: list[bool] = []

    @listener("ping")
    async def on_ping() -> None:
        await asyncio.sleep(0.1)
        finished.append(True)

    app = Litestar(
        listeners=[on_ping],
        plugins=[
            EventsPlugin(
                EventsConfig(
                    backend=MemoryEventEmitter,
                    role="producer",
                    local_listeners=[on_ping],
                )
            )
        ],
    )
    async with AsyncTestClient(app) as client:
        client.app.emit("ping")
    assert finished == [True]


async def test_failed_enter_runs_teardown() -> None:
    torn_down: list[bool] = []

    class _Fails(MemoryEventEmitter):
        async def __aenter__(self) -> Self:
            await super().__aenter__()
            msg = "boom"
            raise ConnectionError(msg)

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            torn_down.append(True)
            await super().__aexit__(exc_type, exc, tb)

    emitter = _Fails([])
    with pytest.raises(ConnectionError):
        async with emitter:
            pass
    assert torn_down == [True]
    assert emitter._publish_queue is None


async def test_failed_enter_tears_down_once_through_nested_overrides() -> None:
    torn_down: list[str] = []

    class _Base(MemoryEventEmitter):
        async def __aenter__(self) -> Self:
            await super().__aenter__()
            msg = "boom"
            raise ConnectionError(msg)

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            torn_down.append("exit")
            await super().__aexit__(exc_type, exc, tb)

    class _Sub(_Base):
        async def __aenter__(self) -> Self:
            return await super().__aenter__()

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            await super().__aexit__(exc_type, exc, tb)

    with pytest.raises(ConnectionError):
        async with _Sub([]):
            pass
    assert torn_down == ["exit"]


def test_default_timeout_finds_plugin_subclass() -> None:
    class _MyPlugin(EventsPlugin):
        pass

    app = Litestar(plugins=[_MyPlugin(EventsConfig(drain_timeout=99))])
    assert _cli._default_timeout(app) == 99


async def test_drain_tracks_a_backend_that_is_not_role_aware() -> None:
    from litestar.events import BaseEventEmitterBackend

    ran = asyncio.Event()

    @listener("ping")
    async def on_ping() -> None:
        ran.set()

    class _Plain(BaseEventEmitterBackend):
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *args: object) -> None: ...

        def emit(self, event_id: str, *args: Any, **kwargs: Any) -> None: ...

    emitter = _Plain([on_ping])
    activity = _cli._Activity()
    original = on_ping.fn
    with _cli._tracking(emitter, activity):
        assert on_ping.fn is not original
        await on_ping.fn()
        assert activity.in_flight == 0
    assert on_ping.fn is original
    assert ran.is_set()


def test_tracked_codec_delegates_other_attributes() -> None:
    from litestar_events.codec import JSONCodec

    tracked = _cli._Activity().track(JSONCodec(typed=False))
    assert getattr(tracked, "typed") is False  # noqa: B009


async def test_request_id_echo_without_headers_key() -> None:
    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    async def app(scope: Any, receive: Any, send: Any) -> None:
        await send({"type": "websocket.accept"})

    scope: Any = {"type": "websocket", "headers": [], "path": "/", "query_string": b""}
    handle: Any = RequestIdMiddleware().handle
    await handle(scope, None, send, app)
    assert sent[0]["headers"]


async def test_drain_leaves_listener_functions_untouched() -> None:
    @listener("ping")
    async def on_ping() -> None: ...

    original = on_ping.fn

    class _Shared(MemoryEventEmitter):
        process_local = False

    app = Litestar(
        listeners=[on_ping], plugins=[EventsPlugin(EventsConfig(backend=_Shared))]
    )
    emitter = app.event_emitter
    assert isinstance(emitter, RoleAwareEmitter)
    emitter.use_role(EventsRole.WORKER)
    await _cli._drain(app, 0.05)
    assert on_ping.fn is original


def test_memory_emitter_is_role_aware() -> None:
    emitter = MemoryEventEmitter([])
    assert isinstance(emitter, RoleAwareEmitter)
    assert isinstance(emitter, QueuedEmitterMixin)


def test_config_validates_early() -> None:
    with pytest.raises(ValueError, match="Invalid events role"):
        EventsConfig(role="bogus")
    with pytest.raises(ValueError, match="drain_timeout"):
        EventsConfig(drain_timeout=0)
    with pytest.raises(ValueError, match="request_id_header"):
        EventsConfig(request_id_header=" ")
    assert EventsConfig(role="worker").role is EventsRole.WORKER


def test_backend_options_cannot_shadow_config() -> None:
    config = EventsConfig(backend=MemoryEventEmitter, backend_options={"role": "x"})
    with pytest.raises(ImproperlyConfiguredException, match="role"):
        Litestar(plugins=[EventsPlugin(config)])


def test_unwrap_finds_user_function_behind_litestar_listener() -> None:
    from litestar_events.codec import _unwrap

    async def handler(n: int) -> None: ...

    wrapped = listener("e")(handler)
    assert _unwrap(wrapped.fn) is handler


def test_installed_backend_is_a_class_of_the_configured_backend() -> None:
    config = AppConfig()
    EventsPlugin(EventsConfig(backend=MemoryEventEmitter)).on_app_init(config)
    installed = config.event_emitter_backend
    assert isinstance(installed, type)
    assert issubclass(installed, MemoryEventEmitter)
    assert installed.__name__ == "MemoryEventEmitter"


async def test_request_id_echoed_on_websocket_accept() -> None:
    sent: list[dict[str, Any]] = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        await send({"type": "websocket.accept", "headers": []})

    async def receive() -> Any:
        return {"type": "websocket.connect"}

    async def send(message: Any) -> None:
        sent.append(message)

    middleware = RequestIdMiddleware(header="x-request-id")
    scope: Any = {"type": "websocket", "headers": [(b"x-request-id", b"abc-1")]}
    await middleware.handle(scope, receive, send, app)
    assert (b"x-request-id", b"abc-1") in sent[0]["headers"]


def test_installed_backend_does_not_rewrap_lifecycle_methods() -> None:
    config = AppConfig()
    EventsPlugin(EventsConfig(backend=MemoryEventEmitter)).on_app_init(config)
    installed = config.event_emitter_backend
    assert "__aenter__" not in installed.__dict__
    assert "__aexit__" not in installed.__dict__
