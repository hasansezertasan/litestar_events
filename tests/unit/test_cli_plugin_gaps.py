"""Edge paths of the ``events`` CLI, plugin, middleware and memory emitter."""

from __future__ import annotations

import asyncio
import os
import signal
from pathlib import Path
from typing import Any, cast

import click
import pytest
from click.testing import CliRunner
from litestar import Litestar
from litestar.events import BaseEventEmitterBackend, listener
from litestar.exceptions import ImproperlyConfiguredException
from litestar.middleware import DefineMiddleware
from typing_extensions import Self

from litestar_events import _cli
from litestar_events.config import EventsConfig
from litestar_events.contrib.memory import MemoryEventEmitter
from litestar_events.middleware import RequestIdMiddleware
from litestar_events.plugin import EventsPlugin


class _Shared(MemoryEventEmitter):
    process_local = False


class _NoRoles(BaseEventEmitterBackend):
    def __init__(self, listeners: Any, **_: Any) -> None:
        super().__init__(listeners)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def emit(self, event_id: str, *args: Any, **kwargs: Any) -> None:
        return None


class _Unreachable(_Shared):
    async def __aenter__(self) -> Self:
        msg = "connection refused"
        raise ConnectionError(msg)


def _app(backend: type[BaseEventEmitterBackend], **config: Any) -> Litestar:
    return Litestar(plugins=[EventsPlugin(EventsConfig(backend=backend, **config))])


def _invoke(app: Litestar, *args: str):
    from litestar.cli._utils import LitestarEnv

    env = LitestarEnv(app_path="tests:app", app=app, cwd=Path.cwd())
    return CliRunner().invoke(_cli.events_group, [*args], obj=env)


async def test_activity_tracks_in_flight_and_idle() -> None:
    activity = _cli._Activity()
    seen: list[int] = []

    async def fn(n: int) -> int:
        seen.append(activity.in_flight)
        assert activity.idle_for() < 1e-9
        return n

    assert await activity.wrap(fn)(3) == 3
    assert seen == [1]
    assert activity.in_flight == 0
    assert activity.idle_for() >= 0.0


def test_run_warns_when_not_durable(monkeypatch: pytest.MonkeyPatch) -> None:
    async def serve(_: Litestar) -> None:
        return None

    monkeypatch.setattr(_cli, "_serve", serve)
    result = _invoke(_app(_Shared), "run")
    assert result.exit_code == 0, result.output
    assert "not durable" in result.output


def test_run_suppresses_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    async def serve(_: Litestar) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(_cli, "_serve", serve)
    assert _invoke(_app(_Shared), "run").exit_code == 0


async def test_serve_waits_for_signal_then_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_wait() -> None:
        return None

    monkeypatch.setattr(_cli, "_wait_for_signal", fake_wait)
    await _cli._serve(_app(_Shared))


async def test_wait_for_signal_returns_on_sigterm() -> None:
    asyncio.get_running_loop().call_later(0.05, os.kill, os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(_cli._wait_for_signal(), timeout=5)


def test_set_role_rejects_backend_without_roles() -> None:
    with pytest.raises(click.UsageError, match="does not support roles"):
        _cli._set_role(_NoRoles([]), _cli.EventsRole.WORKER)


def test_status_reachable_and_unreachable() -> None:
    ok = _invoke(_app(_Shared), "status")
    assert ok.exit_code == 0, ok.output
    assert "reachable" in ok.output

    bad = _invoke(_app(_Unreachable), "status")
    assert bad.exit_code == 1
    assert "connection refused" in bad.output


def test_default_timeout_without_events_plugin() -> None:
    assert _cli._default_timeout(Litestar()) == EventsConfig.drain_timeout


def test_info_lists_events() -> None:
    @listener("ping")
    async def on_ping() -> None: ...

    app = Litestar(
        listeners=[on_ping], plugins=[EventsPlugin(EventsConfig(backend=_Shared))]
    )
    result = _invoke(app, "info")
    assert "ping" in result.output


def test_plugin_adds_request_id_middleware() -> None:
    app = Litestar(plugins=[EventsPlugin(EventsConfig(request_id_header="x-trace"))])
    first = app.middleware[0]
    assert isinstance(first, DefineMiddleware)
    assert first.middleware is RequestIdMiddleware


def test_plugin_rejects_local_listeners_on_unsupported_backend() -> None:
    @listener("ping")
    async def on_ping() -> None: ...

    with pytest.raises(ImproperlyConfiguredException, match="local_listeners"):
        Litestar(
            listeners=[on_ping],
            plugins=[
                EventsPlugin(EventsConfig(backend=_NoRoles, local_listeners=[on_ping]))
            ],
        )


def test_plugin_registers_cli_group() -> None:
    group = click.Group()
    EventsPlugin().on_cli_init(group)
    assert "events" in group.commands


async def _run_middleware(
    scope: dict[str, Any], app_sends: list[dict[str, Any]], **kw: Any
):
    async def inner(scope: Any, receive: Any, send: Any) -> None:
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"x-request-id", b"mine")],
        })

    sent: list[dict[str, Any]] = app_sends

    async def send(message: Any) -> None:
        sent.append(message)

    async def receive() -> dict[str, Any]:
        return {}

    await RequestIdMiddleware(inner, **kw)(
        cast("Any", scope), cast("Any", receive), send
    )


async def test_middleware_passes_through_non_http_scope() -> None:
    sent: list[dict[str, Any]] = []
    await _run_middleware({"type": "lifespan"}, sent)
    assert sent[0]["type"] == "http.response.start"


async def test_middleware_keeps_existing_response_header_and_skips_echo_off() -> None:
    for kw in ({}, {"echo": False}):
        sent: list[dict[str, Any]] = []
        await _run_middleware({"type": "http", "headers": []}, sent, **kw)
        headers = [h for h in sent[0]["headers"] if h[0] == b"x-request-id"]
        assert headers == [(b"x-request-id", b"mine")]


async def test_memory_emitter_drops_garbage_and_survives_failing_listener(
    caplog: pytest.LogCaptureFixture,
) -> None:
    ran = asyncio.Event()

    @listener("ping")
    async def boom() -> None:
        ran.set()
        msg = "nope"
        raise RuntimeError(msg)

    boom.fn = _raising(ran)  # bypass Litestar's error-swallowing wrapper
    emitter = MemoryEventEmitter([boom])
    async with emitter:
        queue = emitter._publish_queue
        assert queue is not None
        queue.put_nowait(("ping", b"\xff not json"))
        emitter.emit("ping")
        await asyncio.wait_for(ran.wait(), timeout=5)
        await asyncio.sleep(0.05)
    assert "Dropping unparseable event ping" in caplog.text
    assert "Listener" in caplog.text


def _raising(ran: asyncio.Event):
    async def fn() -> None:
        ran.set()
        msg = "nope"
        raise RuntimeError(msg)

    return fn


def test_drain_uses_configured_timeout_by_default() -> None:
    result = _invoke(_app(_Shared, drain_timeout=0.2), "drain")
    assert result.exit_code == 0, result.output
    assert "0.2s" in result.output


def _producer(listener_: Any) -> MemoryEventEmitter:
    emitter = MemoryEventEmitter([listener_], role="producer")
    emitter.set_local_listeners([listener_])
    return emitter


async def test_producer_ignores_events_without_local_listeners() -> None:
    @listener("ping")
    async def on_ping() -> None: ...

    emitter = _producer(on_ping)
    async with emitter:
        emitter.emit("unrelated")
    assert not emitter._local_tasks


async def test_undecodable_local_event_is_dropped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    @listener("ping")
    async def on_ping() -> None: ...

    await _producer(on_ping)._run_local("ping", b"\xff not json", [on_ping])
    assert "Dropping undecodable local event ping" in caplog.text


async def test_closing_cancels_pending_local_tasks() -> None:
    @listener("ping")
    async def on_ping() -> None: ...

    emitter = _producer(on_ping)
    task = asyncio.get_running_loop().create_task(asyncio.sleep(60))
    emitter._local_tasks.add(task)
    emitter._close_queue()
    await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()
