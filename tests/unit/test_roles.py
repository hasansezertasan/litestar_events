"""Roles, the plugin and the ``events`` CLI group (no broker needed)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import cast

import pytest
from click.testing import CliRunner
from litestar import Litestar
from litestar.events import BaseEventEmitterBackend, EventListener, listener

from litestar_events.config import EventsConfig, EventsRole, resolve_role
from litestar_events.contrib.memory import MemoryEventEmitter
from litestar_events.contrib.rabbit import RabbitEventEmitter
from litestar_events.contrib.redis import RedisEventEmitter
from litestar_events.plugin import EventsPlugin


def test_resolve_role_default_and_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LITESTAR_EVENTS_ROLE", "worker")  # no longer read
    assert resolve_role() is EventsRole.EMBEDDED
    assert resolve_role("producer") is EventsRole.PRODUCER


def test_resolve_role_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="Invalid events role"):
        resolve_role("bogus")


def test_worker_cannot_emit() -> None:
    emitter = RabbitEventEmitter([], role="worker")
    with pytest.raises(RuntimeError, match="worker-role"):
        emitter.emit("x")


def test_durable_flags() -> None:
    assert RedisEventEmitter([]).durable is False
    assert RabbitEventEmitter([]).durable is False
    assert RabbitEventEmitter([], queue_name="q").durable is True
    assert MemoryEventEmitter([]).process_local is True


async def test_memory_emitter_dispatches() -> None:
    seen: list[int] = []

    @listener("ping")
    async def on_ping(n: int) -> None:
        seen.append(n)

    emitter = MemoryEventEmitter([on_ping])
    async with emitter:
        emitter.emit("ping", 1)
        await asyncio.sleep(0.05)
    assert seen == [1]


async def test_producer_role_skips_consumer() -> None:
    from unittest.mock import AsyncMock, MagicMock, patch

    @listener("ping")
    async def on_ping() -> None: ...

    emitter = RedisEventEmitter([on_ping], role="producer")
    client = MagicMock()
    client.aclose = AsyncMock()
    client.ping = AsyncMock()
    client.pubsub.return_value = MagicMock(subscribe=AsyncMock(), aclose=AsyncMock())
    with patch("redis.asyncio.Redis.from_url", return_value=client):
        async with emitter:
            assert emitter._consumer_task is None
            assert emitter._publisher_task is not None
        client.pubsub.return_value.subscribe.assert_not_called()


def _app(emitter: type[BaseEventEmitterBackend] = MemoryEventEmitter) -> Litestar:
    return Litestar(plugins=[EventsPlugin(EventsConfig(backend=emitter))])


def _invoke(app: Litestar, *args: str):
    from litestar.cli._utils import LitestarEnv

    from litestar_events._cli import events_group

    env = LitestarEnv(app_path="tests:app", app=app, cwd=Path.cwd())
    return CliRunner().invoke(events_group, [*args], obj=env)


def test_plugin_installs_backend() -> None:
    assert isinstance(_app().event_emitter, MemoryEventEmitter)


def test_info_command() -> None:
    result = _invoke(_app(), "info")
    assert result.exit_code == 0, result.output
    assert "MemoryEventEmitter" in result.output


def test_run_rejects_process_local() -> None:
    result = _invoke(_app(), "run")
    assert result.exit_code != 0
    assert "process-local" in result.output


def test_plugin_forwards_backend_options_and_role() -> None:
    config = EventsConfig(
        backend=RedisEventEmitter,
        backend_options={"channel_prefix": "x:"},
        role="producer",
    )
    emitter = Litestar(plugins=[EventsPlugin(config)]).event_emitter
    assert isinstance(emitter, RedisEventEmitter)
    assert emitter.role is EventsRole.PRODUCER
    assert emitter._channel_prefix == "x:"


def test_drain_consumes_then_exits() -> None:
    seen: list[int] = []

    @listener("ping")
    async def on_ping(n: int) -> None:
        seen.append(n)

    class _Shared(MemoryEventEmitter):
        process_local = False

    app = Litestar(
        listeners=[on_ping],
        plugins=[EventsPlugin(EventsConfig(backend=_Shared, drain_timeout=0.2))],
    )
    result = _invoke(app, "drain", "--timeout", "0.2")
    assert result.exit_code == 0, result.output
    assert "Draining" in result.output


def _two_listeners(seen: list[str]):
    @listener("ping")
    async def local_one(n: int) -> None:
        seen.append(f"local:{n}")

    @listener("ping")
    async def remote_one(n: int) -> None:
        seen.append(f"remote:{n}")

    return local_one, remote_one


async def test_producer_runs_only_local_listeners_directly() -> None:
    seen: list[str] = []
    local_one, remote_one = _two_listeners(seen)
    app = Litestar(
        listeners=[local_one, remote_one],
        plugins=[
            EventsPlugin(
                EventsConfig(
                    backend=MemoryEventEmitter,
                    role="producer",
                    local_listeners=[local_one],
                )
            )
        ],
    )
    async with app.event_emitter:
        app.emit("ping", 1)
        await asyncio.sleep(0.05)
    assert seen == ["local:1"]


def test_worker_skips_local_listeners_and_their_subscriptions() -> None:
    seen: list[str] = []
    local_one, remote_one = _two_listeners(seen)

    @listener("only_local")
    async def only_local() -> None: ...

    app = Litestar(
        listeners=[local_one, remote_one, only_local],
        plugins=[
            EventsPlugin(
                EventsConfig(
                    backend=RabbitEventEmitter,
                    role="worker",
                    local_listeners=[local_one, only_local],
                )
            )
        ],
    )
    by_event = _by_event(app)
    assert by_event == {"ping": [remote_one]}


def test_embedded_ignores_local_listeners() -> None:
    seen: list[str] = []
    local_one, remote_one = _two_listeners(seen)
    app = Litestar(
        listeners=[local_one, remote_one],
        plugins=[
            EventsPlugin(
                EventsConfig(backend=MemoryEventEmitter, local_listeners=[local_one])
            )
        ],
    )
    assert len(_by_event(app)["ping"]) == 2


def test_local_listener_must_be_registered() -> None:
    from litestar.exceptions import ImproperlyConfiguredException

    local_one, _ = _two_listeners([])
    with pytest.raises(ImproperlyConfiguredException, match="local_one"):
        Litestar(
            plugins=[
                EventsPlugin(
                    EventsConfig(
                        backend=MemoryEventEmitter, local_listeners=[local_one]
                    )
                )
            ]
        )


async def test_failing_local_listener_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    @listener("ping")
    async def boom() -> None:
        msg = "nope"
        raise RuntimeError(msg)

    boom.fn = _raising  # bypass Litestar's own error-swallowing wrapper
    app = Litestar(
        logging_config=None,  # keep caplog's handler
        listeners=[boom],
        plugins=[
            EventsPlugin(
                EventsConfig(
                    backend=MemoryEventEmitter, role="producer", local_listeners=[boom]
                )
            )
        ],
    )
    async with app.event_emitter:
        app.emit("ping")
        await asyncio.sleep(0.05)
    assert "Local listener" in caplog.text


async def _raising() -> None:
    msg = "nope"
    raise RuntimeError(msg)


def _by_event(app: Litestar) -> dict[str, list[EventListener]]:
    return cast("MemoryEventEmitter", app.event_emitter)._by_event
