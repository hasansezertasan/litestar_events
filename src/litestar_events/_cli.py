"""The ``litestar events`` command group."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import signal
import time
from typing import TYPE_CHECKING, Any, cast

import click
from litestar.cli._utils import (  # noqa: PLC2701  # no public API for CLI groups
    LitestarGroup,
    console,
)

from litestar_events.config import EventsConfig, EventsRole

if TYPE_CHECKING:
    from collections.abc import Callable

    from litestar import Litestar
    from litestar.events import BaseEventEmitterBackend, EventListener

__all__ = ["events_group"]


logger = logging.getLogger(__name__)


class _Activity:
    """Tracks in-flight and recent listener invocations."""

    def __init__(self) -> None:
        self.in_flight = 0
        self.last = time.monotonic()

    def wrap(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        async def tracked(*args: Any, **kwargs: Any) -> Any:
            self.in_flight += 1
            try:
                return await fn(*args, **kwargs)
            finally:
                self.in_flight -= 1
                self.last = time.monotonic()

        return tracked

    def idle_for(self) -> float:
        return 0.0 if self.in_flight else time.monotonic() - self.last


def _listeners(emitter: BaseEventEmitterBackend) -> set[EventListener]:
    return set().union(*emitter.listeners.values())


def _emitter(app: Litestar) -> BaseEventEmitterBackend:
    return app.event_emitter


def _require_shareable(emitter: BaseEventEmitterBackend) -> None:
    if getattr(emitter, "process_local", False):
        msg = (
            f"{type(emitter).__name__} is process-local: a separate worker would "
            "never see events emitted by the web process. Use a broker backend."
        )
        raise click.UsageError(msg)
    if not getattr(emitter, "durable", True):
        console.print(
            f"[yellow]Warning: {type(emitter).__name__} is not durable; events "
            "emitted while no worker is connected are lost.[/]"
        )


def _set_role(emitter: BaseEventEmitterBackend, role: EventsRole) -> None:
    use_role = getattr(emitter, "use_role", None)
    if use_role is None:
        msg = f"{type(emitter).__name__} does not support roles"
        raise click.UsageError(msg)
    use_role(role)


def _as_worker(emitter: BaseEventEmitterBackend) -> None:
    _set_role(emitter, EventsRole.WORKER)


async def _wait_for_signal() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()


async def _serve(app: Litestar) -> None:
    async with app.lifespan():
        console.print("[green]Worker consuming events. Press Ctrl+C to stop.[/]")
        await _wait_for_signal()
        console.print("[yellow]Shutting down...[/]")


async def _drain(app: Litestar, timeout: float) -> None:
    activity = _Activity()
    for listener in _listeners(_emitter(app)):
        listener.fn = activity.wrap(listener.fn)
    async with app.lifespan():
        console.print(f"Draining until idle for {timeout:g}s...")
        # Polls listener activity, which has no event to wait on.
        while activity.idle_for() < timeout:  # noqa: ASYNC110
            await asyncio.sleep(min(0.2, timeout))


async def _status(app: Litestar) -> None:
    emitter = _emitter(app)
    _set_role(emitter, EventsRole.PRODUCER)
    async with emitter:
        pass


def _info_rows(emitter: BaseEventEmitterBackend) -> list[tuple[str, str]]:
    events = sorted(emitter.listeners)
    return [
        ("Backend", type(emitter).__name__),
        ("Role", str(getattr(emitter, "role", EventsRole.EMBEDDED).value)),
        ("Durable", str(getattr(emitter, "durable", True))),
        ("Process-local", str(getattr(emitter, "process_local", False))),
        ("Listeners", str(len(_listeners(emitter)))),
        ("Local listeners", str(len(getattr(emitter, "_local", ())))),
        ("Events", ", ".join(events) or "-"),
    ]


@click.group(cls=LitestarGroup, name="events", no_args_is_help=True)
def events_group() -> None:
    """Run and inspect the event consumer process."""


@click.command(name="run")
def run_command(app: Litestar) -> None:
    """Consume events until SIGINT/SIGTERM."""
    emitter = _emitter(app)
    _require_shareable(emitter)
    _as_worker(emitter)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_serve(app))


@click.command(name="drain")
@click.option(
    "--timeout",
    type=click.FloatRange(min=0.0, min_open=True),
    default=None,
    help="Exit once no event arrived for this many seconds (default: config).",
)
def drain_command(app: Litestar, timeout: float | None) -> None:
    """Consume the backlog, then exit once idle."""
    emitter = _emitter(app)
    _require_shareable(emitter)
    _as_worker(emitter)
    asyncio.run(_drain(app, timeout if timeout is not None else _default_timeout(app)))


def _default_timeout(app: Litestar) -> float:
    for plugin in app.plugins.cli:
        config = getattr(plugin, "config", None)
        if isinstance(config, EventsConfig):
            return config.drain_timeout
    return EventsConfig.drain_timeout


@click.command(name="status")
def status_command(app: Litestar) -> None:
    """Check that the broker is reachable.

    Raises:
        Exit: with code 1 when the connection fails.
    """
    try:
        asyncio.run(_status(app))
    except Exception as exc:
        console.print(f"[red]Broker unreachable: {exc}[/]")
        raise click.exceptions.Exit(1) from exc
    console.print("[green]Broker reachable.[/]")


@click.command(name="info")
def info_command(app: Litestar) -> None:
    """Show the emitter configuration."""
    for key, value in _info_rows(_emitter(app)):
        console.print(f"[bold]{key}:[/] {value}")


for _command in (run_command, drain_command, status_command, info_command):
    # LitestarGroup is typed to take rich-click commands; it wraps any command.
    events_group.add_command(cast("Any", _command))
