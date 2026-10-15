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
from litestar.cli._utils import LitestarGroup, console  # noqa: PLC2701  # no public API for CLI groups

from litestar_events._queue import RoleAwareEmitter
from litestar_events.config import EventsConfig, EventsRole

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator

    from litestar import Litestar
    from litestar.events import BaseEventEmitterBackend, EventListener

    from litestar_events.codec import Codec, Message

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

    def track(self, codec: Codec) -> Codec:
        """Wrap ``codec`` so the listener invocations it makes are counted."""
        return _TrackedCodec(codec, self)

    def idle_for(self) -> float:
        return 0.0 if self.in_flight else time.monotonic() - self.last


class _TrackedCodec:
    """Delegates to ``inner`` and reports listener calls to an :class:`_Activity`."""

    def __init__(self, inner: Codec, activity: _Activity) -> None:
        self._inner = inner
        self._activity = activity
        self.content_type = inner.content_type

    def __getattr__(self, name: str) -> Any:
        # Anything beyond the protocol (e.g. ``JSONCodec.typed``) is the inner codec's.
        if name.startswith("_"):  # not yet set (copy/unpickle): avoid recursing
            raise AttributeError(name)
        return getattr(self._inner, name)

    def encode(self, message: Message) -> bytes:
        return self._inner.encode(message)

    def decode(self, data: bytes | str) -> Message:
        return self._inner.decode(data)

    def invoke(
        self, fn: Callable[..., Awaitable[Any]], event_id: str, message: Message
    ) -> Awaitable[Any]:
        return self._activity.wrap(self._inner.invoke)(fn, event_id, message)


def _listeners(emitter: BaseEventEmitterBackend) -> set[EventListener]:
    return set().union(*emitter.listeners.values())


def _emitter(app: Litestar) -> BaseEventEmitterBackend:
    return app.event_emitter


def _require_shareable(emitter: BaseEventEmitterBackend) -> None:
    if not isinstance(emitter, RoleAwareEmitter):
        return
    if emitter.process_local:
        msg = (
            f"{type(emitter).__name__} is process-local: a separate worker would "
            "never see events emitted by the web process. Use a broker backend."
        )
        raise click.UsageError(msg)
    if not emitter.durable:
        console.print(
            f"[yellow]Warning: {type(emitter).__name__} is not durable; events "
            "emitted while no worker is connected are lost.[/]"
        )


def _set_role(emitter: BaseEventEmitterBackend, role: EventsRole) -> None:
    if not isinstance(emitter, RoleAwareEmitter):
        msg = f"{type(emitter).__name__} does not support roles"
        raise click.UsageError(msg)
    emitter.use_role(role)


def _as_worker(emitter: BaseEventEmitterBackend) -> None:
    _set_role(emitter, EventsRole.WORKER)


async def _wait_for_signal() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows event loops lack add_signal_handler
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    await stop.wait()


async def _serve(app: Litestar) -> None:
    async with app.lifespan():
        console.print("[green]Worker consuming events. Press Ctrl+C to stop.[/]")
        await _wait_for_signal()
        console.print("[yellow]Shutting down...[/]")


@contextlib.contextmanager
def _tracking(
    emitter: BaseEventEmitterBackend, activity: _Activity
) -> Generator[None, None, None]:
    """Count listener invocations of ``emitter`` for the duration of the block.

    A role-aware emitter runs listeners through its codec, so the codec is
    wrapped. Any other backend gets its listener functions wrapped instead.
    Both are restored on exit.
    """
    if isinstance(emitter, RoleAwareEmitter):
        original = emitter.codec
        emitter.use_codec(activity.track(original))
        try:
            yield
        finally:
            emitter.use_codec(original)
        return
    listeners = _listeners(emitter)
    originals = {item: item.fn for item in listeners}
    for item, fn in originals.items():
        item.fn = activity.wrap(fn)
    try:
        yield
    finally:
        for item, fn in originals.items():
            item.fn = fn


async def _drain(app: Litestar, timeout: float) -> None:
    activity = _Activity()
    with _tracking(_emitter(app), activity):
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
    # Defaults of ``QueuedEmitterMixin`` for a backend that is not role-aware.
    role, durable, process_local, local = EventsRole.EMBEDDED, True, False, 0
    if isinstance(emitter, RoleAwareEmitter):
        role, durable = emitter.role, emitter.durable
        process_local, local = emitter.process_local, len(emitter.local_listeners)
    return [
        ("Backend", type(emitter).__name__),
        ("Role", str(role.value)),
        ("Durable", str(durable)),
        ("Process-local", str(process_local)),
        ("Listeners", str(len(_listeners(emitter)))),
        ("Local listeners", str(local)),
        ("Events", ", ".join(sorted(emitter.listeners)) or "-"),
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
    # By config type, since the plugin module imports this one; this also
    # finds a subclass of the plugin.
    for plugin in app.plugins.init:
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
