"""Shared utilities for queue-backed event emitter backends."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
    runtime_checkable,
)

from litestar_events.codec import Codec, JSONCodec, Message, listener_name
from litestar_events.config import EventsRole, resolve_role
from litestar_events.context import outgoing_correlation_id, outgoing_headers

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping

    from litestar.events import BaseEventEmitterBackend, EventListener

__all__ = [
    "CORRELATION_HEADER",
    "QueuedEmitterMixin",
    "RoleAwareEmitter",
    "native_headers",
    "require",
]


_T = TypeVar("_T")

logger = logging.getLogger(__name__)

#: Header carrying the correlation id in a broker's native metadata.
CORRELATION_HEADER = "correlation-id"

#: The payload buffered on a backend's publish queue:
#: ``(event_id, encoded_body, native_metadata)``. The metadata (event headers
#: plus the correlation id) is a copy for brokers that can carry it outside the
#: body; consumers still read the values from the decoded envelope.
QueuePayload: TypeAlias = tuple[str, bytes, dict[str, str]]


def native_headers(
    metadata: Mapping[str, str], content_type: str, *, key: str = "content-type"
) -> dict[str, str]:
    """Headers for a broker's native metadata, led by ``content_type`` under ``key``.

    A user header named ``content-type`` (any case) is dropped: the codec's
    content type describes the body and must not be overridden.
    """
    rest = {k: v for k, v in metadata.items() if k.lower() != "content-type"}
    return {key: content_type, **rest}


@runtime_checkable
class RoleAwareEmitter(Protocol):
    """What the ``events`` CLI needs from an emitter beyond Litestar's own API.

    :class:`QueuedEmitterMixin` satisfies it; a custom backend can too.
    """

    durable: bool
    process_local: bool

    @property
    def role(self) -> EventsRole:
        """The effective role of this emitter."""
        raise NotImplementedError

    @property
    def local_listeners(self) -> frozenset[EventListener]:
        """Listeners marked to run in the producer process."""
        raise NotImplementedError

    @property
    def codec(self) -> Codec:
        """The codec encoding and decoding events."""
        raise NotImplementedError

    def use_codec(self, codec: Codec) -> None:
        """Replace the codec, before the async context is entered."""

    def use_role(self, role: EventsRole | str) -> None:
        """Set the role explicitly, before the async context is entered."""

    def set_local_listeners(self, listeners: Iterable[EventListener]) -> None:
        """Mark listeners that run in the producer process."""


def _guard_enter(
    enter: Callable[[Any], Awaitable[Any]],
) -> Callable[[Any], Awaitable[Any]]:
    """Run ``__aexit__`` when ``__aenter__`` fails halfway.

    Litestar does not call ``__aexit__`` for a backend whose ``__aenter__``
    raised, so clients and tasks started before the failure would leak.

    A subclass overriding ``__aenter__`` is wrapped on top of its parent's
    wrapper; only the outermost one tears down, so it runs exactly once.
    """

    @functools.wraps(enter)
    async def wrapper(self: QueuedEmitterMixin) -> Any:
        self._loop = asyncio.get_running_loop()  # pylint: disable=protected-access  # module-level wrapper for the mixin
        nested = self._entering  # pylint: disable=protected-access  # module-level wrapper for the mixin
        self._entering = True  # pylint: disable=protected-access  # module-level wrapper for the mixin
        try:
            return await enter(self)
        except BaseException:
            if not nested:
                with contextlib.suppress(Exception):
                    await cast("Any", self).__aexit__(None, None, None)
            raise
        finally:
            if not nested:
                self._entering = False  # pylint: disable=protected-access  # module-level wrapper for the mixin

    return wrapper


def _guard_exit(
    exit_: Callable[..., Awaitable[None]],
) -> Callable[..., Awaitable[None]]:
    """Let local listener tasks finish before the backend tears down."""

    @functools.wraps(exit_)
    async def wrapper(
        self: QueuedEmitterMixin, exc_type: object, exc: object, tb: object
    ) -> None:
        try:
            await self._finish_local_tasks()  # pylint: disable=protected-access  # module-level wrapper for the mixin
        finally:
            self._loop = None  # pylint: disable=protected-access  # module-level wrapper for the mixin
            await exit_(self, exc_type, exc, tb)

    return wrapper


def require(value: _T | None, name: str) -> _T:
    """Return ``value`` if set, else raise -- an ``assert`` that survives ``-O``.

    Background publisher/consumer loops narrow attributes established in
    ``__aenter__`` (queues, clients, pools, producers). ``assert x is not None``
    narrows for the type checkers but is stripped under ``python -O``, which
    would defer a stray ``None`` to an opaque ``AttributeError`` deep inside a
    detached task. This helper raises a clear ``RuntimeError`` unconditionally
    and returns the non-``None`` value, so the caller can bind a narrowed local
    (``pool = require(self._pool, "pool")``) or use it as a bare guard.

    Raises:
        RuntimeError: if ``value`` is ``None``.
    """
    if value is None:
        msg = f"{name} is unavailable; emitter used outside its async context"
        raise RuntimeError(msg)
    return value


class QueuedEmitterMixin:
    """Provides the synchronous ``emit`` for queue-backed emitters.

    Backends that buffer events on an in-process ``asyncio.Queue`` (drained by
    a background publisher task started in ``__aenter__``) share this method.
    Subclasses set ``self._publish_queue`` to a live queue in ``__aenter__``;
    outside that window it is ``None`` (the class default below). Subclasses
    must list this mixin before ``BaseEventEmitterBackend`` so this ``emit``
    satisfies the abstract method.

    The class-level ``= None`` default is deliberate: it guarantees the
    ``emit`` guard raises a clear ``RuntimeError`` even for a subclass that
    forgets to initialize the attribute, rather than an opaque ``AttributeError``.
    """

    # Class default so the guard below holds even before any ``__init__`` /
    # ``__aenter__`` assignment; per-instance assignment shadows it normally.
    _publish_queue: asyncio.Queue[QueuePayload] | None = None
    #: ``True`` when events are only visible inside this process.
    process_local: bool = False
    #: ``False`` when events are lost while no consumer is connected.
    durable: bool = True
    _role: EventsRole = EventsRole.EMBEDDED
    _codec: Codec = JSONCodec()
    _local: frozenset[EventListener] = frozenset()
    #: The loop the emitter's async context runs on; ``emit`` may be called from
    #: another thread (a sync handler) and hands work to this loop.
    _loop: asyncio.AbstractEventLoop | None = None
    _local_tasks: set[asyncio.Task[None]]
    #: ``True`` while the outermost ``__aenter__`` wrapper is running.
    _entering: bool = False
    # Per-event listeners this process consumes from the broker; set by each
    # backend's ``__init__`` and recomputed by ``_apply_placement``.
    _by_event: dict[str, list[EventListener]]

    @property
    def role(self) -> EventsRole:
        """The effective role of this emitter."""
        return self._role

    @property
    def _publishes(self) -> bool:
        return self._role is not EventsRole.WORKER

    @property
    def _consumes(self) -> bool:
        return self._role is not EventsRole.PRODUCER

    @property
    def local_listeners(self) -> frozenset[EventListener]:
        """Listeners marked to run in the producer process."""
        return self._local

    @property
    def codec(self) -> Codec:
        """The codec encoding and decoding events."""
        return self._codec

    def use_codec(self, codec: Codec) -> None:
        """Replace the codec, before the async context is entered."""
        self._codec = codec

    def use_role(self, role: EventsRole | str) -> None:
        """Set the role explicitly, replacing the configured one.

        Must be called before the emitter's async context is entered.
        """
        self._role = EventsRole(role)
        self._apply_placement()

    def _registered(self) -> Mapping[str, Iterable[EventListener]]:
        # ``listeners`` comes from ``BaseEventEmitterBackend``, which this
        # mixin is always combined with (see ``__init_subclass__``).
        return cast("BaseEventEmitterBackend", self).listeners

    def set_local_listeners(self, listeners: Iterable[EventListener]) -> None:
        """Mark listeners that run in the producer process, not on the worker.

        A ``producer`` calls them directly from ``emit``; a ``worker`` skips
        them; an ``embedded`` emitter ignores the marking (it runs everything).
        """
        self._local = frozenset(listeners)
        self._apply_placement()

    def _apply_placement(self) -> None:
        """Rebuild ``_by_event`` without the listeners this role must not consume."""
        skip = self._local if self._role is EventsRole.WORKER else frozenset()
        by_event: dict[str, list[EventListener]] = defaultdict(list)
        for event_id, listeners in self._registered().items():
            by_event[event_id] = [
                listener for listener in listeners if listener not in skip
            ]
        self._by_event = defaultdict(list, {k: v for k, v in by_event.items() if v})

    def _init_codec(self, codec: Codec | None) -> None:
        self._codec = codec or JSONCodec()

    def _init_role(self, role: EventsRole | str | None) -> None:
        self._role = resolve_role(role)
        self._local_tasks = set()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # The MRO ordering (mixin before the ABC) is not type-checkable, so
        # enforce it at class-definition time: a wrong order would otherwise
        # leave ``emit`` abstract, deferring the failure to an opaque
        # ``TypeError`` at first instantiation instead of failing loudly here.
        super().__init_subclass__(**kwargs)
        from litestar.events import BaseEventEmitterBackend

        # Wrap only what this class itself defines; inherited methods are
        # already wrapped on the class that defined them.
        if "__aenter__" in cls.__dict__:
            setattr(cls, "__aenter__", _guard_enter(cls.__dict__["__aenter__"]))  # noqa: B010
        if "__aexit__" in cls.__dict__:
            setattr(cls, "__aexit__", _guard_exit(cls.__dict__["__aexit__"]))  # noqa: B010

        mro = cls.__mro__
        if BaseEventEmitterBackend in mro and mro.index(QueuedEmitterMixin) > mro.index(
            BaseEventEmitterBackend
        ):
            msg = (
                f"{cls.__name__} must list QueuedEmitterMixin before "
                "BaseEventEmitterBackend so the mixin's concrete `emit` "
                "satisfies the abstract method."
            )
            raise TypeError(msg)

    def emit(self, event_id: str, *args: Any, **kwargs: Any) -> None:
        if self._role is EventsRole.WORKER:
            msg = "Cannot emit from a worker-role emitter; it only consumes events"
            raise RuntimeError(msg)
        if self._publish_queue is None:
            msg = "Emitter used outside its async context"
            raise RuntimeError(msg)
        # Encode here so an unserializable payload fails at the call site
        # instead of being dropped later in the background publisher.
        headers = outgoing_headers()
        correlation_id = outgoing_correlation_id()
        body = self._codec.encode(
            Message(
                args=args, kwargs=kwargs, headers=headers, correlation_id=correlation_id
            )
        )
        metadata = {**headers}
        if correlation_id:
            metadata[CORRELATION_HEADER] = correlation_id
        self._dispatch(event_id, body, metadata)

    def _dispatch(self, event_id: str, body: bytes, metadata: dict[str, str]) -> None:
        """Hand the event to the emitter's loop, from whichever thread called.

        Events handed over from another thread just before shutdown, or after
        the emitter's loop has closed, may be lost or raise ``RuntimeError``.

        Raises:
            RuntimeError: if the emitter has no loop, i.e. is not entered.
        """
        try:
            running: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        loop = self._loop or running
        if loop is None:
            msg = "Emitter used outside its async context"
            raise RuntimeError(msg)
        if loop is running:
            self._enqueue(event_id, body, metadata)
        else:
            # A sync handler runs in a worker thread; ``asyncio.Queue`` and
            # task creation are only safe on the emitter's own loop.
            loop.call_soon_threadsafe(self._enqueue, event_id, body, metadata)

    def _enqueue(self, event_id: str, body: bytes, metadata: dict[str, str]) -> None:
        queue = self._publish_queue
        if queue is None:
            logger.warning("Dropping event %s emitted during shutdown", event_id)
            return
        queue.put_nowait((event_id, body, metadata))
        if self._role is EventsRole.PRODUCER and self._local:
            self._spawn_local(event_id, body)

    def _spawn_local(self, event_id: str, body: bytes) -> None:
        listeners = [
            listener
            for listener in self._registered().get(event_id, ())
            if listener in self._local
        ]
        if not listeners:
            return
        task = asyncio.create_task(self._run_local(event_id, body, listeners))
        self._local_tasks.add(task)
        task.add_done_callback(self._local_tasks.discard)

    async def _run_local(
        self, event_id: str, body: bytes, listeners: list[EventListener]
    ) -> None:
        # Decode the encoded body, as a worker would, so local and remote
        # listeners see the same values (and typed decoding applies to both).
        try:
            event = self._codec.decode(body)
        except Exception:
            logger.exception("Dropping undecodable local event %s", event_id)
            return
        results = await asyncio.gather(
            *(self._codec.invoke(item.fn, event_id, event) for item in listeners),
            return_exceptions=True,
        )
        for listener, result in zip(listeners, results, strict=True):
            if isinstance(result, Exception):
                logger.error(
                    "Local listener %s failed for event %s",
                    listener_name(listener.fn),
                    event_id,
                    exc_info=result,
                )

    async def _flush_publisher(self, timeout: float = 5.0) -> None:  # noqa: ASYNC109
        """Give buffered events up to ``timeout`` seconds to reach the broker.

        Backends call this from ``__aexit__`` before cancelling the publisher
        task, so events emitted just before shutdown are not silently dropped.
        Publisher loops mark each item done with ``queue.task_done()``.
        """
        queue = self._publish_queue
        task = getattr(self, "_publisher_task", None)
        if queue is None or task is None or task.done():
            return
        try:
            await asyncio.wait_for(queue.join(), timeout)
        except TimeoutError:
            logger.warning(
                "Shutting down with %d unpublished event(s) after %gs",
                queue.qsize(),
                timeout,
            )

    @staticmethod
    def _watch(task: asyncio.Task[None]) -> asyncio.Task[None]:
        """Log a background task's failure instead of letting it die silently."""

        def _log(done: asyncio.Task[None]) -> None:
            if done.cancelled():
                return
            exc = done.exception()
            if exc is not None:
                logger.error("Event consumer task stopped: %r", exc, exc_info=exc)

        task.add_done_callback(_log)
        return task

    def _close_queue(self) -> None:
        """Drop the publish queue so a post-teardown ``emit`` raises again.

        Backends call this from ``__aexit__`` after cancelling the publisher
        task. It restores the "no live queue outside the async context"
        invariant, so a late ``emit`` fails loudly instead of silently
        buffering onto an abandoned queue that nothing will drain.
        """
        self._publish_queue = None

    async def _finish_local_tasks(self, timeout: float = 5.0) -> None:  # noqa: ASYNC109
        """Wait up to ``timeout`` seconds for local listeners, then cancel them."""
        tasks = set(self._local_tasks)
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=timeout)
        if pending:
            logger.warning(
                "Cancelling %d local listener(s) still running after %gs",
                len(pending),
                timeout,
            )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
