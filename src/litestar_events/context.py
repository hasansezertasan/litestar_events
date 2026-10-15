"""Event headers, correlation ids and the listener-side event context.

Every event carries a correlation id and optional string headers across the
broker. Set them for the events emitted in a block with :func:`event_headers`;
read them in a listener by annotating a parameter as :class:`EventContext` or by
calling :func:`current_event`. Events emitted from inside a listener inherit its
correlation id, so one request can be traced across processes.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

__all__ = [
    "EventContext",
    "current_event",
    "event_headers",
    "new_correlation_id",
    "outgoing_correlation_id",
    "outgoing_headers",
    "use_event",
]

_EMPTY: Mapping[str, str] = MappingProxyType({})

_ambient_headers: ContextVar[Mapping[str, str]] = ContextVar(
    "litestar_events_headers", default=_EMPTY
)
_ambient_correlation: ContextVar[str | None] = ContextVar(
    "litestar_events_correlation_id", default=None
)
_current: ContextVar[EventContext | None] = ContextVar(
    "litestar_events_current", default=None
)


@dataclass(frozen=True)
class EventContext:
    """What a listener can know about the event it is handling.

    Annotate a listener parameter with this type and it is injected::

        @listener("order_created")
        async def on_order(order_id: int, ctx: EventContext) -> None: ...
    """

    event_id: str
    headers: Mapping[str, str]
    correlation_id: str | None


def new_correlation_id() -> str:
    """Return a fresh correlation id."""
    return uuid.uuid4().hex


def current_event() -> EventContext | None:
    """Return the event the running listener is handling, else ``None``."""
    return _current.get()


@contextmanager
def event_headers(
    headers: Mapping[str, str] | None = None,
    /,
    *,
    correlation_id: str | None = None,
    **extra: str,
) -> Iterator[None]:
    """Attach headers (and optionally a correlation id) to events emitted inside.

    Nested blocks merge, inner values winning. Header names and values are
    strings. Example::

        with event_headers(tenant="acme", correlation_id=request_id):
            app.emit("order_created", order_id=1)

    Raises:
        TypeError: if a header name or value is not a string.
    """
    merged = {**_ambient_headers.get(), **(headers or {}), **extra}
    for key, value in merged.items():
        if not isinstance(key, str) or not isinstance(value, str):
            msg = f"Event header {key!r} must map a string to a string"
            raise TypeError(msg)
    headers_token = _ambient_headers.set(MappingProxyType(merged))
    correlation_token = (
        _ambient_correlation.set(correlation_id) if correlation_id else None
    )
    try:
        yield
    finally:
        _ambient_headers.reset(headers_token)
        if correlation_token is not None:
            _ambient_correlation.reset(correlation_token)


@contextmanager
def use_event(context: EventContext) -> Iterator[None]:
    """Make ``context`` the current event; used while a listener runs."""
    token = _current.set(context)
    try:
        yield
    finally:
        _current.reset(token)


def outgoing_headers() -> dict[str, str]:
    """Headers to stamp on an event emitted now."""
    return dict(_ambient_headers.get())


def outgoing_correlation_id() -> str:
    """Correlation id for an event emitted now.

    The explicit one from :func:`event_headers`, else the one of the event the
    current listener handles, else a new id.
    """
    explicit = _ambient_correlation.get()
    if explicit:
        return explicit
    current = _current.get()
    if current is not None and current.correlation_id:
        return current.correlation_id
    return new_correlation_id()
