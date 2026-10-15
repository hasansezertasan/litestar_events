"""Configuration shared by the producer and the worker process."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar.events import BaseEventEmitterBackend, EventListener

    from litestar_events.codec import Codec

__all__ = ["EventsConfig", "EventsRole", "resolve_role"]


class EventsRole(str, Enum):
    """What an emitter does in the current process."""

    #: Publish and consume in the same process (the historical behavior).
    EMBEDDED = "embedded"
    #: Publish only: no broker subscription, no consumer task.
    PRODUCER = "producer"
    #: Consume only: listeners run here, ``emit`` is rejected.
    WORKER = "worker"


def resolve_role(role: EventsRole | str | None = None) -> EventsRole:
    """Resolve ``role`` to an :class:`EventsRole`; ``None`` means embedded.

    Raises:
        ValueError: if ``role`` is not a valid role.
    """
    raw = role or EventsRole.EMBEDDED
    try:
        return EventsRole(raw)
    except ValueError:
        valid = ", ".join(r.value for r in EventsRole)
        msg = f"Invalid events role {raw!r}; expected one of: {valid}"
        raise ValueError(msg) from None


@dataclass
class EventsConfig:
    """Settings for :class:`~litestar_events.plugin.EventsPlugin`.

    Import the same config in the web app and the worker: the worker loads the
    same Litestar app (``litestar events run``) and only the role differs.
    """

    backend: type[BaseEventEmitterBackend] | None = None
    """Emitter backend to install on the app. ``None`` keeps the app's own."""
    backend_options: dict[str, Any] = field(default_factory=dict)
    """Keyword arguments forwarded to the backend constructor."""
    role: EventsRole | str | None = None
    """Role for this process; ``events run`` always uses ``worker``."""
    codec: Codec | None = None
    """Wire format shared by producer and worker; default is :class:`JSONCodec`."""
    typed_decoding: bool = True
    """Convert worker-side arguments to the listener's annotated types.

    Only applies to the default codec; a custom ``codec`` decides for itself."""
    local_listeners: Sequence[EventListener] = ()
    """Listeners that run in the web (producer) process instead of the worker.

    A ``producer`` calls them directly from ``app.emit()`` without the broker;
    a ``worker`` skips them. Each must also be in the app's ``listeners``."""
    request_id_header: str | None = None
    """Request header whose value becomes the correlation id of events emitted
    while serving the request (e.g. ``"x-request-id"``). ``None`` adds no
    middleware."""
    echo_request_id: bool = True
    """Also set the request id on the response (needs ``request_id_header``)."""
    drain_timeout: float = 30.0
    """Default seconds ``events drain`` waits for in-flight listeners."""
