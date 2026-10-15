"""ASGI middleware tying a request id to the events emitted while serving it."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from litestar.datastructures import MutableScopeHeaders

from litestar_events.context import event_headers, new_correlation_id

if TYPE_CHECKING:
    from litestar.types import ASGIApp, Message, Receive, Scope, Send

__all__ = ["RequestIdMiddleware"]


# Request ids end up in logs and broker messages, so accept only a safe
# charset and length; anything else is replaced by a generated id.
_SAFE_ID = re.compile(r"^[A-Za-z0-9._\-]{1,128}$")


class RequestIdMiddleware:
    """Use the request's id as the correlation id of events emitted during it.

    The id is read from ``header`` (``x-request-id`` by default). A missing or
    unsafe value is replaced with a generated one. Events emitted while the
    request is handled carry the id, so a worker's logs can be tied back to the
    request. With ``echo`` the id is also set on the response.

    Add it through ``EventsConfig(request_id_header=...)``, or register it
    yourself as ordinary Litestar middleware.
    """

    def __init__(
        self, app: ASGIApp, *, header: str = "x-request-id", echo: bool = True
    ) -> None:
        self.app = app
        self.header = header.lower()
        self.echo = echo

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        request_id = self._incoming(scope) or new_correlation_id()

        async def send_with_id(message: Message) -> None:
            if self.echo and message["type"] == "http.response.start":
                headers = MutableScopeHeaders(message)
                if self.header not in headers:
                    headers.add(self.header, request_id)
            await send(message)

        with event_headers(correlation_id=request_id):
            await self.app(scope, receive, send_with_id)

    def _incoming(self, scope: Scope) -> str | None:
        wanted = self.header.encode()
        for name, value in scope.get("headers", ()):
            if name.lower() == wanted:
                candidate: Any = value.decode("latin-1")
                return candidate if _SAFE_ID.match(candidate) else None
        return None
