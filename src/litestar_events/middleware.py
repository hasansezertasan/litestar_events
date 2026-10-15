"""ASGI middleware tying a request id to the events emitted while serving it."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, cast

from litestar.datastructures import Headers, MutableScopeHeaders
from litestar.enums import ScopeType
from litestar.middleware import ASGIMiddleware

from litestar_events.context import event_headers, new_correlation_id

if TYPE_CHECKING:
    from litestar.types import ASGIApp, Message, Receive, Scope, Send
    from litestar.types.asgi_types import HeaderScope

__all__ = ["RequestIdMiddleware"]


# Request ids end up in logs and broker messages, so accept only a safe
# charset and length; anything else is replaced by a generated id.
_SAFE_ID = re.compile(r"[A-Za-z0-9._\-]{1,128}")

# The messages that carry response headers: HTTP, and the WebSocket handshake.
_ECHO_ON = frozenset({"http.response.start", "websocket.accept"})


class RequestIdMiddleware(ASGIMiddleware):
    """Use the request's id as the correlation id of events emitted during it.

    The id is read from ``header`` (``x-request-id`` by default). A missing or
    unsafe value is replaced with a generated one. Events emitted while the
    request is handled carry the id, so a worker's logs can be tied back to the
    request. With ``echo`` the id is also set on the response.

    Routes can opt out with ``opt={"exclude_request_id": True}`` or by path
    through ``exclude_path_pattern``.

    Add it through ``EventsConfig(request_id_header=...)``, or register it
    yourself as ordinary Litestar middleware.
    """

    scopes = (ScopeType.HTTP, ScopeType.WEBSOCKET)
    exclude_opt_key = "exclude_request_id"

    def __init__(
        self,
        *,
        header: str = "x-request-id",
        echo: bool = True,
        exclude_path_pattern: str | tuple[str, ...] | None = None,
    ) -> None:
        self.header = header.lower()
        self.echo = echo
        self.exclude_path_pattern = exclude_path_pattern

    async def handle(
        self, scope: Scope, receive: Receive, send: Send, next_app: ASGIApp
    ) -> None:
        request_id = self._incoming(scope) or new_correlation_id()

        async def send_with_id(message: Message) -> None:
            if self.echo and message["type"] in _ECHO_ON:
                # ``headers`` is optional in ASGI for these message types.
                scoped = cast("HeaderScope", message)
                scoped.setdefault("headers", [])
                headers = MutableScopeHeaders(scoped)
                if self.header not in headers:
                    headers.add(self.header, request_id)
            await send(message)

        with event_headers(correlation_id=request_id):
            await next_app(scope, receive, send_with_id)

    def _incoming(self, scope: Scope) -> str | None:
        candidate = Headers.from_scope(scope).get(self.header)
        if candidate is not None and _SAFE_ID.fullmatch(candidate):
            return candidate
        return None
