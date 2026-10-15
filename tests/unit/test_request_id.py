"""The request-id middleware and its plugin option."""

from __future__ import annotations

import time
from typing import Any

from litestar import Litestar, Request, get
from litestar.events import listener
from litestar.testing import TestClient

from litestar_events.config import EventsConfig
from litestar_events.context import EventContext
from litestar_events.contrib.memory import MemoryEventEmitter
from litestar_events.plugin import EventsPlugin


def _app(seen: list[str | None], **config: Any) -> Litestar:
    @listener("hit")
    async def on_hit(ctx: EventContext) -> None:
        seen.append(ctx.correlation_id)

    @get("/hit")
    async def hit(request: Request[Any, Any, Any]) -> str:
        request.app.emit("hit")
        return "ok"

    return Litestar(
        route_handlers=[hit],
        listeners=[on_hit],
        plugins=[EventsPlugin(EventsConfig(backend=MemoryEventEmitter, **config))],
    )


def _call(
    app: Litestar,
    seen: list[str | None],
    headers: dict[str, str] | None = None,
    path: str = "/hit",
) -> Any:
    with TestClient(app) as client:
        response = client.get(path, headers=headers)
        # The listener runs on the emitter's background task.
        deadline = time.monotonic() + 5
        while not seen and time.monotonic() < deadline:
            time.sleep(0.01)
    return response


def test_request_id_becomes_correlation_id_and_is_echoed() -> None:
    seen: list[str | None] = []
    response = _call(
        _app(seen, request_id_header="x-request-id"), seen, {"x-request-id": "req-123"}
    )
    assert seen == ["req-123"]
    assert response.headers["x-request-id"] == "req-123"


def test_missing_request_id_is_generated() -> None:
    seen: list[str | None] = []
    response = _call(_app(seen, request_id_header="x-request-id"), seen)
    assert seen[0] is not None
    assert response.headers["x-request-id"] == seen[0]


def test_unsafe_request_id_is_replaced() -> None:
    seen: list[str | None] = []
    _call(
        _app(seen, request_id_header="x-request-id"), seen, {"x-request-id": "a b\tc"}
    )
    assert seen[0] != "a b\tc"
    assert seen[0] is not None
    assert len(seen[0]) == 32


def test_echo_can_be_disabled() -> None:
    seen: list[str | None] = []
    response = _call(
        _app(seen, request_id_header="x-request-id", echo_request_id=False),
        seen,
        {"x-request-id": "r1"},
    )
    assert seen == ["r1"]
    assert "x-request-id" not in response.headers


def test_no_middleware_by_default() -> None:
    seen: list[str | None] = []
    response = _call(_app(seen), seen, {"x-request-id": "r1"})
    assert seen[0] != "r1"
    assert "x-request-id" not in response.headers


def test_excluded_path_gets_no_request_id() -> None:
    seen: list[str | None] = []
    response = _call(
        _app(
            seen,
            request_id_header="x-request-id",
            request_id_exclude_path_pattern="^/hit",
        ),
        seen,
        {"x-request-id": "r1"},
    )
    assert seen[0] != "r1"
    assert "x-request-id" not in response.headers
