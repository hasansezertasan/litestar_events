from __future__ import annotations

import asyncio

import pytest

from litestar_events.contrib.postgres import PostgresEventEmitter
from tests.integration._helpers import make_capture_handler, make_failing_handler


@pytest.mark.integration
async def test_emit_delivers_to_listener(postgres_dsn) -> None:
    handler, received, captured = make_capture_handler("user_registered")
    async with PostgresEventEmitter([handler], dsn=postgres_dsn) as emitter:
        await asyncio.sleep(0.2)
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.wait_for(received.wait(), timeout=10)
    assert captured == {"email": "ada@example.com"}


@pytest.mark.integration
async def test_failing_listener_does_not_block_sibling(postgres_dsn) -> None:
    good, received, captured = make_capture_handler("isolation_check")
    bad = make_failing_handler("isolation_check")
    async with PostgresEventEmitter([bad, good], dsn=postgres_dsn) as emitter:
        await asyncio.sleep(0.2)
        emitter.emit("isolation_check", payload="ok")
        await asyncio.wait_for(received.wait(), timeout=10)
    assert captured == {"payload": "ok"}


@pytest.mark.integration
async def test_invalid_event_id_raises_at_enter(postgres_dsn) -> None:
    from litestar.events import listener as litestar_listener

    @litestar_listener("user.created.with.dots")
    async def handler(**_) -> None:
        pass

    with pytest.raises(ValueError, match="invalid PostgreSQL"):
        async with PostgresEventEmitter([handler], dsn=postgres_dsn):
            pass
