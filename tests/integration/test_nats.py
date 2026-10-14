from __future__ import annotations

import asyncio

import pytest

from litestar_events.contrib.nats import NATSEventEmitter
from tests.integration._helpers import make_capture_handler, make_failing_handler


@pytest.mark.integration
async def test_emit_delivers_to_listener(nats_url) -> None:
    handler, received, captured = make_capture_handler("user_registered")
    async with NATSEventEmitter([handler], servers=nats_url) as emitter:
        await asyncio.sleep(0.2)
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.wait_for(received.wait(), timeout=10)
    assert captured == {"email": "ada@example.com"}


@pytest.mark.integration
async def test_failing_listener_does_not_block_sibling(nats_url) -> None:
    good, received, captured = make_capture_handler("isolation_check")
    bad = make_failing_handler("isolation_check")
    async with NATSEventEmitter([bad, good], servers=nats_url) as emitter:
        await asyncio.sleep(0.2)
        emitter.emit("isolation_check", payload="ok")
        await asyncio.wait_for(received.wait(), timeout=10)
    assert captured == {"payload": "ok"}
