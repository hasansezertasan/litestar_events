from __future__ import annotations

import asyncio

import pytest

from litestar_events.contrib.zmq import ZeroMQEventEmitter
from tests.integration._helpers import make_capture_handler, make_failing_handler

# ZeroMQ is brokerless: these tests need no container, only loopback sockets.
# Distinct ports per test avoid "address already in use" between cases.


@pytest.mark.integration
async def test_emit_delivers_to_listener() -> None:
    handler, received, captured = make_capture_handler("user_registered")
    async with ZeroMQEventEmitter(
        [handler], pub_address="tcp://127.0.0.1:5571"
    ) as emitter:
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.wait_for(received.wait(), timeout=10)
    assert captured == {"email": "ada@example.com"}


@pytest.mark.integration
async def test_failing_listener_does_not_block_sibling() -> None:
    good, received, captured = make_capture_handler("isolation_check")
    bad = make_failing_handler("isolation_check")
    async with ZeroMQEventEmitter(
        [bad, good], pub_address="tcp://127.0.0.1:5572"
    ) as emitter:
        emitter.emit("isolation_check", payload="ok")
        await asyncio.wait_for(received.wait(), timeout=10)
    assert captured == {"payload": "ok"}
