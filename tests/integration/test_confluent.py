from __future__ import annotations

import asyncio
import uuid

import pytest

from litestar_events.contrib.confluent import ConfluentEventEmitter
from tests.integration._helpers import make_capture_handler, make_failing_handler


@pytest.mark.integration
async def test_emit_delivers_to_listener(kafka_bootstrap) -> None:
    handler, received, captured = make_capture_handler("user_registered")
    async with ConfluentEventEmitter(
        [handler],
        bootstrap_servers=kafka_bootstrap,
        topic_prefix=f"test{uuid.uuid4().hex[:8]}.",
        consumer_config={"auto.offset.reset": "earliest"},
    ) as emitter:
        await asyncio.sleep(2.0)
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.wait_for(received.wait(), timeout=20)
    assert captured == {"email": "ada@example.com"}


@pytest.mark.integration
async def test_failing_listener_does_not_block_sibling(kafka_bootstrap) -> None:
    good, received, captured = make_capture_handler("isolation_check")
    bad = make_failing_handler("isolation_check")
    async with ConfluentEventEmitter(
        [bad, good],
        bootstrap_servers=kafka_bootstrap,
        topic_prefix=f"test{uuid.uuid4().hex[:8]}.",
        consumer_config={"auto.offset.reset": "earliest"},
    ) as emitter:
        await asyncio.sleep(2.0)
        emitter.emit("isolation_check", payload="ok")
        await asyncio.wait_for(received.wait(), timeout=20)
    assert captured == {"payload": "ok"}
