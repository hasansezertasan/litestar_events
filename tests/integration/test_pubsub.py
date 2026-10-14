from __future__ import annotations

import asyncio

import pytest

from litestar_events.contrib.pubsub import PubSubEventEmitter
from tests.integration._helpers import make_capture_handler, make_failing_handler


def _emitter(listeners, emulator_host, topic_id):
    return PubSubEventEmitter(
        listeners,
        project_id="test-project",
        topic_id=topic_id,
        emulator_host=emulator_host,
    )


@pytest.mark.integration
async def test_emit_delivers_to_listener(pubsub_emulator) -> None:
    handler, received, captured = make_capture_handler("user_registered")
    async with _emitter([handler], pubsub_emulator, "test-deliver") as emitter:
        await asyncio.sleep(0.3)
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.wait_for(received.wait(), timeout=30)
    assert captured == {"email": "ada@example.com"}


@pytest.mark.integration
async def test_failing_listener_does_not_block_sibling(pubsub_emulator) -> None:
    good, received, captured = make_capture_handler("isolation_check")
    bad = make_failing_handler("isolation_check")
    async with _emitter([bad, good], pubsub_emulator, "test-isolation") as emitter:
        await asyncio.sleep(0.3)
        emitter.emit("isolation_check", payload="ok")
        await asyncio.wait_for(received.wait(), timeout=30)
    assert captured == {"payload": "ok"}
