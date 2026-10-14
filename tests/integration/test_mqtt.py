from __future__ import annotations

import asyncio

import pytest

from litestar_events.contrib.mqtt import MQTTEventEmitter
from tests.integration._helpers import make_capture_handler, make_failing_handler


@pytest.mark.integration
async def test_emit_delivers_to_listener(mqtt_host_port) -> None:
    host, port = mqtt_host_port
    handler, received, captured = make_capture_handler("user_registered")
    async with MQTTEventEmitter([handler], hostname=host, port=port, qos=1) as emitter:
        await asyncio.sleep(0.3)
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.wait_for(received.wait(), timeout=10)
    assert captured == {"email": "ada@example.com"}


@pytest.mark.integration
async def test_failing_listener_does_not_block_sibling(mqtt_host_port) -> None:
    host, port = mqtt_host_port
    good, received, captured = make_capture_handler("isolation_check")
    bad = make_failing_handler("isolation_check")
    async with MQTTEventEmitter(
        [bad, good], hostname=host, port=port, qos=1
    ) as emitter:
        await asyncio.sleep(0.3)
        emitter.emit("isolation_check", payload="ok")
        await asyncio.wait_for(received.wait(), timeout=10)
    assert captured == {"payload": "ok"}
