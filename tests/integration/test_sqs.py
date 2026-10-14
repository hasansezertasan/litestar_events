from __future__ import annotations

import asyncio

import pytest

from litestar_events.contrib.sqs import SQSEventEmitter
from tests.integration._helpers import make_capture_handler, make_failing_handler


def _emitter(listeners, sqs_endpoint, queue_name):
    return SQSEventEmitter(
        listeners,
        queue_name=queue_name,
        endpoint_url=sqs_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        wait_time_seconds=1,
    )


@pytest.mark.integration
async def test_emit_delivers_to_listener(sqs_endpoint) -> None:
    handler, received, captured = make_capture_handler("user_registered")
    async with _emitter([handler], sqs_endpoint, "test-deliver") as emitter:
        await asyncio.sleep(0.2)
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.wait_for(received.wait(), timeout=30)
    assert captured == {"email": "ada@example.com"}


@pytest.mark.integration
async def test_failing_listener_does_not_block_sibling(sqs_endpoint) -> None:
    good, received, captured = make_capture_handler("isolation_check")
    bad = make_failing_handler("isolation_check")
    async with _emitter([bad, good], sqs_endpoint, "test-isolation") as emitter:
        await asyncio.sleep(0.2)
        emitter.emit("isolation_check", payload="ok")
        await asyncio.wait_for(received.wait(), timeout=30)
    assert captured == {"payload": "ok"}
