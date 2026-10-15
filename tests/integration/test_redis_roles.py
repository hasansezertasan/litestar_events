from __future__ import annotations

import asyncio

import pytest
from litestar.events import listener as litestar_listener

from litestar_events.context import EventContext, event_headers
from litestar_events.contrib.redis import RedisEventEmitter


@pytest.mark.integration
async def test_producer_to_worker_round_trip(redis_url) -> None:
    received = asyncio.Event()
    captured: dict[str, object] = {}

    @litestar_listener("order_created")
    async def on_order(order_id: int, ctx: EventContext) -> None:
        captured.update(
            order_id=order_id,
            headers=dict(ctx.headers),
            correlation_id=ctx.correlation_id,
        )
        received.set()

    # Separate emitters stand in for the web (producer) and worker processes.
    async with (
        RedisEventEmitter([on_order], redis_url=redis_url, role="worker"),
        RedisEventEmitter([on_order], redis_url=redis_url, role="producer") as web,
    ):
        await asyncio.sleep(0.2)
        with event_headers({"tenant": "acme"}):
            web.emit("order_created", order_id="42")
        await asyncio.wait_for(received.wait(), timeout=10)

    # typed decoding coerced "42" -> 42 on the worker side
    assert captured["order_id"] == 42
    assert captured["headers"] == {"tenant": "acme"}
    assert captured["correlation_id"]


@pytest.mark.integration
async def test_worker_cannot_emit_and_producer_does_not_consume(redis_url) -> None:
    ran = asyncio.Event()

    @litestar_listener("producer_only_check")
    async def handler(**_) -> None:
        ran.set()

    async with RedisEventEmitter([handler], redis_url=redis_url, role="producer") as p:
        p.emit("producer_only_check", x=1)
        await asyncio.sleep(0.5)
    assert not ran.is_set()

    async with RedisEventEmitter([handler], redis_url=redis_url, role="worker") as w:
        with pytest.raises(Exception, match="worker"):
            w.emit("producer_only_check", x=1)
