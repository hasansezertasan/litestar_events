"""Shared helpers for integration tests.

Each test asserts the same contract:
  emit() -> listener receives the kwargs -> per-listener error isolation.
The shape is identical across backends; only the emitter class differs.
"""

from __future__ import annotations

import asyncio
from typing import NoReturn

from litestar.events import listener as litestar_listener


def make_capture_handler(event_id: str):
    """Build a listener that records its first kwargs and signals an Event.

    Returns (listener, asyncio.Event, captured_dict).
    """
    received = asyncio.Event()
    captured: dict[str, object] = {}

    @litestar_listener(event_id)
    async def handler(**kwargs) -> None:
        if not received.is_set():
            captured.update(kwargs)
            received.set()

    return handler, received, captured


def make_failing_handler(event_id: str):
    """Build a listener that always raises."""

    @litestar_listener(event_id)
    async def handler(**_) -> NoReturn:
        msg = "listener boom"
        raise RuntimeError(msg)

    return handler
