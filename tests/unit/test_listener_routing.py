"""Unit test for the listener-routing dict every backend builds in __init__.

This is the only piece of routing logic shared by every backend. It maps
event_id -> list[EventListener], handling multi-event listeners and multiple
listeners per event. We test it via the rabbit emitter (cheapest to import)
since the behavior is identical across all seven.
"""

from __future__ import annotations

from litestar.events import listener


def _emitter_for(*listeners):
    """Build a rabbit emitter purely to introspect ``_by_event``.

    We never enter the async context, so no broker is needed.
    """
    from litestar_events.contrib.rabbit.emitter import RabbitEventEmitter

    return RabbitEventEmitter(listeners)


async def test_single_event_single_listener() -> None:
    @listener("user_registered")
    async def lstn(**_) -> None:
        pass

    e = _emitter_for(lstn)
    assert set(e._by_event) == {"user_registered"}
    assert e._by_event["user_registered"] == [lstn]


async def test_multiple_listeners_same_event() -> None:
    @listener("user_registered")
    async def a(**_) -> None:
        pass

    @listener("user_registered")
    async def b(**_) -> None:
        pass

    e = _emitter_for(a, b)
    assert e._by_event["user_registered"] == [a, b]


async def test_listener_with_multiple_event_ids() -> None:
    @listener("user_registered", "password_changed")
    async def both(**_) -> None:
        pass

    e = _emitter_for(both)
    assert set(e._by_event) == {"user_registered", "password_changed"}
    assert e._by_event["user_registered"] == [both]
    assert e._by_event["password_changed"] == [both]


async def test_empty_listeners() -> None:
    e = _emitter_for()
    assert dict(e._by_event) == {}
