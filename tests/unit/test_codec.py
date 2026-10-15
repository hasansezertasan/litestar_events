"""Wire format, typed decoding, headers and correlation ids."""

from __future__ import annotations

import asyncio
import datetime as dt
import sys
import typing
import uuid
from dataclasses import dataclass
from typing import Any, cast

import msgspec
import pytest
from litestar import Litestar
from litestar.events import listener
from litestar.exceptions import SerializationException

from litestar_events.codec import Codec, JSONCodec, Message
from litestar_events.config import EventsConfig
from litestar_events.context import (
    EventContext,
    current_event,
    event_headers,
    outgoing_correlation_id,
)
from litestar_events.contrib.memory import MemoryEventEmitter
from litestar_events.plugin import EventsPlugin

UTC = dt.timezone.utc


@dataclass
class Order:
    """A dataclass payload."""

    id: int
    placed: dt.datetime


class Item(msgspec.Struct):
    """A msgspec payload."""

    sku: str
    qty: int


class Totals(typing.TypedDict):
    """A TypedDict payload."""

    total: int


def _roundtrip_message(codec: Codec, *args: Any, **kwargs: Any) -> Message:
    return codec.decode(codec.encode(Message(args=args, kwargs=kwargs)))


async def _roundtrip(codec: Codec, fn: Any, *args: Any, **kwargs: Any) -> None:
    await codec.invoke(fn, "evt", _roundtrip_message(codec, *args, **kwargs))


def test_roundtrip_rich_types_become_json_values() -> None:
    codec = JSONCodec()
    when = dt.datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
    key = uuid.uuid4()
    message = _roundtrip_message(codec, key, order=Order(1, when))
    assert message.args == (str(key),)
    assert message.kwargs == {
        "order": {"id": 1, "placed": when.isoformat().replace("+00:00", "Z")}
    }


def test_encode_stamps_version_and_optional_fields() -> None:
    codec = JSONCodec()
    assert codec.encode(Message()).startswith(b'{"v":1')
    body = codec.encode(Message(headers={"t": "a"}, correlation_id="abc"))
    assert b'"cid":"abc"' in body
    assert b'"h":{"t":"a"}' in body
    decoded = codec.decode(body)
    assert (decoded.headers, decoded.correlation_id) == ({"t": "a"}, "abc")


def test_decode_accepts_legacy_unversioned_messages() -> None:
    message = JSONCodec().decode(b'{"args": [1], "kwargs": {"a": 2}}')
    assert (message.args, message.kwargs) == ((1,), {"a": 2})
    assert (message.headers, message.correlation_id) == ({}, None)


def test_decode_rejects_unknown_version_and_non_objects() -> None:
    codec = JSONCodec()
    with pytest.raises(ValueError, match="wire version"):
        codec.decode(b'{"v": 99, "args": [], "kwargs": {}}')
    with pytest.raises(TypeError, match="JSON object"):
        codec.decode(b"[1]")


def test_default_codec_satisfies_protocol() -> None:
    assert isinstance(JSONCodec(), Codec)


def test_typed_decoding_is_the_default() -> None:
    assert JSONCodec().typed is True
    assert EventsConfig().typed_decoding is True
    assert JSONCodec().content_type == "application/json"


def test_emit_fails_at_call_site_for_unserializable_payload() -> None:
    emitter = MemoryEventEmitter([])
    emitter._publish_queue = asyncio.Queue()
    with pytest.raises(SerializationException):
        emitter.emit("evt", object())
    assert emitter._publish_queue.empty()


async def test_typed_decoding_converts_to_annotations() -> None:
    seen: dict[str, Any] = {}

    async def handler(
        order: Order, key: uuid.UUID, *, n: int = 0, **extra: Any
    ) -> None:
        seen.update(order=order, key=key, n=n, extra=extra)

    when = dt.datetime(2026, 1, 2, tzinfo=UTC)
    key = uuid.uuid4()
    await _roundtrip(JSONCodec(), handler, Order(7, when), key, n=3, tag="x")
    assert seen == {"order": Order(7, when), "key": key, "n": 3, "extra": {"tag": "x"}}


async def test_untyped_decoding_passes_plain_values() -> None:
    seen: list[Any] = []

    async def handler(order: Order) -> None:
        seen.append(order)

    placed = dt.datetime(2026, 1, 1, tzinfo=UTC)
    await _roundtrip(JSONCodec(typed=False), handler, Order(1, placed))
    assert isinstance(seen[0], dict)


async def test_typed_decoding_error_is_raised_to_caller() -> None:
    async def handler(n: int) -> None: ...

    with pytest.raises(msgspec.ValidationError):
        await _roundtrip(JSONCodec(), handler, "not-a-number")


async def test_typed_decoding_pydantic_model() -> None:
    pydantic = pytest.importorskip("pydantic")

    class Customer(pydantic.BaseModel):
        name: str
        age: int

    seen: list[Any] = []

    async def handler(customer: Any) -> None:
        seen.append(customer)

    # ``Customer`` is local, which postponed annotations cannot resolve.
    handler.__annotations__["customer"] = Customer

    await _roundtrip(JSONCodec(), handler, Customer(name="Ada", age=36))
    await _roundtrip(JSONCodec(), handler, name="Bo", age=5)  # whole body
    assert seen == [Customer(name="Ada", age=36), Customer(name="Bo", age=5)]


async def test_unresolvable_annotations_fall_back_to_plain_values() -> None:
    seen: list[Any] = []

    async def handler(order: DoesNotExist) -> None:  # type: ignore[name-defined]  # noqa: F821  # ty: ignore[unresolved-reference]
        seen.append(order)

    await _roundtrip(JSONCodec(), handler, {"id": 1})
    assert seen == [{"id": 1}]


async def test_unannotated_listener_gets_plain_values() -> None:
    seen: list[Any] = []

    async def handler(order, *, n=0) -> None:
        seen.append((order, n))

    await _roundtrip(JSONCodec(), handler, {"id": 1}, n=2)
    assert seen == [({"id": 1}, 2)]


# -- whole-body models ------------------------------------------------------


async def test_whole_body_models() -> None:
    seen: list[Any] = []

    async def on_item(item: Item) -> None:
        seen.append(item)

    async def on_order(order: Order) -> None:
        seen.append(order)

    async def on_totals(totals: Totals) -> None:
        seen.append(totals)

    await _roundtrip(JSONCodec(), on_item, sku="a", qty=2)
    placed = dt.datetime(2026, 1, 1, tzinfo=UTC)
    await _roundtrip(JSONCodec(), on_order, id=1, placed=placed)
    await _roundtrip(JSONCodec(), on_totals, total=9)
    assert seen == [Item("a", 2), Order(1, placed), {"total": 9}]


async def test_whole_body_not_used_when_parameter_is_named() -> None:
    seen: list[Any] = []

    async def on_item(item: Item) -> None:
        seen.append(item)

    await _roundtrip(JSONCodec(), on_item, item={"sku": "a", "qty": 1})
    assert seen == [Item("a", 1)]


async def test_whole_body_skipped_with_var_keyword_or_extra_parameter() -> None:
    seen: list[Any] = []

    async def with_extra(item: Item, **rest: Any) -> None:
        seen.append((item, rest))

    with pytest.raises(TypeError):  # missing ``item``: not a whole-body listener
        await _roundtrip(JSONCodec(), with_extra, sku="a", qty=1)


# -- headers and correlation ids --------------------------------------------


def test_event_headers_validates_and_nests() -> None:
    from litestar_events.context import outgoing_headers

    with event_headers(tenant="a"), event_headers({"x-trace": "t"}, tenant="b"):
        assert outgoing_headers() == {"tenant": "b", "x-trace": "t"}
    assert outgoing_headers() == {}
    with (
        pytest.raises(TypeError, match="string"),
        event_headers(cast("Any", {"bad": 1})),
    ):
        pass


def test_correlation_id_explicit_then_generated() -> None:
    with event_headers(correlation_id="req-1"):
        assert outgoing_correlation_id() == "req-1"
    assert len(outgoing_correlation_id()) == 32


async def test_emit_carries_headers_and_correlation_id_to_listener() -> None:
    seen: list[EventContext] = []
    plain: list[Any] = []

    @listener("placed")
    async def on_placed(order_id: int, ctx: EventContext) -> None:
        seen.append(ctx)
        plain.append(order_id)
        assert current_event() == ctx

    app = Litestar(
        listeners=[on_placed],
        plugins=[EventsPlugin(EventsConfig(backend=MemoryEventEmitter))],
    )
    async with app.event_emitter:
        with event_headers(tenant="acme", correlation_id="req-9"):
            app.emit("placed", 5)
        await asyncio.sleep(0.05)
    assert plain == [5]
    assert seen[0].event_id == "placed"
    assert seen[0].headers == {"tenant": "acme"}
    assert seen[0].correlation_id == "req-9"
    assert current_event() is None


async def test_nested_emit_inherits_correlation_id() -> None:
    seen: list[str | None] = []
    app_box: list[Litestar] = []

    @listener("first")
    async def first(ctx: EventContext) -> None:
        app_box[0].emit("second")

    @listener("second")
    async def second(ctx: EventContext) -> None:
        seen.append(ctx.correlation_id)

    app = Litestar(
        listeners=[first, second],
        plugins=[EventsPlugin(EventsConfig(backend=MemoryEventEmitter))],
    )
    app_box.append(app)
    async with app.event_emitter:
        with event_headers(correlation_id="trace-1"):
            app.emit("first")
        await asyncio.sleep(0.1)
    assert seen == ["trace-1"]


async def test_memory_emitter_end_to_end_typed() -> None:
    seen: list[Order] = []

    @listener("placed")
    async def on_placed(order: Order) -> None:
        seen.append(order)

    app = Litestar(
        listeners=[on_placed],
        plugins=[EventsPlugin(EventsConfig(backend=MemoryEventEmitter))],
    )
    when = dt.datetime(2026, 1, 2, tzinfo=UTC)
    async with app.event_emitter:
        app.emit("placed", Order(1, when))
        app.emit("placed", id=2, placed=when)  # whole body from kwargs
        await asyncio.sleep(0.05)
    assert seen == [Order(1, when), Order(2, when)]


def test_plugin_uses_custom_codec() -> None:
    custom = JSONCodec(typed=True)
    app = Litestar(
        plugins=[EventsPlugin(EventsConfig(backend=MemoryEventEmitter, codec=custom))]
    )
    assert cast("MemoryEventEmitter", app.event_emitter)._codec is custom


def test_codec_protocol_methods_are_abstract() -> None:
    from litestar_events.codec import Codec

    msg = Message(args=(), kwargs={})
    with pytest.raises(NotImplementedError):
        Codec.encode(cast("Any", None), msg)  # pyright: ignore[reportAbstractUsage]
    with pytest.raises(NotImplementedError):
        Codec.decode(cast("Any", None), b"")  # pyright: ignore[reportAbstractUsage]
    with pytest.raises(NotImplementedError):
        Codec.invoke(cast("Any", None), cast("Any", None), "x", msg)  # pyright: ignore[reportAbstractUsage]


def test_pydantic_hooks_absent_without_pydantic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from litestar_events.codec import _pydantic_hooks

    monkeypatch.setitem(sys.modules, "litestar.plugins.pydantic", None)
    assert _pydantic_hooks() == ({}, [])


def test_unwrap_stops_at_non_callable_closure_cell() -> None:
    from litestar_events.codec import _unwrap

    fn = "not callable"

    def wrapper() -> str:
        return fn

    assert _unwrap(wrapper) is wrapper
