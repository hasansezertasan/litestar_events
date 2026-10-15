"""Wire format and listener-argument binding shared by every backend.

The producer and the worker must agree on the bytes that travel through the
broker. This module is that single definition: :class:`JSONCodec` encodes
``emit(event_id, *args, **kwargs)`` into a versioned JSON envelope and decodes
it again on the worker, optionally converting the decoded values into the
types annotated on the listener's parameters.
"""

from __future__ import annotations

import dataclasses
import functools
import inspect
import logging
import typing
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import msgspec
from litestar.serialization import (
    decode_json,
    default_deserializer,
    encode_json,
    get_serializer,
)

from litestar_events.context import EventContext, use_event

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

logger = logging.getLogger(__name__)

__all__ = ["WIRE_VERSION", "Codec", "JSONCodec", "Message", "listener_name"]


#: Version stamped on every message (``"v"``). Messages without it are
#: version 1 messages written before the field existed. The envelope fields
#: added since (``"cid"``, ``"h"``) are optional, so the version is unchanged.
WIRE_VERSION = 1


@dataclass
class Message:
    """One event as it crosses the broker."""

    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    correlation_id: str | None = None


@runtime_checkable
class Codec(Protocol):
    """How events cross the broker and reach listeners."""

    content_type: str

    def encode(self, message: Message) -> bytes:
        """Serialize an event. Called synchronously from ``emit``."""
        raise NotImplementedError

    def decode(self, data: bytes | str) -> Message:
        """Parse a message back into a :class:`Message`."""
        raise NotImplementedError

    def invoke(
        self, fn: Callable[..., Awaitable[Any]], event_id: str, message: Message
    ) -> Awaitable[Any]:
        """Call a listener with a decoded message."""
        raise NotImplementedError


class JSONCodec:
    """JSON envelope: ``v``, ``cid``, ``h``, ``args`` and ``kwargs`` keys.

    Encoding uses Litestar's serializer, so ``datetime``, ``UUID``, ``Decimal``,
    dataclasses and msgspec/Pydantic models are all supported on the producer.

    Args:
        typed: when ``True`` (default), the worker converts each decoded
            argument to the type annotated on the listener's parameter
            (``order: Order``, ``when: datetime``) before calling it. Unannotated
            parameters, and listeners whose annotations cannot be resolved, get
            plain JSON values. When ``False``, every listener gets plain JSON
            values, as in earlier releases.
        type_encoders: extra Litestar-style type encoders for ``encode``.
        type_decoders: extra ``(predicate, decoder)`` pairs for typed decoding.

    A listener whose single parameter is a model (dataclass, msgspec ``Struct``,
    Pydantic model or ``TypedDict``) can receive the whole keyword payload:
    ``app.emit("order_created", id=1, total=9)`` populates ``order: Order``.
    A parameter annotated :class:`~litestar_events.context.EventContext` is
    injected with the event's id, headers and correlation id.
    """

    content_type = "application/json"
    """Advertised on brokers with a native content-type field (RabbitMQ)."""

    def __init__(
        self,
        *,
        typed: bool = True,
        type_encoders: dict[Any, Callable[[Any], Any]] | None = None,
        type_decoders: Sequence[tuple[Callable[[Any], bool], Callable[[Any, Any], Any]]]
        | None = None,
    ) -> None:
        encoders, decoders = _pydantic_hooks()
        self.typed = typed
        self._serializer = get_serializer({**encoders, **(type_encoders or {})})
        self._decoders = [*(type_decoders or ()), *decoders]

    def encode(self, message: Message) -> bytes:
        envelope: dict[str, Any] = {"v": WIRE_VERSION}
        if message.correlation_id:
            envelope["cid"] = message.correlation_id
        if message.headers:
            envelope["h"] = message.headers
        envelope["args"] = list(message.args)
        envelope["kwargs"] = message.kwargs
        return encode_json(envelope, self._serializer)

    def decode(self, data: bytes | str) -> Message:
        payload = decode_json(data)
        if not isinstance(payload, dict):
            msg = "Event message must be a JSON object"
            raise TypeError(msg)
        version = payload.get("v", WIRE_VERSION)
        if version != WIRE_VERSION:
            msg = f"Unsupported event wire version {version!r}; expected {WIRE_VERSION}"
            raise ValueError(msg)
        return Message(
            args=tuple(payload.get("args", ())),
            kwargs=dict(payload.get("kwargs", {})),
            headers=dict(payload.get("h", {})),
            correlation_id=payload.get("cid"),
        )

    async def invoke(
        self, fn: Callable[..., Awaitable[Any]], event_id: str, message: Message
    ) -> Any:
        context = EventContext(event_id, message.headers, message.correlation_id)
        target = _unwrap(fn)
        hints = _type_hints(target)
        args, kwargs = message.args, dict(message.kwargs)
        context_params = [n for n, hint in hints.items() if hint is EventContext]
        if self.typed:
            args, kwargs = _convert_arguments(
                target, hints, context_params, args, kwargs, decoders=self._decoders
            )
        for name in context_params:
            kwargs[name] = context
        with use_event(context):
            return await fn(*args, **kwargs)


def _pydantic_hooks() -> tuple[dict[Any, Callable[[Any], Any]], list[Any]]:
    """Litestar's Pydantic encoders/decoders, when Pydantic is installed."""
    try:
        from litestar.plugins.pydantic import PydanticInitPlugin
    except ImportError:
        return {}, []
    return PydanticInitPlugin.encoders(), PydanticInitPlugin.decoders()


def _unwrap(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Find the user's function behind Litestar's listener error-handling wrapper.

    ``EventListener`` stores ``wrapped(*args, **kwargs)``, a closure over the
    decorated function, which hides its annotations. Follow ``__wrapped__``
    where it exists, else that closure's ``fn`` cell. If neither is found the
    wrapper is returned and no conversion happens.
    """
    for _ in range(10):
        inner = getattr(fn, "__wrapped__", None)
        if inner is None:
            code = getattr(fn, "__code__", None)
            cells = getattr(fn, "__closure__", None)
            if code is None or not cells or "fn" not in code.co_freevars:
                break
            inner = cells[code.co_freevars.index("fn")].cell_contents
            if not callable(inner):
                break
        fn = inner
    return fn


def listener_name(fn: Callable[..., Any]) -> str:
    """Name of the user's function behind a listener's ``fn``, for messages."""
    target = _unwrap(fn)
    return getattr(target, "__name__", repr(target))


@functools.lru_cache(maxsize=256)
def _type_hints(fn: Callable[..., Any]) -> dict[str, Any]:
    """Resolve ``fn``'s annotations; ``{}`` (with one warning) if they can't be.

    Annotations that only import under ``TYPE_CHECKING`` raise ``NameError``;
    skipping conversion keeps such listeners working as they always did.
    """
    try:
        return typing.get_type_hints(fn)
    except Exception:
        logger.warning(
            "Cannot resolve annotations of listener %s; arguments are passed "
            "unconverted",
            getattr(fn, "__name__", repr(fn)),
            exc_info=True,
        )
        return {}


def _is_model(hint: Any) -> bool:
    return isinstance(hint, type) and (
        dataclasses.is_dataclass(hint)
        or issubclass(hint, msgspec.Struct)
        or hasattr(hint, "model_validate")
        or typing.is_typeddict(hint)
    )


def _convert_arguments(
    target: Callable[..., Any],
    hints: dict[str, Any],
    context_params: list[str],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    decoders: Sequence[tuple[Callable[[Any], bool], Callable[[Any, Any], Any]]],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    signature = inspect.signature(target)
    convert = functools.partial(
        msgspec.convert,
        dec_hook=functools.partial(default_deserializer, type_decoders=decoders),
        strict=False,
    )
    whole = _whole_body_parameter(signature, hints, context_params, args, kwargs)
    if whole is not None:
        return (), {whole: convert(kwargs, hints[whole])}
    bound = signature.bind_partial(*args, **kwargs)
    for name, value in list(bound.arguments.items()):
        kind = signature.parameters[name].kind
        if (
            name in hints
            and name not in context_params
            and kind
            not in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
        ):
            bound.arguments[name] = convert(value, hints[name])
    return bound.args, bound.kwargs


def _whole_body_parameter(
    signature: inspect.Signature,
    hints: dict[str, Any],
    context_params: list[str],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> str | None:
    """Name of the lone model parameter that should receive the whole payload.

    Applies when the listener has one ordinary parameter (besides injected
    context and no ``**kwargs``), it is annotated with a model type, and the
    event was emitted with keyword arguments that do not name that parameter.
    """
    kinds = inspect.Parameter
    params = [
        p
        for p in signature.parameters.values()
        if p.name not in context_params
        and p.kind not in {kinds.VAR_POSITIONAL, kinds.VAR_KEYWORD}
    ]
    has_var_kw = any(p.kind is kinds.VAR_KEYWORD for p in signature.parameters.values())
    if has_var_kw or len(params) != 1 or args or not kwargs:
        return None
    (param,) = params
    if param.name in kwargs or not _is_model(hints.get(param.name)):
        return None
    return param.name
