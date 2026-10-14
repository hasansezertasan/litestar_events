"""Unit test for the MQTT emitter's client-CM guard in ``__aenter__``.

``aiomqtt.Client`` is an async context manager whose ``__aenter__`` returns the
live client. The emitter guards (rather than ``assert``s) that contract so a
``None`` return surfaces as a clear ``RuntimeError`` instead of an opaque
``AttributeError`` on the first subscribe/publish -- and the guard survives
``python -O``. Covered by stubbing ``aiomqtt.Client`` with a CM that yields
``None``; no broker required.
"""

from __future__ import annotations

import aiomqtt
import pytest


class _NullClientCM:
    """Stands in for ``aiomqtt.Client``: a CM whose ``__aenter__`` yields None."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        pass

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc: object) -> None:
        return None


async def test_none_client_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from litestar_events.contrib.mqtt.emitter import MQTTEventEmitter

    monkeypatch.setattr(aiomqtt, "Client", _NullClientCM)
    emitter = MQTTEventEmitter([])
    with pytest.raises(RuntimeError, match="returned no client"):
        async with emitter:
            pass
