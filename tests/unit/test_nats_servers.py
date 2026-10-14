"""Unit test for NATS ``servers`` normalization in ``__aenter__``.

``nats.connect`` accepts ``str | list[str]``; the emitter accepts the broader
``str | Sequence[str]`` and normalizes a non-str sequence (e.g. a tuple) to a
concrete ``list`` before connecting. The integration suite only ever passes a
``str``, so the sequence branch is covered here by stubbing ``nats.connect`` --
no broker required.
"""

from __future__ import annotations

import pytest

import nats


class _FakeClient:
    async def drain(self) -> None:
        pass

    async def close(self) -> None:
        pass


@pytest.fixture
def _captured(monkeypatch: pytest.MonkeyPatch) -> dict:
    captured: dict = {}

    async def fake_connect(*, servers: object) -> _FakeClient:
        captured["servers"] = servers
        return _FakeClient()

    monkeypatch.setattr(nats, "connect", fake_connect)
    return captured


async def test_sequence_servers_normalized_to_list(_captured: dict) -> None:
    from litestar_events.contrib.nats.emitter import NATSEventEmitter

    emitter = NATSEventEmitter(
        [],
        servers=("nats://a:4222", "nats://b:4222"),  # a tuple, not a list
    )
    async with emitter:
        pass

    assert _captured["servers"] == ["nats://a:4222", "nats://b:4222"]
    assert isinstance(_captured["servers"], list)


async def test_str_servers_passed_through(_captured: dict) -> None:
    from litestar_events.contrib.nats.emitter import NATSEventEmitter

    emitter = NATSEventEmitter([], servers="nats://only:4222")
    async with emitter:
        pass

    assert _captured["servers"] == "nats://only:4222"
