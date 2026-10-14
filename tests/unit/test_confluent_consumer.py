"""Unit tests for confluent consumer-error classification.

``_consumer_loop`` treats broker errors differently by class: benign
``_PARTITION_EOF`` and other non-fatal errors are skipped (the loop continues),
while a fatal error stops the loop instead of spinning and re-logging every
poll. Driven here without a broker by stubbing the instance's ``_run_consumer``
to feed a scripted sequence of poll results.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from confluent_kafka import KafkaError


class _FakeErr:
    def __init__(self, *, code: int, fatal: bool) -> None:
        self._code = code
        self._fatal = fatal

    def code(self) -> int:
        return self._code

    def fatal(self) -> bool:
        return self._fatal


class _FakeMsg:
    def __init__(self, err: _FakeErr) -> None:
        self._err = err

    def error(self) -> _FakeErr:
        return self._err


def _emitter():
    from litestar_events.contrib.confluent.emitter import ConfluentEventEmitter

    emitter = ConfluentEventEmitter([])
    # require() only checks non-None; poll is never really called because
    # _run_consumer is stubbed below.
    emitter._consumer = cast("Any", SimpleNamespace(poll=lambda *_a: None))
    return emitter


async def test_consumer_stops_on_fatal_error(monkeypatch: pytest.MonkeyPatch) -> None:
    emitter = _emitter()
    fatal = _FakeMsg(_FakeErr(code=KafkaError._ALL_BROKERS_DOWN, fatal=True))
    calls = 0

    async def fake_run_consumer(_fn: Any, *_args: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls > 5:
            msg = "loop failed to break on a fatal error"
            raise AssertionError(msg)
        return fatal

    monkeypatch.setattr(emitter, "_run_consumer", fake_run_consumer)

    await emitter._consumer_loop()

    assert calls == 1  # broke out immediately, did not spin


@pytest.mark.parametrize(
    "code", [KafkaError._PARTITION_EOF, KafkaError._ALL_BROKERS_DOWN]
)
async def test_consumer_continues_on_nonfatal_error(
    monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    emitter = _emitter()
    nonfatal = _FakeMsg(_FakeErr(code=code, fatal=False))
    calls = 0

    async def fake_run_consumer(_fn: Any, *_args: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            return nonfatal
        emitter._stopping = True  # end the loop cleanly on the next poll
        return None

    monkeypatch.setattr(emitter, "_run_consumer", fake_run_consumer)

    await emitter._consumer_loop()

    assert calls == 2  # did NOT break; polled again after the non-fatal error
