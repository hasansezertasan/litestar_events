"""Mocked-transport unit tests for the Postgres (LISTEN/NOTIFY) emitter.

No database needed: the ``psycopg_pool`` pool and connections are stubbed
with in-memory fakes covering ``__aenter__`` validation, both pool-ownership
paths, ``__aexit__`` teardown, the publisher loop (success and failure), and
the consumer loop (valid dispatch, unparseable payload, unknown event,
listener isolation).
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
from contextlib import asynccontextmanager, suppress
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest
from dirty_equals import IsStr
from litestar.events import EventListener, listener


class _FakeConnection:
    """Stand-in for a psycopg async connection (LISTEN + NOTIFY + notifies)."""

    def __init__(self, notifies: list[Any] | None = None) -> None:
        self._notifies = list(notifies or [])
        self.executed: list[Any] = []
        self.execute_error: Exception | None = None

    async def execute(self, *args: Any) -> None:
        if self.execute_error is not None:
            raise self.execute_error
        self.executed.append(args)

    async def notifies(self):
        for notify in self._notifies:
            yield notify


class _FakePool:
    """Stand-in for ``psycopg_pool.AsyncConnectionPool``."""

    instances: ClassVar[list[_FakePool]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs
        self.conn = _FakeConnection()
        self.opened = False
        self.closed = False
        _FakePool.instances.append(self)

    async def open(self) -> None:
        self.opened = True

    async def close(self) -> None:
        self.closed = True

    @asynccontextmanager
    async def connection(self):
        yield self.conn


def _patch_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub ``psycopg_pool`` (and ``psycopg.sql``) in ``sys.modules``.

    The real ``psycopg`` package needs a libpq wrapper that is not available
    on every dev machine; the emitter only imports these inside ``__aenter__``
    / ``_consumer_loop`` and hands the composed LISTEN query to a connection
    our fakes ignore, so lightweight stubs keep these tests broker- *and*
    libpq-free.
    """
    _FakePool.instances.clear()
    pool_mod = types.ModuleType("psycopg_pool")
    cast("Any", pool_mod).AsyncConnectionPool = _FakePool
    monkeypatch.setitem(sys.modules, "psycopg_pool", pool_mod)

    class _SQL:
        def __init__(self, query: str) -> None:
            self.query = query

        def format(self, *args: Any) -> str:
            return f"{self.query}:{args!r}"

    def _identifier(name: str) -> str:
        # The fake connection ignores the composed query; only the attribute
        # needs to exist for the emitter's ``Identifier(...)`` call.
        return name

    pg_mod = types.ModuleType("psycopg")
    sql_mod = types.ModuleType("psycopg.sql")
    cast("Any", sql_mod).SQL = _SQL
    cast("Any", sql_mod).Identifier = _identifier
    monkeypatch.setitem(sys.modules, "psycopg", pg_mod)
    monkeypatch.setitem(sys.modules, "psycopg.sql", sql_mod)


def _emitter(*listeners: Any, **kwargs: Any):
    from litestar_events.contrib.postgres.emitter import PostgresEventEmitter

    return PostgresEventEmitter(listeners, **kwargs)


def _raw_listener(event_id: str) -> EventListener:
    """Cover the emitter's per-listener guard with a raising ``fn``.

    Litestar wraps ``fn`` in its own error handler, so a normally-decorated
    failing listener never reaches the emitter's per-listener ``except``
    guard. Overwriting ``.fn`` with the raw coroutine covers that guard (and
    proves sibling isolation at the emitter layer).
    """

    async def _boom(*args: Any, **kwargs: Any) -> None:
        msg = "raw listener boom"
        raise RuntimeError(msg)

    evt_listener = listener(event_id)(_boom)
    evt_listener.fn = _boom
    return evt_listener


def _notify(channel: str, payload: Any) -> Any:
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return SimpleNamespace(channel=channel, payload=body)


async def test_enter_with_dsn_opens_owned_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    @listener("user_registered")
    async def lstn(**_: Any) -> None:
        pass

    _patch_pool(monkeypatch)
    emitter = _emitter(lstn, dsn="postgresql://localhost/db")

    async with emitter:
        assert _FakePool.instances[0].opened is True
        assert emitter._consumer_task is not None
        emitter.emit("user_registered", email="ada@example.com")
        await asyncio.sleep(0.1)

    assert _FakePool.instances[0].closed is True
    assert emitter._publish_queue is None
    kinds = [call[0] for call in _FakePool.instances[0].conn.executed]
    assert any(str(k).startswith("LISTEN") for k in kinds)  # consumer ran LISTEN
    assert (
        "SELECT pg_notify(%s, %s)",
        (
            "litestar_events_user_registered",
            IsStr(
                regex=r'^\{"v":1,"cid":"[0-9a-f]{32}","args":\[\],"kwargs":\{"email":"ada@example.com"\}\}$'
            ),
        ),
    ) in _FakePool.instances[0].conn.executed


async def test_enter_with_external_pool_does_not_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_pool(monkeypatch)
    external = _FakePool()
    _FakePool.instances.clear()  # only the dsn path constructs a pool
    emitter = _emitter(pool=cast("Any", external))

    async with emitter:
        assert emitter._consumer_task is None  # no listeners -> no consumer
        assert _FakePool.instances == []

    assert external.closed is False


async def test_enter_rejects_invalid_channel(monkeypatch: pytest.MonkeyPatch) -> None:
    @listener("bad-channel")
    async def lstn(**_: Any) -> None:
        pass

    _patch_pool(monkeypatch)
    emitter = _emitter(lstn, dsn="postgresql://localhost/db")
    with pytest.raises(ValueError, match="invalid PostgreSQL"):
        async with emitter:
            pass  # pragma: no cover - enter raises before the body


async def test_publisher_loop_logs_failure() -> None:
    from litestar_events.contrib.postgres.emitter import PostgresEventEmitter

    emitter = PostgresEventEmitter([], pool=cast("Any", _FakePool()))
    pool = cast("_FakePool", emitter._pool)
    pool.conn.execute_error = RuntimeError("boom")
    emitter._publish_queue = asyncio.Queue()
    emitter._publish_queue.put_nowait(("evt", b"{}", {}))

    task = asyncio.create_task(emitter._publisher_loop())
    await asyncio.sleep(0.1)
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    assert pool.conn.executed == []


async def test_consumer_loop_full_branch_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_pool(monkeypatch)  # _consumer_loop imports psycopg.sql for LISTEN
    received: list[tuple[Any, ...]] = []

    @listener("evt")
    async def good(*args: Any) -> None:
        received.append(args)

    from litestar_events.contrib.postgres.emitter import PostgresEventEmitter

    emitter = PostgresEventEmitter(
        [good, _raw_listener("evt")], pool=cast("Any", _FakePool())
    )
    pool = cast("_FakePool", emitter._pool)
    pool.conn = _FakeConnection([
        _notify("litestar_events_evt", {"args": [1], "kwargs": {}}),
        _notify("litestar_events_evt", "not-json{{{"),
        _notify("litestar_events_unknown", {"args": [2], "kwargs": {}}),
    ])

    await emitter._consumer_loop()

    assert received == [(1,)]
    # One LISTEN per registered event id.
    assert len(pool.conn.executed) == 1


async def test_exit_without_enter_is_safe() -> None:
    from litestar_events.contrib.postgres.emitter import PostgresEventEmitter

    emitter = PostgresEventEmitter([], pool=cast("Any", _FakePool()))
    await emitter.__aexit__(None, None, None)
    assert emitter._publish_queue is None
