from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING

from litestar.events import BaseEventEmitterBackend, EventListener
from typing_extensions import Self

from litestar_events._queue import QueuedEmitterMixin, QueuePayload, require

if TYPE_CHECKING:
    from collections.abc import Sequence

    from psycopg_pool import AsyncConnectionPool

__all__ = ["PostgresEventEmitter"]


logger = logging.getLogger(__name__)

_VALID_PG_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _validate_channel(channel: str) -> None:
    if not _VALID_PG_IDENT.match(channel):
        msg = (
            f"Invalid PostgreSQL channel name {channel!r}. "
            "Channel names must match [A-Za-z_][A-Za-z0-9_$]*."
        )
        raise ValueError(msg)
    if len(channel.encode("utf-8")) > 63:
        msg = f"PostgreSQL channel name {channel!r} exceeds 63 bytes."
        raise ValueError(msg)


class PostgresEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by PostgreSQL ``LISTEN/NOTIFY``.

    Delivery semantics:
      - At-most-once, fire-and-forget. Notifications delivered only to
        sessions currently ``LISTEN``-ing. No durability across restarts.
      - Listener exceptions are caught per-listener; siblings still complete.
      - Every app instance receives every event (broadcast/fanout).

    Channel layout:
      - One PostgreSQL channel per registered event id:
        ``f"{channel_prefix}{event_id}"``.
      - Event ids combined with the prefix must be valid PostgreSQL identifiers
        (``[A-Za-z_][A-Za-z0-9_$]*``, <=63 bytes). Validated at ``__aenter__``;
        invalid combinations raise ``ValueError`` immediately.
      - Inspect active channels with ``SELECT pg_listening_channels();`` from
        the consumer connection or ``pg_stat_activity`` for cluster-wide view.

    Payload:
      - JSON-encoded ``{"args": [...], "kwargs": {...}}``.
      - PostgreSQL caps NOTIFY payloads at ~8000 bytes. Large payloads will
        raise from the publisher; either trim what you emit or use the
        rabbit backend.

    Not suitable for:
      - durability across restarts (use the rabbit backend),
      - work-queue / single-consumer semantics (use the rabbit backend with
        a shared ``queue_name``),
      - large payloads (~8 KB hard limit).
    """

    def __init__(
        self,
        listeners: Sequence[EventListener],
        *,
        dsn: str | None = None,
        pool: AsyncConnectionPool | None = None,
        channel_prefix: str = "litestar_events_",
    ) -> None:
        super().__init__(listeners)
        if (dsn is None) == (pool is None):
            msg = "Provide exactly one of `dsn` or `pool`."
            raise ValueError(msg)

        self._dsn = dsn
        self._pool = pool
        self._owns_pool = pool is None
        self._channel_prefix = channel_prefix

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    def _channel(self, event_id: str) -> str:
        return f"{self._channel_prefix}{event_id}"

    async def __aenter__(self) -> Self:
        from psycopg_pool import AsyncConnectionPool

        for event_id in self._by_event:
            try:
                _validate_channel(self._channel(event_id))
            except ValueError as exc:
                msg = (
                    f"event_id {event_id!r} produces an invalid PostgreSQL "
                    f"channel name. {exc}"
                )
                raise ValueError(msg) from exc

        if self._owns_pool:
            # __init__ guarantees exactly one of dsn/pool, so owning the pool
            # means dsn was provided.
            dsn = require(self._dsn, "dsn")
            self._pool = AsyncConnectionPool(
                dsn, kwargs={"autocommit": True}, open=False
            )
            await self._pool.open()

        require(self._pool, "connection pool")
        if self._by_event:
            self._consumer_task = asyncio.create_task(self._consumer_loop())

        self._publish_queue = asyncio.Queue()
        self._publisher_task = asyncio.create_task(self._publisher_loop())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        for task in (self._publisher_task, self._consumer_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._close_queue()
        if self._owns_pool and self._pool is not None:
            await self._pool.close()

    async def _publisher_loop(self) -> None:
        queue = require(self._publish_queue, "publish queue")
        pool = require(self._pool, "connection pool")
        while True:
            event_id, args, kwargs = await queue.get()
            try:
                body = json.dumps({"args": list(args), "kwargs": kwargs})
                async with pool.connection() as conn:
                    await conn.execute(
                        "SELECT pg_notify(%s, %s)", (self._channel(event_id), body)
                    )
            except Exception:
                logger.exception("Failed to publish event %s", event_id)

    async def _consumer_loop(self) -> None:
        from psycopg.sql import SQL, Identifier

        pool = require(self._pool, "connection pool")
        prefix_len = len(self._channel_prefix)
        async with pool.connection() as conn:
            for event_id in self._by_event:
                # LISTEN cannot be parameterized; psycopg.sql.Identifier quotes
                # the channel name safely, and _validate_channel has already
                # rejected anything outside [A-Za-z_][A-Za-z0-9_$]*.
                # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query  # ruff: ignore[commented-out-code]
                await conn.execute(
                    SQL("LISTEN {}").format(Identifier(self._channel(event_id)))
                )

            async for notify in conn.notifies():
                event_id = notify.channel[prefix_len:]
                try:
                    payload = json.loads(notify.payload)
                    args = payload.get("args", [])
                    kwargs = payload.get("kwargs", {})
                except Exception:
                    logger.exception(
                        "Dropping unparseable NOTIFY on channel %s", notify.channel
                    )
                    continue

                listeners = self._by_event.get(event_id, [])
                if not listeners:
                    continue

                async def _run_one(listener: EventListener) -> None:
                    try:
                        await listener.fn(*args, **kwargs)
                    except Exception:
                        logger.exception(
                            "Listener %s failed for event %s",
                            getattr(listener.fn, "__name__", repr(listener.fn)),
                            event_id,
                        )

                await asyncio.gather(
                    *(_run_one(listener) for listener in listeners),
                    return_exceptions=True,
                )
