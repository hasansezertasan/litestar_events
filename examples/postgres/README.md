# PostgreSQL event emitter example

Drop-in `BaseEventEmitterBackend` for Litestar backed by PostgreSQL
`LISTEN/NOTIFY`.

## Run

```bash
# 1. Postgres
docker run --rm -p 5432:5432 \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=events \
    postgres:16

# 2. App
uv run litestar --app examples.postgres.main:app run

# 3. Trigger an event
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

Both listeners (`send_welcome_email`, `record_analytics`) react concurrently.

## Inspect

```bash
# What channels is the consumer connection listening on?
# (run from any psql session connected to the same DB)
psql -c "SELECT pg_listening_channels();"

# Fire a manual NOTIFY from psql to test consumption:
psql -c "SELECT pg_notify(
    'litestar_events_user_registered',
    '{\"args\": [], \"kwargs\": {\"email\": \"manual@example.com\"}}'
);"
```

## Delivery semantics

- **Fire-and-forget.** Events emitted while no subscriber is connected are
  dropped. `LISTEN/NOTIFY` has no buffer; messages live only while a listener
  session is connected.
- **Fanout.** Every connected app instance receives every event. There is no
  work-queue / single-consumer mode (use the rabbit backend for that).
- **Per-listener error isolation.** One listener raising does not cancel
  siblings; exceptions are logged.
- **Payload limit.** PostgreSQL caps NOTIFY payloads at ~8000 bytes. Large
  payloads fail at publish time.

## Event id rules

Postgres channel names must be valid identifiers:
`[A-Za-z_][A-Za-z0-9_$]*`, <=63 bytes after combining with the prefix.

If any registered `@listener` event id fails this check, the emitter raises
`ValueError` at startup naming the offending event. Either rename the event
or pick a different backend.

In practice, snake_case event ids (`user_registered`, `order_placed`,
`password_changed`) — Litestar's canonical style — always pass.

## When not to use this backend

- You need events to survive app restarts → use [`rabbit`](../rabbit).
- You want only one of N replicas to handle each event → use [`rabbit`](../rabbit)
  with a shared `queue_name`.
- Your payloads can exceed 8 KB → use [`rabbit`](../rabbit) or [`redis`](../redis).

## Configuration

```python
# Option A: let the emitter own the pool
PostgresEventEmitter(
    listeners,
    dsn="postgresql://user:pass@host:5432/db",
    channel_prefix="litestar_events_",
)

# Option B: share an existing pool with the rest of your app
from psycopg_pool import AsyncConnectionPool

shared_pool = AsyncConnectionPool(
    "postgresql://user:pass@host:5432/db",
    kwargs={"autocommit": True},   # required for LISTEN/NOTIFY
)
PostgresEventEmitter(listeners, pool=shared_pool)
```
