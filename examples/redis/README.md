# Redis event emitter example

Drop-in `BaseEventEmitterBackend` for Litestar backed by Redis Pub/Sub.

## Run

```bash
# 1. Redis
docker run --rm -p 6379:6379 redis:7

# 2. App
uv run litestar --app examples.redis.main:app run

# 3. Trigger an event
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

Both listeners (`send_welcome_email`, `record_analytics`) react concurrently.

## Inspect

```bash
# What channels is the app currently subscribed to?
redis-cli PUBSUB CHANNELS 'litestar.events:*'

# How many subscribers per channel?
redis-cli PUBSUB NUMSUB litestar.events:user_registered

# Watch events flow in real time
redis-cli PSUBSCRIBE 'litestar.events:*'
```

## Delivery semantics

- **Fire-and-forget.** Events emitted while no subscriber is connected are
  dropped. Redis Pub/Sub has no buffer.
- **Fanout.** Every connected app instance receives every event. There is no
  work-queue / single-consumer mode (use the rabbit backend for that).
- **Per-listener error isolation.** One listener raising does not cancel
  siblings; exceptions are logged.

## When not to use this backend

- You need events to survive app restarts → use [`rabbit`](../rabbit).
- You want only one of N replicas to handle each event → use [`rabbit`](../rabbit)
  with a shared `queue_name`.
- You want broker-backed pub/sub for arbitrary external publishers → use
  [`litestar-channels`](https://docs.litestar.dev/2/usage/channels.html), not
  the event bus.

## Configuration

```python
RedisEventEmitter(
    listeners,
    redis_url="redis://localhost:6379/0",
    channel_prefix="litestar.events:",  # change to namespace per app
)
```
