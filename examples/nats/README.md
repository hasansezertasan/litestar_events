# NATS event emitter example

Drop-in `BaseEventEmitterBackend` for Litestar backed by core NATS pub/sub.

## Run

```bash
# 1. NATS
docker run --rm -p 4222:4222 nats:2

# 2. App
uv run litestar --app examples.nats.main:app run

# 3. Trigger an event
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

Both listeners (`send_welcome_email`, `record_analytics`) react concurrently.

## Inspect

```bash
# Tail every event the app might emit (wildcard at the end)
nats sub 'litestar.events.>'

# Tail only one specific event
nats sub 'litestar.events.user_registered'

# Fire a manual publish to test consumption
nats pub litestar.events.user_registered \
    '{"args": [], "kwargs": {"email": "manual@example.com"}}'
```

## Delivery semantics

- **Fire-and-forget.** Events published while no subscriber is connected are
  dropped. Core NATS has no buffer.
- **Fanout.** Every connected app instance receives every event. There is no
  work-queue / single-consumer mode (use NATS queue groups, or the rabbit
  backend with a shared `queue_name`).
- **Per-listener error isolation.** One listener raising does not cancel
  siblings; exceptions are logged.

## Event id rules

NATS subjects are dot-separated tokens. Each token (between dots) must be
non-empty and must not contain whitespace, `*`, or `>` (those are reserved
as NATS wildcards).

If any registered `@listener` event id fails this check, the emitter raises
`ValueError` at startup naming the offending event.

In practice, snake_case event ids (`user_registered`, `order_placed`,
`password_changed`) — Litestar's canonical style — always pass. Dotted event
ids like `user.registered` also work and become multi-token subjects under
the prefix.

## When not to use this backend

- You need events to survive app restarts → use [`rabbit`](../rabbit), or
  upgrade to NATS JetStream (out of scope for this backend).
- You want only one of N replicas to handle each event → use
  [`rabbit`](../rabbit) with a shared `queue_name`, or add NATS queue groups
  in a future revision.

## Configuration

```python
NATSEventEmitter(
    listeners,
    servers="nats://localhost:4222",            # or list of URLs for a cluster
    subject_prefix="litestar.events.",          # change to namespace per app
)
```
