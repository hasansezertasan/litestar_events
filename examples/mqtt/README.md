# MQTT event emitter example

Drop-in `BaseEventEmitterBackend` for Litestar backed by MQTT (`aiomqtt`).

## Run

```bash
# 1. MQTT broker (mosquitto, anonymous auth)
docker run --rm -p 1883:1883 eclipse-mosquitto:2 \
    sh -c 'echo "listener 1883
allow_anonymous true" > /m.conf && mosquitto -c /m.conf'

# 2. App
uv run litestar --app examples.mqtt.main:app run

# 3. Trigger an event
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

## Inspect

```bash
# Tail every event the app might emit
mosquitto_sub -h localhost -t 'litestar/events/#'

# Tail only one event
mosquitto_sub -h localhost -t 'litestar/events/user_registered'

# Fire a manual publish
mosquitto_pub -h localhost \
    -t 'litestar/events/user_registered' \
    -m '{"args": [], "kwargs": {"email": "manual@example.com"}}'
```

## Delivery semantics

- **QoS 0 by default** — at-most-once, fire-and-forget.
- **QoS 1** — at-least-once. Set `qos=1` for delivery acks at the cost of
  throughput.
- **QoS 2** — exactly-once. Set `qos=2` for the strongest MQTT guarantee
  (slowest).
- **Broadcast.** Every connected app instance receives every event. There
  is no work-queue / single-consumer mode in core MQTT (use the rabbit
  backend for that).
- **Per-listener error isolation.** One listener raising does not cancel
  siblings; exceptions are logged.

## Event id rules

MQTT topics are slash-separated. Event ids must not contain `+` or `#`
(those are MQTT wildcards) or NUL bytes. Validated at startup.

In practice, snake_case event ids (`user_registered`, `order_placed`,
`password_changed`) — Litestar's canonical style — always pass.

## When not to use this backend

- You need events to survive app restarts → use [`rabbit`](../rabbit).
  (MQTT retained messages are not a replacement for durable queues.)
- You want only one of N replicas to handle each event → use
  [`rabbit`](../rabbit) with a shared `queue_name`.

## Configuration

```python
MQTTEventEmitter(
    listeners,
    hostname="localhost",
    port=1883,
    username="user",            # optional
    password="pass",            # optional
    topic_prefix="litestar/events/",
    qos=0,                      # 0, 1, or 2
    client_id="my-app-1",       # optional, useful with persistent sessions
)
```
