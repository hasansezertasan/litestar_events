# RabbitMQ event emitter example

Drop-in `BaseEventEmitterBackend` for Litestar backed by RabbitMQ via
`aio-pika`. Of all the backends in this project, this is the one with the
strongest delivery guarantees out of the box: at-least-once with
per-listener error isolation and a dead-letter exchange for poison
messages.

## Run

```bash
# 1. RabbitMQ (with management UI on :15672)
docker run --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# 2. App
uv run litestar --app examples.rabbit.main:app run

# 3. Trigger an event
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

Both listeners (`send_welcome_email`, `record_analytics`) react concurrently.

## Inspect

The RabbitMQ management UI at <http://localhost:15672> (guest/guest) shows:

- the `litestar.events` topic exchange and its bindings (one routing key
  per registered event id),
- the consumer queue (auto-named when `queue_name=None`, durable when
  you set it),
- the `litestar.events.dlx` dead-letter exchange and its `litestar.events.dead`
  queue,
- live message rates per queue.

CLI alternatives:

```bash
# List exchanges
docker exec -it <container> rabbitmqctl list_exchanges

# List queues with message counts
docker exec -it <container> rabbitmqctl list_queues name messages consumers

# Tail messages (requires the rabbitmq tracing plugin or rabbitmqadmin)
docker exec -it <container> rabbitmqadmin get queue=<name> count=10
```

## Delivery semantics

- **At-least-once.** Messages are acked only after every matching listener
  has run. If the app crashes mid-dispatch, the broker redelivers the
  message on reconnect.
- **Per-listener error isolation.** One listener raising does not cancel
  siblings; the message is still acked once all listeners have either
  succeeded or failed (failures are logged).
- **Poison-message handling.** Messages whose body fails to parse are
  rejected to the dead-letter exchange (`litestar.events.dlx`) instead of
  being requeued in a tight loop.
- **Durable topology.** The topic exchange and dead-letter queue are
  declared durable; they survive broker restarts.

## Broadcast vs work-queue

This is the one knob that fundamentally changes the topology:

```python
# Default: queue_name=None  → server-named exclusive queue
# Every app replica gets its own copy of every event (broadcast/fanout).
RabbitEventEmitter(listeners, amqp_url=...)

# queue_name="my-app"       → durable shared queue
# Replicas compete for messages — exactly one handles each event
# (work-queue / competing consumers).
RabbitEventEmitter(listeners, amqp_url=..., queue_name="my-app")
```

The durable shared queue also gives you durability across replica restarts:
events emitted while every replica is down accumulate in the queue and are
delivered when one comes back online.

## Configuration

```python
RabbitEventEmitter(
    listeners,
    amqp_url="amqp://guest:guest@localhost/",
    exchange_name="litestar.events",      # topic exchange name
    queue_name=None,                       # None = broadcast; str = work-queue
    dlx_name="litestar.events.dlx",        # dead-letter exchange
    prefetch=32,                           # consumer QoS prefetch count
)
```

## When to pick this backend over the others

- You need **durability**: events must survive a worker crash or restart.
- You need **work-queue semantics**: exactly one of N replicas should
  handle each event.
- You need **poison-message handling**: bad payloads should go somewhere
  observable rather than crash a consumer loop.

If none of those apply, [`redis`](../redis), [`nats`](../nats),
[`postgres`](../postgres), or [`mqtt`](../mqtt) are all simpler choices for
in-process-plus-fanout side effects.
