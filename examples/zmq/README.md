# ZeroMQ event emitter example

Drop-in `BaseEventEmitterBackend` for Litestar backed by brokerless ZeroMQ
PUB/SUB sockets (via [`pyzmq`](https://pyzmq.readthedocs.io)).

## Run

ZeroMQ has no broker — there is nothing to start alongside the app.

```bash
# 1. App (PUB binds, SUB connects back to it)
uv run litestar --app examples.zmq.main:app run

# 2. Trigger an event
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

Both listeners (`send_welcome_email`, `record_analytics`) react concurrently.

## Delivery semantics

- **Fire-and-forget, at-most-once.** No persistence, no acks, no buffering for
  absent peers. Events published before a SUB socket finishes connecting are
  dropped (ZeroMQ's "slow joiner" problem — `subscribe_warmup` mitigates it at
  startup).
- **Broadcast.** Every connected SUB peer receives every event it subscribed
  to. There is no work-queue mode.
- **Per-listener error isolation.** One listener raising does not cancel
  siblings; exceptions are logged.

## Cross-process fanout

Run each instance with a distinct `pub_address` and point every instance's
`connect_addresses` at all peers:

```python
# instance A
ZeroMQEventEmitter(listeners, pub_address="tcp://10.0.0.1:5557",
                   connect_addresses=["tcp://10.0.0.1:5557", "tcp://10.0.0.2:5557"])
# instance B
ZeroMQEventEmitter(listeners, pub_address="tcp://10.0.0.2:5557",
                   connect_addresses=["tcp://10.0.0.1:5557", "tcp://10.0.0.2:5557"])
```

## When not to use this backend

- You need durability or guaranteed delivery → use [`rabbit`](../rabbit),
  [`sqs`](../sqs), or [`pubsub`](../pubsub).
- You need dynamic peer discovery → ZeroMQ addresses are static configuration.

## Configuration

```python
ZeroMQEventEmitter(
    listeners,
    pub_address="tcp://127.0.0.1:5557",   # where this instance binds PUB
    connect_addresses=None,               # peers' PUB addrs; default [pub_address]
    subscribe_warmup=0.2,                 # seconds to let subscriptions propagate
)
```
