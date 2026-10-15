# Separate producer and worker processes

The web app publishes events; a different process consumes them. Both load the
same Litestar app (`app.py`) and differ only by role: the app
is configured with `role="producer"`, and `events run` always runs as a worker.

```bash
docker run --rm -p 5672:5672 rabbitmq:3

# Terminal 1: worker (consume only)
uv run litestar --app examples.producer_worker.app:app events run

# Terminal 2: web (publish only)
uv run litestar --app examples.producer_worker.app:app run

# Terminal 3
curl -X POST http://localhost:8000/orders -H 'x-request-id: req-1'
```

The worker prints `[worker] receipt for order 42`, and the web process prints
`[web] order 42 created` (`log_in_web_process` is a `local_listeners` entry).

Other commands: `events info`, `events status` (broker reachable?) and
`events drain --timeout 10` (process the backlog, exit once idle).

See the "Producer and worker processes" page of the docs for details.
