"""Example Litestar app using the ZeroMQ event emitter backend.

ZeroMQ is brokerless -- there is nothing to run alongside the app. The emitter
binds a PUB socket and connects a SUB socket back to it, so a single process
talks to itself out of the box:

    uv run litestar --app examples.zmq.main:app run

And trigger an event:

    curl -X POST http://localhost:8000/users \
        -H 'content-type: application/json' \
        -d '{"email": "ada@example.com"}'

The ``user_registered`` event is published over the PUB socket and received by
the SUB socket, which dispatches to both listeners. Delivery is fire-and-forget
and at-most-once: events published before the SUB socket finished connecting
are dropped (the ``subscribe_warmup`` delay mitigates this at startup).

For cross-process fanout, run several instances on distinct ``pub_address``
ports and point each instance's ``connect_addresses`` at every peer.

ZeroMQ is not durable: use the rabbit / sqs / pubsub backends if you need
persistence or guaranteed delivery.
"""

from __future__ import annotations

from functools import partial
from typing import Any

from litestar import Litestar, Request, post
from litestar.events import listener

from litestar_events.contrib.zmq import ZeroMQEventEmitter

USER_REGISTERED = "user_registered"


@listener(USER_REGISTERED)
async def send_welcome_email(*, email: str, **_: Any) -> None:
    print(f"[email] welcome to {email}")


@listener(USER_REGISTERED)
async def record_analytics(*, email: str, **_: Any) -> None:
    print(f"[analytics] new signup: {email}")


@post("/users")
async def register_user(request: Request, data: dict[str, str]) -> dict[str, str]:
    email = data["email"]
    request.app.emit(USER_REGISTERED, email=email)
    return {"status": "queued", "email": email}


app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        ZeroMQEventEmitter,
        pub_address="tcp://127.0.0.1:5557",
        # connect_addresses=["tcp://127.0.0.1:5557", "tcp://10.0.0.2:5557"],
    ),
)
