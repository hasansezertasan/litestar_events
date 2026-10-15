"""One Litestar app, two processes: a web producer and an event worker.

Both processes import this module. The app is configured as a producer, and
``litestar events run`` always starts the same app as a worker, so one image
serves both.

    # RabbitMQ
    docker run --rm -p 5672:5672 rabbitmq:3

    # Web process: publishes only, consumes nothing
    uv run litestar --app examples.producer_worker.app:app run

    # Worker process: consumes only
    uv run litestar --app examples.producer_worker.app:app events run

    curl -X POST http://localhost:8000/orders
"""

from __future__ import annotations

from typing import Any

from litestar import Litestar, Request, post
from litestar.events import listener

from litestar_events.config import EventsConfig
from litestar_events.context import EventContext, event_headers
from litestar_events.contrib.rabbit import RabbitEventEmitter
from litestar_events.plugin import EventsPlugin

ORDER_CREATED = "order_created"


@listener(ORDER_CREATED)
async def send_receipt(order_id: int, ctx: EventContext) -> None:
    print(
        f"[worker] receipt for order {order_id} "
        f"(correlation {ctx.correlation_id}, tenant {ctx.headers.get('tenant')})"
    )


@listener(ORDER_CREATED)
async def log_in_web_process(order_id: int) -> None:
    print(f"[web] order {order_id} created")


@post("/orders")
async def create_order(request: Request[Any, Any, Any]) -> dict[str, int]:
    # Headers and the correlation id travel with the event to the worker.
    # The correlation id is the request's ``x-request-id`` (see the config below).
    with event_headers(tenant="acme"):
        request.app.emit(ORDER_CREATED, order_id=42)
    return {"order_id": 42}


app = Litestar(
    route_handlers=[create_order],
    listeners=[send_receipt, log_in_web_process],
    plugins=[
        EventsPlugin(
            EventsConfig(
                backend=RabbitEventEmitter,
                role="producer",
                # Events emitted while serving a request carry its id.
                request_id_header="x-request-id",
                # Runs in the web process; send_receipt runs on the worker.
                local_listeners=[log_in_web_process],
                # A named queue is durable: events survive a worker restart.
                backend_options={"queue_name": "orders-worker"},
            )
        )
    ],
)
