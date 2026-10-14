"""Example Litestar app using the AWS SQS event emitter backend.

For local development you can point the backend at LocalStack instead of real
AWS:

    docker run --rm -p 4566:4566 localstack/localstack:3

Then start the app (the backend auto-creates the queue on startup):

    uv run litestar --app examples.sqs.main:app run

And trigger an event:

    curl -X POST http://localhost:8000/users \
        -H 'content-type: application/json' \
        -d '{"email": "ada@example.com"}'

The ``user_registered`` event is sent to SQS and picked up by the consumer
loop, which dispatches to both listeners below. SQS delivery is durable and
at-least-once: a message is deleted only after its listeners run.

Against real AWS, drop the ``endpoint_url`` / credential overrides and rely on
the standard boto credential chain (env vars, instance role, etc.):

    event_emitter_backend=partial(
        SQSEventEmitter,
        queue_name="litestar-events",
        region_name="eu-central-1",
    )
"""

from __future__ import annotations

from functools import partial
from typing import Any

from litestar import Litestar, Request, post
from litestar.events import listener

from litestar_events.contrib.sqs import SQSEventEmitter

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
        SQSEventEmitter,
        queue_name="litestar-events",
        # LocalStack defaults; remove these for real AWS.
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ),
)
