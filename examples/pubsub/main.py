"""Example Litestar app using the GCP Pub/Sub event emitter backend.

For local development, run the Pub/Sub emulator instead of real GCP:

    docker run --rm -p 8681:8681 messagebird/gcloud-pubsub-emulator:latest

Then start the app (topic + a per-instance subscription are created on startup):

    uv run litestar --app examples.pubsub.main:app run

And trigger an event:

    curl -X POST http://localhost:8000/users \
        -H 'content-type: application/json' \
        -d '{"email": "ada@example.com"}'

The ``user_registered`` event is published to a Pub/Sub topic and delivered to
this instance's subscription, which dispatches to both listeners. Delivery is
durable and at-least-once.

Against real GCP, drop ``emulator_host`` and rely on the standard credential
chain (``GOOGLE_APPLICATION_CREDENTIALS`` or the attached service account):

    event_emitter_backend=partial(
        PubSubEventEmitter,
        project_id="my-gcp-project",
        topic_id="litestar-events",
    )
"""

from __future__ import annotations

from functools import partial
from typing import Any

from litestar import Litestar, Request, post
from litestar.events import listener

from litestar_events.contrib.pubsub import PubSubEventEmitter

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
        PubSubEventEmitter,
        project_id="local-project",
        topic_id="litestar-events",
        emulator_host="localhost:8681",  # remove for real GCP
        # subscription_name="my-app",     # set for work-queue across replicas
    ),
)
