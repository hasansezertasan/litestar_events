from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from contextlib import AsyncExitStack, suppress
from typing import TYPE_CHECKING, Any

from litestar.events import BaseEventEmitterBackend, EventListener
from typing_extensions import Self

from litestar_events._queue import QueuedEmitterMixin, QueuePayload, require

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_events.codec import Codec
    from litestar_events.config import EventsRole

__all__ = ["SQSEventEmitter"]


logger = logging.getLogger(__name__)


class SQSEventEmitter(QueuedEmitterMixin, BaseEventEmitterBackend):
    """A Litestar event emitter backend backed by AWS SQS via aiobotocore.

    Delivery semantics:
      - At-least-once + durable. A message is deleted (acked) only after all
        matching in-process listeners run, so a crash mid-dispatch leaves the
        message to reappear after its visibility timeout.
      - Listener exceptions are caught per-listener; siblings still complete.
      - Unparseable messages are deleted with a logged error. Configure a
        redrive policy (DLQ) on the queue if you need to retain poison
        messages instead of dropping them.

    Topology:
      - A single SQS queue carries every event id. The originating ``event_id``
        travels in the ``event_id`` message attribute; the body is the same
        ``{"args": [...], "kwargs": {...}}`` JSON contract used by the other
        backends.
      - ``queue_url`` set: use that queue directly.
      - ``queue_url=None`` (default): resolve ``queue_name`` via ``GetQueueUrl``,
        creating it when ``create_queue=True`` (handy for dev / LocalStack).

    Cross-process behavior:
      - SQS is a point-to-point queue. Multiple app instances sharing one queue
        are *competing consumers* -> each event is handled by exactly one
        instance (work-queue semantics, like the rabbit backend with a shared
        ``queue_name``).
      - For broadcast fanout (every instance sees every event), front the
        queue(s) with an SNS topic (SNS -> per-instance SQS subscription). That
        is intentionally out of scope for this backend.

    Credentials:
      - By default the standard boto credential chain is used (env vars, shared
        config, instance role, etc.). ``aws_access_key_id`` /
        ``aws_secret_access_key`` / ``endpoint_url`` overrides exist mainly for
        LocalStack and tests.
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-locals  # one keyword knob per SQS feature; keyword-only past `listeners`
        self,
        listeners: Sequence[EventListener],
        *,
        role: EventsRole | str | None = None,
        codec: Codec | None = None,
        queue_url: str | None = None,
        queue_name: str = "litestar-events",
        region_name: str = "us-east-1",
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        create_queue: bool = True,
        wait_time_seconds: int = 20,
        max_messages: int = 10,
        visibility_timeout: int = 30,
    ) -> None:
        super().__init__(listeners)
        self._init_role(role)
        self._init_codec(codec)
        self._queue_url = queue_url
        self._queue_name = queue_name
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._create_queue = create_queue
        self._wait_time_seconds = wait_time_seconds
        self._max_messages = max_messages
        self._visibility_timeout = visibility_timeout

        self._by_event: dict[str, list[EventListener]] = defaultdict(list)
        for listener in listeners:
            for event_id in listener.event_ids:
                self._by_event[event_id].append(listener)

        self._stack: AsyncExitStack | None = None
        self._pub_client: Any = None
        self._sub_client: Any = None
        self._publish_queue: asyncio.Queue[QueuePayload] | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    def _create_client(self, session: Any) -> Any:
        return session.create_client(
            "sqs",
            region_name=self._region_name,
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        )

    async def _resolve_queue_url(self) -> str:
        try:
            resp = await self._pub_client.get_queue_url(QueueName=self._queue_name)
            return resp["QueueUrl"]
        except self._pub_client.exceptions.QueueDoesNotExist:
            if not self._create_queue:
                raise
            resp = await self._pub_client.create_queue(QueueName=self._queue_name)
            return resp["QueueUrl"]

    async def __aenter__(self) -> Self:
        from aiobotocore.session import get_session

        self._stack = AsyncExitStack()
        session = get_session()
        # Separate clients for publishing and the long-poll consumer so a
        # blocking ReceiveMessage never starves SendMessage.
        self._pub_client = await self._stack.enter_async_context(
            self._create_client(session)
        )
        self._sub_client = await self._stack.enter_async_context(
            self._create_client(session)
        )

        if self._queue_url is None:
            self._queue_url = await self._resolve_queue_url()

        if self._publishes:
            self._publish_queue = asyncio.Queue()
            self._publisher_task = asyncio.create_task(self._publisher_loop())
        if self._by_event and self._consumes:
            self._consumer_task = asyncio.create_task(self._consumer_loop())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        for task in (self._publisher_task, self._consumer_task):
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        self._close_queue()
        if self._stack is not None:
            await self._stack.aclose()

    async def _publisher_loop(self) -> None:
        queue = require(self._publish_queue, "publish queue")
        client: Any = require(self._pub_client, "SQS publish client")
        queue_url = require(self._queue_url, "SQS queue URL")
        while True:
            event_id, body = await queue.get()
            try:
                await client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=body.decode(),
                    MessageAttributes={
                        "event_id": {"DataType": "String", "StringValue": event_id},
                        "content_type": {
                            "DataType": "String",
                            "StringValue": self._codec.content_type,
                        },
                    },
                )
            except Exception:
                logger.exception("Failed to publish event %s", event_id)

    async def _consumer_loop(self) -> None:
        client: Any = require(self._sub_client, "SQS consume client")
        queue_url = require(self._queue_url, "SQS queue URL")
        while True:
            try:
                resp = await client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=self._max_messages,
                    WaitTimeSeconds=self._wait_time_seconds,
                    VisibilityTimeout=self._visibility_timeout,
                    MessageAttributeNames=["All"],
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("SQS receive failed; backing off")
                await asyncio.sleep(1)
                continue
            for message in resp.get("Messages", []):
                await self._handle_message(message)

    async def _handle_message(self, message: dict[str, Any]) -> None:
        receipt = message["ReceiptHandle"]
        attrs: dict[str, Any] = message.get("MessageAttributes") or {}
        event_id = (attrs.get("event_id") or {}).get("StringValue", "")
        try:
            event = self._codec.decode(message["Body"])
        except Exception:
            logger.exception(
                "Dropping unparseable SQS message "
                "(configure a redrive DLQ to retain it instead)"
            )
            await self._delete(receipt)
            return

        listeners = self._by_event.get(event_id, [])
        if not listeners:
            logger.info("No listeners for event %s; deleting and dropping", event_id)
            await self._delete(receipt)
            return

        async def _run_one(listener: EventListener) -> None:
            try:
                await self._codec.invoke(listener.fn, event_id, event)
            except Exception:
                logger.exception(
                    "Listener %s failed for event %s",
                    getattr(listener.fn, "__name__", repr(listener.fn)),
                    event_id,
                )

        await asyncio.gather(
            *(_run_one(listener) for listener in listeners), return_exceptions=True
        )
        await self._delete(receipt)

    async def _delete(self, receipt: str) -> None:
        client: Any = require(self._sub_client, "SQS consume client")
        queue_url = require(self._queue_url, "SQS queue URL")
        try:
            await client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
        except Exception:
            logger.exception("Failed to delete SQS message; it will be redelivered")
