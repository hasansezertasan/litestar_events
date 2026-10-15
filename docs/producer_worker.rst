Producer and worker processes
=============================

By default an emitter publishes **and** consumes in the same process, so every
web instance runs every listener. To run listeners in a separate process, give
each process a *role*.

Roles
-----

``embedded``
   Publish and consume in this process. The default; existing apps keep
   working unchanged.

``producer``
   Publish only. No broker subscription is made and no consumer task starts.

``worker``
   Consume only. Listeners run here and ``app.emit()`` raises ``RuntimeError``.

The role is set explicitly with ``EventsConfig.role`` (or a backend's ``role=``
argument) and defaults to ``embedded``. The library reads no environment
variables; if you want to choose the role per deployment, read your own
setting and pass it in. ``litestar events run`` and ``drain`` always use
``worker``, so one image serves both processes: the web app is configured as
``producer`` and the worker command overrides it.

Example
-------

Keep Litestar's own ``@listener`` and ``app.emit()``. Both processes load the
same app:

.. literalinclude:: ../examples/producer_worker/app.py
   :language: python
   :caption: examples/producer_worker/app.py

.. code-block:: console

   $ litestar --app app:app events run                              # worker
   $ litestar --app app:app run                                     # web (producer)

Running some listeners in the web process
------------------------------------------

Listeners are placed per process by default, but you can keep chosen ones in
the web process with ``EventsConfig.local_listeners``. Pass the listener
objects themselves (what ``@listener`` returns); each must also be in the
app's ``listeners``, otherwise startup fails.

.. code-block:: python

   EventsConfig(
       backend=RabbitEventEmitter,
       role="producer",
       local_listeners=[update_cache],  # the rest run on the worker
   )

* A ``producer`` calls local listeners directly from ``app.emit()``, in the
  emitting instance only, without the broker. They run as background tasks;
  a failure is logged and never raised into the request.
* A ``worker`` skips them, and does not subscribe to events that only local
  listeners handle.
* An ``embedded`` emitter ignores the setting and runs every listener.
* Local listeners see the same values a worker would: the event is encoded and
  decoded first, so ``typed_decoding`` applies to them too.

The ``litestar events`` commands
--------------------------------

``run``
   Consume events until SIGINT or SIGTERM, then shut down cleanly. Forces the
   ``worker`` role.

``drain [--timeout SECONDS]``
   Consume the backlog, then exit once no event has arrived for the timeout
   (default ``EventsConfig.drain_timeout``). Useful for batch or cron-style
   processing.

``status``
   Connect to the broker and report whether it is reachable. This connects as
   a producer, so for ZeroMQ it cannot detect a missing peer (connections are
   lazy) and may collide with a running app's PUB port on the same host.

``info``
   Print the backend, role, durability and registered events.

``run`` and ``drain`` refuse the in-memory emitter, which is process-local.

Wire format and typed listeners
-------------------------------

Every backend uses one codec, so the producer and the worker always agree on
the bytes. The default :class:`~litestar_events.codec.JSONCodec` sends
``{"v": 1, "args": [...], "kwargs": {...}}``.

*Encoding happens inside* ``app.emit()``: a payload that cannot be serialized
raises ``SerializationException`` at the call site instead of being dropped
later by a background task. ``datetime``, ``UUID``, ``Decimal``, dataclasses
and msgspec/Pydantic models are supported. Messages without ``"v"`` (written
by earlier releases) are still accepted; an unknown version is rejected and
logged.

By default the worker converts each argument to the type annotated on the
listener, as FastStream does: a ``datetime`` annotation receives a
``datetime``, a dataclass, msgspec or Pydantic annotation receives the model.
Unannotated parameters, and listeners whose annotations cannot be resolved
(for example, names imported only under ``TYPE_CHECKING``; a warning is
logged once), receive plain JSON values. Set ``typed_decoding=False`` to get
plain JSON values everywhere, as in earlier releases:

.. code-block:: python

   @listener("order_created")
   async def send_receipt(order: Order, placed: datetime) -> None: ...

   EventsConfig(backend=RabbitEventEmitter)  # typed_decoding=True is the default

A value that does not match its annotation fails that listener only; the
error is logged like any other listener failure. Pass ``EventsConfig(codec=...)``
with your own object implementing ``encode``, ``decode``, ``invoke`` and a
``content_type`` string (see below) to change the format entirely (both processes must use
the same one). The codec's ``content_type`` is also set in each broker's native
metadata where one exists, so consumers outside this library can tell the
format:

.. list-table::
   :header-rows: 1

   * - Backend
     - Where ``content_type`` goes
   * - RabbitMQ
     - the AMQP ``content_type`` property
   * - Kafka, Confluent
     - record header ``content-type``
   * - NATS
     - message header ``Content-Type`` (needs NATS server 2.2 or newer)
   * - SQS, Google Pub/Sub
     - message attribute ``content_type``
   * - Redis, ZeroMQ, PostgreSQL
     - nowhere: these transports have no per-message metadata
   * - MQTT
     - the MQTT 5 ``Content Type`` property, when the emitter is created with
       ``protocol=aiomqtt.ProtocolVersion.V5``; nowhere on 3.1.1

This library does not check the value when consuming: the worker always decodes
with its configured codec.

Event headers and the correlation id are also copied into the same native
metadata, so brokers and tools outside this library can route or trace on them.
The envelope stays the source of truth: workers read both from it, never from
the native copy. A user header named ``content-type`` is dropped in favour of the
codec's.

.. list-table::
   :header-rows: 1

   * - Backend
     - Headers and correlation id
   * - RabbitMQ
     - AMQP ``headers``; correlation id in the ``correlation_id`` property
   * - Kafka, Confluent, NATS
     - record or message headers; correlation id as ``correlation-id``
   * - Google Pub/Sub
     - message attributes; correlation id as ``correlation-id``
   * - SQS
     - correlation id only, as attribute ``correlation_id`` (SQS allows 10
       attributes per message)
   * - MQTT
     - MQTT 5 user properties, on v5 only
   * - Redis, ZeroMQ, PostgreSQL
     - nowhere

Whole-body models
~~~~~~~~~~~~~~~~~

A listener with one parameter annotated as a model (dataclass, msgspec
``Struct``, Pydantic model or ``TypedDict``) can receive the whole keyword
payload:

.. code-block:: python

   @listener("order_created")
   async def on_order(order: Order) -> None: ...

   app.emit("order_created", id=1, total=9)      # builds Order(id=1, total=9)
   app.emit("order_created", Order(id=1, total=9))  # also works

This applies only when the emit has keyword arguments that do not name the
parameter, and the listener has no ``**kwargs`` and no other parameters.

Headers and correlation ids
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every event carries a correlation id (generated if you don't set one) and
optional string headers. They travel in the message envelope, so they work on
every backend; they are *not* native broker headers.

.. code-block:: python

   from litestar_events.context import EventContext, event_headers

   with event_headers(tenant="acme", correlation_id=request_id):
       app.emit("order_created", order_id=1)

   @listener("order_created")
   async def on_order(order_id: int, ctx: EventContext) -> None:
       ctx.headers["tenant"], ctx.correlation_id   # injected by annotation

Inside a listener, ``litestar_events.context.current_event()`` returns the same
context. Events emitted from a listener inherit its correlation id, so one
request can be followed across processes. Headers are not inherited.

Request ids
^^^^^^^^^^^

To use the incoming request's id as the correlation id of the events emitted
while serving it, name the header in the config:

.. code-block:: python

   EventsConfig(backend=RabbitEventEmitter, request_id_header="x-request-id")

The middleware (:class:`~litestar_events.middleware.RequestIdMiddleware`) is
added outermost. A missing id, or one that is not 1-128 characters of
``A-Z a-z 0-9 . _ -``, is replaced by a generated one. The id is echoed on the
response unless ``echo_request_id=False``. Without ``request_id_header`` no
middleware is added.

A custom codec implements ``encode(Message)``, ``decode(data) -> Message`` and
``invoke(fn, event_id, message)``, where
:class:`~litestar_events.codec.Message` holds ``args``, ``kwargs``, ``headers``
and ``correlation_id``.

Delivery guarantees by backend
------------------------------

While no worker is connected, a non-durable backend loses events. ``run`` and
``drain`` print a warning for these.

.. list-table::
   :header-rows: 1

   * - Backend
     - Durable?
   * - Redis (Pub/Sub), ZeroMQ, MQTT (QoS 0), NATS (core), PostgreSQL
       (LISTEN/NOTIFY)
     - No. At-most-once; events emitted while no worker is connected are lost.
   * - RabbitMQ
     - Only with ``queue_name`` set (shared durable queue).
   * - Google Pub/Sub
     - Only with ``subscription_name`` set.
   * - Kafka, Confluent
     - Only with an explicit ``group_id``; the default is a random group per
       process, so a restarted worker starts from ``latest``.
   * - SQS
     - Yes.

Testing
-------

``litestar_events.contrib.memory.MemoryEventEmitter`` dispatches events inside
the process. Use it in tests; it needs no broker.
