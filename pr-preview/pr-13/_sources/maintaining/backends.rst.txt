Backend design rules
====================

Rules for adding or changing an emitter in ``contrib/<backend>/``.

What Litestar expects
---------------------

Litestar's own ``SimpleEventEmitter`` uses a single in-process stream for every
event. ``emit(event_id, ...)`` looks up the listeners registered for that id and
puts one work item per listener on that stream. There is no queue per event type:
the event id is a lookup key, not a channel. A listener may subscribe to several
event ids, and one event id may have several listeners.

A distributed backend keeps that contract. Whatever the transport, one consumer
receives an event and runs **every** listener registered for its event id in that
process, isolating failures per listener. So the question below is only about
transport topology; it never changes listener semantics.

One queue for all events, or one per event type?
------------------------------------------------

Both are workable. The rule is: route by event id *inside* one consumer, and do
not create a queue per event type. The rest of this section explains why, and what
the alternative would look like.

Map the event id onto the broker's own routing primitive when it has one:

.. list-table::
   :header-rows: 1

   * - Backend
     - Event id becomes
   * - RabbitMQ
     - A routing key on one topic exchange. One consumer queue is bound once per
       event id.
   * - Kafka, Confluent
     - A topic (``topic_prefix`` + event id).
   * - NATS
     - A subject.
   * - Redis
     - A pub/sub channel.
   * - MQTT
     - A topic.
   * - ZeroMQ
     - The subscription topic, sent as the first message frame.
   * - Postgres
     - A ``LISTEN`` channel.

A managed queue has no such primitive, and creating one queue per event id would
mean provisioning, permissions and cost that grow with the number of events.
For those backends, use **a single managed queue for all events**, and carry the
originating event id in a message attribute. SQS (one queue) and Pub/Sub (one
topic) are the precedents: every event shares it, and the consumer reads the
``event_id`` attribute to pick the listeners. Follow it for any future
queue-style backend, such as Azure Service Bus.

Deliberate consequences:

- Several instances that share one queue are competing consumers, so each event
  is handled once (work-queue semantics). Broadcast fanout needs a fan-out layer
  in front of the queues (for example SNS in front of SQS), which the SQS
  backend does not provide itself. Pub/Sub gives it natively, with one
  subscription per instance.
- The message body is the same envelope for every backend, so the event id is
  *not* part of it. A backend that cannot carry the id as metadata is not a fit
  for the single-queue rule.

How separate queues would work
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A queue per event type (for example RabbitMQ with ``queue_name`` set to the event
id, or one SQS queue per event) would work like this:

- ``emit("order_created")`` publishes to the queue named for that event.
- A worker declares one queue per event id it has listeners for, and runs one
  consumer on each.
- Each event type is then its own work queue, with its own depth, retention,
  dead-letter queue, rate limit and access policy.

That buys real things: an event that is slow or failing cannot delay others
(no head-of-line blocking), each event type can be scaled and monitored on its
own, and permissions can differ per event.

Why it is not the default
~~~~~~~~~~~~~~~~~~~~~~~~~

- **Topology grows with the code.** Every new event id needs a queue created,
  permissioned and monitored. Managed queues (SQS, Service Bus) charge and limit
  per queue. On RabbitMQ and Kafka it is cheap, which is why those backends
  already give each event id its own routing key or topic, and still use one
  consumer.
- **A worker must know every event id up front** to declare its queues. With one
  queue, adding a listener needs no new infrastructure.
- **It does not match Litestar's model.** Listeners are keyed by event id in one
  emitter, so one process consuming everything is the natural shape.
- **It is not needed for isolation in the common case.** A failing listener is
  already caught per listener, so it cannot block its siblings.

When separate queues are right
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Give an event type its own queue when it has a different load or reliability
profile from the rest: a slow batch event next to latency-sensitive ones, an
event that needs its own dead-letter queue or retention, or one that only a
dedicated worker pool may consume. Do this by running a separate emitter
(its own ``queue_name`` or ``queue_url``, and its own listeners) for that event,
not by changing a backend to create queues per event.

Consequences of the single queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **An instance without a listener for an event drops it.** On one shared queue
  every instance can receive every event id. SQS and RabbitMQ acknowledge or
  delete a message that has no matching listener and log it, so if workers differ
  in which listeners they register, an event can be lost. Keep every worker that
  shares a queue on the same listener set, or give each distinct set its own
  queue.
- **Head-of-line blocking.** A slow listener delays the events behind it in the
  same queue. Use the separate-emitter approach above when that matters.
