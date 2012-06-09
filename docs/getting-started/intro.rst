=============================
 Introduction to Task Queues
=============================

.. contents::
    :local:
    :depth: 1

What are Task Queues?
=====================


Celery Features
===============

- Messaging Transports (Brokers)

Celery requires a message broker to send and receive messages,
but this term has been stretched to include everything from
financial-grade messaging systems to your fridge (no offense)

RabbitMQ, Redis, MongoDB, Amazon SQS, CouchDB, Beanstalk, Apache ZooKeeper,
or Databases (SQLAlchemy/Django ORM).

- HA

Both clients and workers will automatically retry in the event
of connection loss or failure, and some brokers support
HA in way of Master/Master or Master/Slave replication.

- Multiple Serializers

Messages can be serialized using pickle, json, yaml, msgpack or
even custom serializers.  In addition Celery ships with a special
serializer that signs messages using cryptographic hashes.

- Compression

Messages can be compressed using zlib, bzip2 or custom
compression schemes defined by the user.

Worker
------

- Monitoring

Workers emit a stream of monitoring events, that is used
by monitoring tools like `celery events`, `celerymon` and
the Django Admin monitor.  Users can write custom event consumers
to analyze what the workers are doing in real-time.

- Time Limits

Tasks can be enforced a strict time to run, and this can be set as a default
for all tasks, for a specific worker, or individually for each task.

.. sidebar:: Soft, or hard?

    The time limit is set in two values, `soft` and `hard`.
    The soft time limit allows the task to catch an exception
    to clean up before it is killed: the hard timeout is not catchable
    and force terminates the task.

- Autoreloading

While developing the worker can be set to automatically reload
when the source code for a task changes.

- Autoscaling

The worker pool can be dynamically resized based on worker load,
and autoscaling rules can be customized by the user.

- Memory Leak Cleanup

Sometimes tasks contain memory leaks that are out of the
developers control, or the task allocated other resources
that cannot be cleaned up.  In this case the worker supports
a :option:`--maxtasksperchild` argument that defines how
many task a given pool process can execute before it's
replaced by a fresh process.

- User components

Each worker component can be customized, and additional components
can be defined by the user simply by defining a new boot steps
that will be loaded as part of the workers dependency graph.
