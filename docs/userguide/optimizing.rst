.. _guide-optimizing:

============
 Optimizing
============

Introduction
============
The default configuration makes a lot of compromises.  It's not optimal for
any single case, but works well enough for most situations.

There are optimizations that can be applied based on specific use cases.

Optimizations can apply to different properties of the running environment,
be it the time tasks take to execute, the amount of memory used, or
responsiveness at times of high load.

Ensuring Operations
===================

In the book `Programming Pearls`_, Jon Bentley presents the concept of
back-of-the-envelope calculations by asking the question;

    ❝ How much water flows out of the Mississippi River in a day? ❞

The point of this exercise [*]_ is to show that there is a limit
to how much data a system can process in a timely manner.
Back of the envelope calculations can be used as a means to plan for this
ahead of time.

In Celery; If a task takes 10 minutes to complete,
and there are 10 new tasks coming in every minute, the queue will never
be empty.  This is why it's very important
that you monitor queue lengths!

A way to do this is by :ref:`using Munin <monitoring-munin>`.
You should set up alerts, that will notify you as soon as any queue has
reached an unacceptable size.  This way you can take appropriate action
like adding new worker nodes, or revoking unnecessary tasks.

.. [*] The chapter is available to read for free here:
       `The back of the envelope`_.  The book is a classic text. Highly
       recommended.

.. _`Programming Pearls`: http://www.cs.bell-labs.com/cm/cs/pearls/

.. _`The back of the envelope`:
    http://books.google.com/books?id=kse_7qbWbjsC&pg=PA67

.. _optimizing-general-settings:

General Settings
================

.. _optimizing-librabbitmq:

librabbitmq
-----------

If you're using RabbitMQ (AMQP) as the broker then you can install the
:mod:`librabbitmq` module to use an optimized client written in C:

.. code-block:: console

    $ pip install librabbitmq

The 'amqp' transport will automatically use the librabbitmq module if it's
installed, or you can also specify the transport you want directly by using
the ``pyamqp://`` or ``librabbitmq://`` prefixes.

.. _optimizing-connection-pools:

Broker Connection Pools
-----------------------

The broker connection pool is enabled by default since version 2.5.

You can tweak the :setting:`BROKER_POOL_LIMIT` setting to minimize
contention, and the value should be based on the number of
active threads/greenthreads using broker connections.

.. _optimizing-transient-queues:

Using Transient Queues
----------------------

Queues created by Celery are persistent by default.  This means that
the broker will write messages to disk to ensure that the tasks will
be executed even if the broker is restarted.

But in some cases it's fine that the message is lost, so not all tasks
require durability.  You can create a *transient* queue for these tasks
to improve performance:

.. code-block:: python

    from kombu import Exchange, Queue

    CELERY_QUEUES = (
        Queue('celery', routing_key='celery'),
        Queue('transient', Exchange('transient', delivery_mode=1),
              routing_key='transient', durable=False),
    )


or by using :setting:`CELERY_ROUTES`:

.. code-block:: python

    CELERY_ROUTES = {
        'proj.tasks.add': {'queue': 'celery', 'delivery_mode': 'transient'}
    }


The ``delivery_mode`` changes how the messages to this queue are delivered.
A value of 1 means that the message will not be written to disk, and a value
of 2 (default) means that the message can be written to disk.

To direct a task to your new transient queue you can specify the queue
argument (or use the :setting:`CELERY_ROUTES` setting):

.. code-block:: python

    task.apply_async(args, queue='transient')

For more information see the :ref:`routing guide <guide-routing>`.

.. _optimizing-worker-settings:

Worker Settings
===============

.. _optimizing-prefetch-limit:

Prefetch Limits
---------------

*Prefetch* is a term inherited from AMQP that is often misunderstood
by users.

The prefetch limit is a **limit** for the number of tasks (messages) a worker
can reserve for itself.  If it is zero, the worker will keep
consuming messages, not respecting that there may be other
available worker nodes that may be able to process them sooner [*]_,
or that the messages may not even fit in memory.

The workers' default prefetch count is the
:setting:`CELERYD_PREFETCH_MULTIPLIER` setting multiplied by the number
of concurrency slots[*]_ (processes/threads/greenthreads).

If you have many tasks with a long duration you want
the multiplier value to be 1, which means it will only reserve one
task per worker process at a time.

However -- If you have many short-running tasks, and throughput/round trip
latency is important to you, this number should be large. The worker is
able to process more tasks per second if the messages have already been
prefetched, and is available in memory.  You may have to experiment to find
the best value that works for you.  Values like 50 or 150 might make sense in
these circumstances. Say 64, or 128.

If you have a combination of long- and short-running tasks, the best option
is to use two worker nodes that are configured separately, and route
the tasks according to the run-time. (see :ref:`guide-routing`).

.. [*] RabbitMQ and other brokers deliver messages round-robin,
       so this doesn't apply to an active system.  If there is no prefetch
       limit and you restart the cluster, there will be timing delays between
       nodes starting. If there are 3 offline nodes and one active node,
       all messages will be delivered to the active node.

.. [*] This is the concurrency setting; :setting:`CELERYD_CONCURRENCY` or the
       :option:`-c` option to the :program:`celery worker` program.


Reserve one task at a time
--------------------------

When using early acknowledgement (default), a prefetch multiplier of 1
means the worker will reserve at most one extra task for every active
worker process.

When users ask if it's possible to disable "prefetching of tasks", often
what they really want is to have a worker only reserve as many tasks as there
are child processes.

But this is not possible without enabling late acknowledgements
acknowledgements; A task that has been started, will be
retried if the worker crashes mid execution so the task must be `idempotent`_
(see also notes at :ref:`faq-acks_late-vs-retry`).

.. _`idempotent`: http://en.wikipedia.org/wiki/Idempotent

You can enable this behavior by using the following configuration options:

.. code-block:: python

    CELERY_ACKS_LATE = True
    CELERYD_PREFETCH_MULTIPLIER = 1

.. _prefork-pool-prefetch:

Prefork pool prefetch settings
------------------------------

The prefork pool will asynchronously send as many tasks to the processes
as it can and this means that the processes are, in effect, prefetching
tasks.

This benefits performance but it also means that tasks may be stuck
waiting for long running tasks to complete::

    -> send T1 to Process A
    # A executes T1
    -> send T2 to Process B
    # B executes T2
    <- T2 complete

    -> send T3 to Process A
    # A still executing T1, T3 stuck in local buffer and
    # will not start until T1 returns

The worker will send tasks to the process as long as the pipe buffer is
writable.  The pipe buffer size varies based on the operating system: some may
have a buffer as small as 64kb but on recent Linux versions the buffer
size is 1MB (can only be changed system wide).

You can disable this prefetching behavior by enabling the :option:`-Ofair`
worker option:

.. code-block:: console

    $ celery -A proj worker -l info -Ofair

With this option enabled the worker will only write to processes that are
available for work, disabling the prefetch behavior.
