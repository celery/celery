.. _optimizing:

============
 Optimizing
============

Introduction
============

The default configuration, like any good default, is full of compromises.
It is not tweaked to be optimal for any single use case, but tries to
find middle ground that works *well enough* for most situations.

There are key optimizations to be done if your application is mainly
processing lots of short tasks, and also if you have fewer but very
long tasks.

Optimization here does not necessarily mean optimizing for runtime, but also
optimizing resource usage and ensuring responsiveness at times of high load.

Ensuring Operations
===================

In the book `Programming Pearls`_, Jon Bentley presents the concept of
back-of-the-envelope calculations by asking the question;

    ❝ How much water flows out of the Mississippi River in a day? ❞

The point of this exercise[*] is to demonstrate that there is a limit
to how much data a system can process in a timely manner, and teaches
back of the envelope calculations as a means to plan for this ahead of time.

This is very relevant to Celery; If a task takes 10 minutes to complete,
and there are 10 new tasks coming in every minute, then this means
the queue will *never be processed*.  This is why it's very important
that you monitor queue lengths!

One way to do this is by :ref:`using Munin <monitoring-munin>`.
You should set up alerts, so you are notified as soon as any queue has
reached an unacceptable size, this way you can take appropriate action like
adding new worker nodes, or revoking unnecessary tasks.

.. [*] The chapter is available to read for free here:
       `The back of the envelope`_.  This book is a classic text, highly
       recommended.

.. _`Programming Pearls`: http://www.cs.bell-labs.com/cm/cs/pearls/

.. _`The back of the envelope`:
    http://books.google.com/books?id=kse_7qbWbjsC&pg=PA67

.. _optimizing-worker-settings:

Worker Settings
===============

.. _optimizing-prefetch-limit:

Prefetch Limits
---------------

*Prefetch* is a term inherited from AMQP that is often misunderstood
by users.

The prefetch limit is a **limit** for the number of messages (tasks) a worker
can reserve in advance.  If this is set to zero, the worker will keep
consuming messages *ad infinitum*, not respecting that there may be other
available worker nodes that may be able to process them sooner[#],
or that the messages may not even fit in memory.

The workers initial prefetch count is set by multiplying
the :setting:`CELERYD_PREFETCH_MULTIPLIER` setting by the number
of child worker processes[#].  The default is 4 messages per child process.

If you have many expensive tasks with a long duration you would want
the multiplier value to be 1, which means it will only reserve one
unacknowledged task per worker process at a time.

However -- If you have lots of short tasks, and throughput/roundtrip latency
is important to you, then you want this number to be large.  Say 64, or 128
for example, as the worker is able to process a lot more *tasks/s* if the
messages have already been prefetched in memory.  You may have to experiment
to find the best value that works for you.

If you have a combination of both very long and short tasks, then the best
option is to use two worker nodes that is configured individually, and route
the tasks accordingly (see :ref:`guide-routing`).

.. [*] RabbitMQ and other brokers will deliver the messages in round-robin,
       so this doesn't apply to an active system.  But if there is no prefetch
       limit and you restart the cluster, there will be timing delays between
       nodes starting, so if there are 3 offline nodes and one active node,
       then all messages will be delivered to the active node while the others
       are offline.

.. [*] This is the concurrency setting; :setting:`CELERYD_CONCURRENCY` or the
       :option:`-c` option to :program:`celeryd`.


Reserve one task at a time
--------------------------

When using early acknowledgement (default), a prefetch multiplier of 1
means the worker will reserve at most one extra task for every active
worker process.

Often when users ask if it's possible to disable "prefetching of tasks",
what they really want is to have a worker only reserve as many tasks
as there are child processes at a time.

Sadly, this requirement is not possible without enabling late
acknowledgements; A task that has been started, will be
retried if the worker crashes mid execution so the task must be `reentrant`_
(see also notes at :ref:`faq-acks_late-vs-retry`).

.. _`reentrant`: http://en.wikipedia.org/wiki/Reentrant_(subroutine)

You can enable this behavior by using the following configuration:

.. code-block:: python

    CELERY_ACKS_LATE = True
    CELERYD_PREFETCH_MULTIPLIER = 1

.. optimizing-rate-limits:

Rate Limits
-----------

The subsystem responsible for enforcing rate limits introduces extra
complexity, so if you're not using rate limits it may be a good idea to
disable them completely.

Set the :setting:`CELERY_DISABLE_RATE_LIMITS` setting to disable
the rate limit subsystem:

.. code-block:: python

    CELERY_DISABLE_RATE_LIMITS = True
