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

.. _optimizing-worker-settings:

Worker Settings
===============

.. _optimizing-prefetch-limit:

Prefetch limit
--------------

*Prefetch* is a term inherited from AMQP, and it is often misunderstood.

The prefetch limit is a limit for how many tasks a worker can reserve
in advance.  If this is set to zero, the worker will keep consuming
messages *ad infinitum*, not respecting that there may be other
available worker nodes that may even be able to process them sooner.

In the worker the initial prefetch count is set by multiplying
the :setting:`CELERYD_PREFETCH_MULTIPLIER` setting by the number
of child worker processes.

If you have many expensive tasks with a long duration you would want
the multiplier value to be 1, which means it will only reserve one
unacknowledged task per worker process at a time.

However -- If you have lots of short tasks, and throughput/roundtrip latency
is important to you, then you want this number to be large.  Say 64, or 128
for example, as the worker is able to process a lot more tasks/s if the
messages have already been prefetched in memory.  You may have to experiment
to find the best value.

If you have a combination of both very long and short tasks, then the best
option is to use two worker nodes that is configured individually, and route
the tasks accordingly (see :ref:`guide-routing`).

Scenario 1: Lots of short tasks
===============================

.. code-block:: python

    CELERYD_PREFETCH_MULTIPLIER = 128
    CELERY_DISABLE_RATE_LIMITS = True


Scenario 2: Expensive tasks
===========================

.. code-block:: python

    CELERYD_PREFETCH_MULTIPLIER = 1
