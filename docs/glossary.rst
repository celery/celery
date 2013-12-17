.. _glossary:

Glossary
========

.. glossary::
    :sorted:

    acknowledged
        Workers acknowledge messages to signify that a message has been
        handled.  Failing to acknowledge a message
        will cause the message to be redelivered.   Exactly when a
        transaction is considered a failure varies by transport.  In AMQP the
        transaction fails when the connection/channel is closed (or lost),
        but in Redis/SQS the transaction times out after a configurable amount
        of time (the ``visibility_timeout``).

    ack
        Short for :term:`acknowledged`.

    request
        Task messages are converted to *requests* within the worker.
        The request information is also available as the task's
        :term:`context` (the ``task.request`` attribute).

    calling
        Sends a task message so that the task function is
        :term:`executed <executing>` by a worker.

    kombu
        Python messaging library used by Celery to send and receive messages.

    billiard
        Fork of the Python multiprocessing library containing improvements
        required by Celery.

    executing
        Workers *execute* task :term:`requests <request>`.

    apply
        Originally a synonym to :term:`call <calling>` but used to signify
        that a function is executed by the current process.

    context
        The context of a task contains information like the id of the task,
        it's arguments and what queue it was delivered to.
        It can be accessed as the tasks ``request`` attribute.
        See :ref:`task-request-info`

    idempotent
        Idempotence is a mathematical property that describes a function that
        can be called multiple times without changing the result.
        Practically it means that a function can be repeated many times without
        unintented effects, but not necessarily side-effect free in the pure
        sense (compare to :term:`nullipotent`).

    nullipotent
        describes a function that will have the same effect, and give the same
        result, even if called zero or multiple times (side-effect free).
        A stronger version of :term:`idempotent`.

    reentrant
        describes a function that can be interrupted in the middle of
        execution (e.g. by hardware interrupt or signal) and then safely
        called again later.  Reentrancy is not the same as
        :term:`idempotence <idempotent>` as the return value does not have to
        be the same given the same inputs, and a reentrant function may have
        side effects as long as it can be interrupted;  An idempotent function
        is always reentrant, but the reverse may not be true.

    cipater
        Celery release 3.1 named after song by Autechre
        (http://www.youtube.com/watch?v=OHsaqUr_33Y)

    prefetch multiplier
        The :term:`prefetch count` is configured by using the
        :setting:`CELERYD_PREFETCH_MULTIPLIER` setting, which is multiplied
        by the number of pool slots (threads/processes/greenthreads).

    prefetch count
        Maximum number of unacknowledged messages a consumer can hold and if
        exceeded the transport should not deliver any more messages to that
        consumer.  See :ref:`optimizing-prefetch-limit`.
