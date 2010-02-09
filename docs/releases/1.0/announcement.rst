===============================
 Celery 1.0 has been released!
===============================

We're happy to announce the release of Celery 1.0.

Stable API
==========

From this version on the API will be considered stable. This means there won't be any backwards
incompatible changes to new minor versions. Changes to the API needs to be
deprecated; so, for example, if we decided to remove a function that existed in Celery 1.0:

* Celery 1.2 will contain a backwards-compatible replica of the function which
  will raise a ``PendingDeprecationWarning``.
  This warning is silent by default; you need to explicitly turn on display of these warnings.
* Celery 1.4 will contain the backwards-compatible replica, but the warning will be promoted to
  a full-fledged ``DeprecationWarning``. This warning is loud by default, and will likely be
  quite annoying.
* Celery 1.6 will remove the feature outright.

See the `Celery Deprecation Timeline`_ for a list of pending removals.

.. _`Celery Deprecation Timeline`:
    http://ask.github.com/celery/internals/deprecation.html

What's new?
===========

* New periodic task service.

  Periodic tasks are no longer dispatched by ``celeryd``, but by a separate
  service called ``celerybeat``. This is an optimized, centralized service
  dedicated to your periodic tasks, which means you don't have to
  worry about deadlocks or race conditions any more. But, also it means you
  have to make sure only one instance of the service is running at any one
  time.


  **TIP:** If you're only running a single ``celeryd`` server, you can embed
  ``celerybeat`` inside it. Just add the ``--beat`` argument.

* Tasks are automatically registered

    Registering the tasks manually was getting tedious, so now you don't have
    to anymore. However -- You can still do it manually if you need to, just
    disable :attr:`Task.autoregister`.

* Awesome new task decorators

    Write your tasks as regular functions and decorate them.
    There's both :func:`task`, and :func:`periodic_task` decorators.

* Events

    If enabled, the worker is now sending events, telling you what it's
    doing, and wether it's alive or not. This is the basis for the new
    real-time web monitor we're working on.

* Rate limiting

    Global and per task rate limits. 10 tasks a second? or one an hour? You
    got it. It's using the awesome bucket queue algorithm, which is commonly
    used for network traffic shaping. It accounts for bursts of activity, so
    your workers won't be bored by having nothing to do.

* Broadcast commands

    You can now revoke tasks if you suddenly change your mind and don't want to run
    the task anyway, or you can rate limit tasks or shut down the worker remotely.

    It doesn't have many commands yet, but we're waiting for broadcast commands to
    reach its full potential. Maybe you have some ideas of your own?

* Multiple queues

    The worker is now able to receive tasks on multiple queues at once. This
    means you can route tasks to arbitrary workers. Read about the insane
    routing powers of AMQP, and you will surely end up being mighty impressed.

* Platform agnostic message format.

  The message format has been standardized and now uses the ISO-8601 format
  for dates instead of Python datetime objects. This means you can write task
  consumers in other languages than Python (``eceleryd`` anyone?)

* Timely

  Periodic tasks are now scheduled on the clock, i.e. ``timedelta(hours=1)``
  means every hour at :00 minutes, not every hour from the server starts.
  To revert to the previous behaviour you can enable
  :attr:`PeriodicTask.relative`.

* Plus a lot more

To read more about these and other changes in detail, please refer to the `changelog`_.
This document contains crucial information, so if you're upgrading from a previous version of Celery,
be sure to read the entire change set before you continue.

.. _`changelog`: http://ask.github.com/celery/changelog.html

**TIP:** If you install the :mod:`setproctitle` module you can see which task each
worker process is currently executing in ``ps`` listings. Just install it
using pip: ``pip install setproctitle``.
