===============================
 Celery 1.0 has been released!
===============================

We're happy to announce the release of Celery 1.0.

What is it?
===========

Celery is a task queue/job queue based on distributed message passing.
It is focused on real-time operation, but supports scheduling as well.

The execution units, called tasks, are executed concurrently on one or
more worker servers. Tasks can execute asynchronously (in the background) or synchronously
(wait until ready).

Celery is already used in production to process millions of tasks a day.

Celery was originally created for use with Django, but is now usable
from any Python project. It can
also operate with other languages via webhooks.

The recommended message broker is `RabbitMQ`_, but support for Redis and
databases is also available.

.. _`RabbitMQ`: http://rabbitmq.org

Features
--------

See http://ask.github.com/celery/getting-started/introduction.html#features

Stable API
==========

From this version on the public API is considered stable. This means there won't
be any backwards incompatible changes in new minor versions. Changes to the
API will be deprecated; so, for example, if we decided to remove a function
that existed in Celery 1.0:

* Celery 1.2 will contain a backwards-compatible replica of the function which
  will raise a `PendingDeprecationWarning`.
  This warning is silent by default; you need to explicitly turn on display
  of these warnings.
* Celery 1.4 will contain the backwards-compatible replica, but the warning
  will be promoted to a full-fledged `DeprecationWarning`. This warning
  is loud by default, and will likely be quite annoying.
* Celery 1.6 will remove the feature outright.

See the `Celery Deprecation Timeline`_ for a list of pending removals.

.. _`Celery Deprecation Timeline`:
    http://ask.github.com/celery/internals/deprecation.html

What's new?
===========

* Task decorators

    Write tasks as regular functions and decorate them.
    There are both :func:`task`, and :func:`periodic_task` decorators.

* Tasks are automatically registered

    Registering the tasks manually was getting tedious, so now you don't have
    to anymore. You can still do it manually if you need to, just
    disable :attr:`Task.autoregister`. The concept of abstract task classes
    has also been introduced, this is like django models, where only the
    subclasses of an abstract task is registered.

* Events

    If enabled, the worker will send events, telling you what tasks it
    executes, their results, and how long it took to execute them. It also
    sends out heartbeats, so listeners are able to detect nonfunctional
    workers. This is the basis for the new real-time web monitor we're working on
    (`celerymon`_).

.. _`celerymon`: http://github.com/ask/celerymon/

* Rate limiting

    Global and per task rate limits. 10 tasks a second? or one an hour? You
    decide. It's using the awesome `token bucket algorithm`_, which is
    commonly used for network traffic shaping. It accounts for bursts of
    activity, so your workers won't be bored by having nothing to do.

.. _`token bucket algorithm`: http://en.wikipedia.org/wiki/Token_bucket

* New periodic task service.

    Periodic tasks are no longer dispatched by `celeryd`, but instead by a
    separate service called `celerybeat`. This is an optimized, centralized
    service dedicated to your periodic tasks, which means you don't have to
    worry about deadlocks or race conditions any more. But that does mean you
    have to make sure only one instance of this service is running at any one
    time.

  **TIP:** If you're only running a single `celeryd` server, you can embed
  `celerybeat` inside it. Just add the `--beat` argument.


* Broadcast commands

    If you change your mind and don't want to run a task after all, you
    now have the option to revoke it.

    Also, you can rate limit tasks or even shut down the worker remotely.

    It doesn't have many commands yet, but we're waiting for broadcast
    commands to reach its full potential, so please share your ideas
    if you have any.

* Multiple queues

    The worker is able to receive tasks on multiple queues at once.
    This opens up a lot of new possibilities when combined with the impressive
    routing support in AMQP.

* Platform agnostic message format.

  The message format has been standardized and is now using the ISO-8601 format
  for dates instead of Python `datetime` objects. This means you can write task
  consumers in other languages than Python (`eceleryd` anyone?)

* Timely

  Periodic tasks are now scheduled on the clock, i.e. `timedelta(hours=1)`
  means every hour at :00 minutes, not every hour from the server starts.
  To revert to the previous behavior you have the option to enable
  :attr:`PeriodicTask.relative`.

* ... and a lot more!

To read about these and other changes in detail, please refer to
the `changelog`_. This document contains crucial information for those
upgrading from a previous version of Celery, so be sure to read the entire
change set before you continue.

.. _`changelog`: http://ask.github.com/celery/changelog.html

**TIP:** If you install the :mod:`setproctitle` module you can see which
task each worker process is currently executing in `ps` listings.
Just install it using pip: `pip install setproctitle`.

Resources
=========

* Homepage: http://celeryproject.org

* Download: http://pypi.python.org/pypi/celery

* Documentation: http://docs.celeryproject.org/

* Changelog: http://docs.celeryproject.org/changelog.html

* Code: http://github.com/ask/celery/

* FAQ: http://ask.github.com/celery/faq.html

* Mailing-list: http://groups.google.com/group/celery-users

* IRC: #celery on irc.freenode.net.
