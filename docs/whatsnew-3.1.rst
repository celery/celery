.. _whatsnew-3.1:

===========================================
 What's new in Celery 3.1 (Cipater)
===========================================
:Author: Ask Solem (ask at celeryproject.org)

.. sidebar:: Change history

    What's new documents describe the changes in major versions,
    we also have a :ref:`changelog` that lists the changes in bugfix
    releases (0.0.x), while older series are archived under the :ref:`history`
    section.

Celery is a simple, flexible and reliable distributed system to
process vast amounts of messages, while providing operations with
the tools required to maintain such a system.

It's a task queue with focus on real-time processing, while also
supporting task scheduling.

Celery has a large and diverse community of users and contributors,
you should come join us :ref:`on IRC <irc-channel>`
or :ref:`our mailing-list <mailing-list>`.

To read more about Celery you should go read the :ref:`introduction <intro>`.

While this version is backward compatible with previous versions
it's important that you read the following section.

This version is officially supported on CPython 2.6, 2.7 and 3.3,
and also supported on PyPy.

.. _`website`: http://celeryproject.org/

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 2

Preface
=======

Deadlocks have long plagued our workers, and while uncommon they are
not acceptable.  They are also infamous for being extremely hard to diagnose
and reproduce, so to make this job easier I wrote a stress test suite that
bombards the worker with different tasks in an attempt to break it.

What happens if thousands of worker child processes are killed every
second? what if we also kill the broker connection every 10
seconds?  These are examples of what the stress test suite will do to the
worker, and it reruns these tests using different configuration combinations
to find edge case bugs.

The end result was that I had to rewrite the prefork pool to avoid the use
of the POSIX semaphore.  This was extremely challenging, but after
months of hard work the worker now finally passes the stress test suite.

There's probably more bugs to find, but the good news is
that we now have a tool to reproduce them, so should you be so unlucky to
experience a bug then we'll write a test for it and squash it!

Note that I have also moved many broker transports into experimental status:
the only transports recommended for production use today is RabbitMQ and
Redis.

I don't have the resources to maintain all of them, so bugs are left
unresolved.  I wish that someone will step up and take responsibility for
these transports or donate resources to improve them, but  as the situation
is now I don't think the quality is up to date with the rest of the code-base
so I cannot recommend them for production use.

The next version of Celery 3.2 will focus on performance and removing
rarely used parts of the library.  Work has also started on a new message
protocol, supporting multiple languages and more.  The initial draft can
be found :ref:`here <protov2draft>`.

This has probably been the hardest release I've worked on, so no
introduction to this changelog would be complete without a massive
thank you to everyone who contributed and helped me test it!

Thank you for your support!

*— Ask Solem*

.. _v310-important:

Important Notes
===============

Dropped support for Python 2.5
------------------------------

Celery now requires Python 2.6 or later.

The new dual code base runs on both Python 2 and 3, without
requiring the ``2to3`` porting tool.

.. note::

    This is also the last version to support Python 2.6! From Celery 3.2 and
    onwards Python 2.7 or later will be required.

Last version to enable Pickle by default
----------------------------------------

Starting from Celery 3.2 the default serializer will be json.

If you depend on pickle being accepted you should be prepared
for this change by explicitly allowing your worker
to consume pickled messages using the :setting:`CELERY_ACCEPT_CONTENT`
setting:

.. code-block:: python

    CELERY_ACCEPT_CONTENT = ['pickle', 'json', 'msgpack', 'yaml']

Make sure you only select the serialization formats you'll actually be using,
and make sure you have properly secured your broker from unwanted access
(see the :ref:`Security Guide <guide-security>`).

The worker will emit a deprecation warning if you don't define this setting.

.. topic:: for Kombu users

    Kombu 3.0 no longer accepts pickled messages by default, so if you
    use Kombu directly then you have to configure your consumers:
    see the :ref:`Kombu 3.0 Changelog <kombu:version-3.0.0>` for more
    information.

Old command-line programs removed and deprecated
------------------------------------------------

Everyone should move to the new :program:`celery` umbrella
command, so we are incrementally deprecating the old command names.

In this version we've removed all commands that are not used
in init scripts.  The rest will be removed in 3.2.

+-------------------+--------------+-------------------------------------+
| Program           | New Status   | Replacement                         |
+===================+==============+=====================================+
| ``celeryd``       | *DEPRECATED* | :program:`celery worker`            |
+-------------------+--------------+-------------------------------------+
| ``celerybeat``    | *DEPRECATED* | :program:`celery beat`              |
+-------------------+--------------+-------------------------------------+
| ``celeryd-multi`` | *DEPRECATED* | :program:`celery multi`             |
+-------------------+--------------+-------------------------------------+
| ``celeryctl``     | **REMOVED**  | :program:`celery inspect|control`   |
+-------------------+--------------+-------------------------------------+
| ``celeryev``      | **REMOVED**  | :program:`celery events`            |
+-------------------+--------------+-------------------------------------+
| ``camqadm``       | **REMOVED**  | :program:`celery amqp`              |
+-------------------+--------------+-------------------------------------+

If this is not a new installation then you may want to remove the old
commands:

.. code-block:: bash

    $ pip uninstall celery
    $ # repeat until it fails
    # ...
    $ pip uninstall celery
    $ pip install celery

Please run :program:`celery --help` for help using the umbrella command.

.. _v310-news:

News
====

Prefork Pool Improvements
-------------------------

These improvements are only active if you use an async capable
transport.  This means only RabbitMQ (AMQP) and Redis are supported
at this point and other transports will still use the thread-based fallback
implementation.

- Pool is now using one IPC queue per child process.

    Previously the pool shared one queue between all child processes,
    using a POSIX semaphore as a mutex to achieve exclusive read and write
    access.

    The POSIX semaphore has now been removed and each child process
    gets a dedicated queue.  This means that the worker will require more
    file descriptors (two descriptors per process), but it also means
    that performance is improved and we can send work to individual child
    processes.

    POSIX semaphores are not released when a process is killed, so killing
    processes could lead to a deadlock if it happened while the semaphore was
    acquired.  There is no good solution to fix this, so the best option
    was to remove the semaphore.

- Asynchronous write operations

    The pool now uses async I/O to send work to the child processes.

- Lost process detection is now immediate.

    If a child process is killed or exits mysteriously the pool previously
    had to wait for 30 seconds before marking the task with a
    :exc:`~celery.exceptions.WorkerLostError`.  It had to do this because
    the outqueue was shared between all processes, and the pool could not
    be certain whether the process completed the task or not.  So an arbitrary
    timeout of 30 seconds was chosen, as it was believed that the outqueue
    would have been drained by this point.

    This timeout is no longer necessary, and so the task can be marked as
    failed as soon as the pool gets the notification that the process exited.

- Rare race conditions fixed

    Most of these bugs were never reported to us, but was discovered while
    running the new stress test suite.

Caveats
~~~~~~~

.. topic:: Long running tasks

    The new pool will send tasks to a child process as long as the process
    inqueue is writable, and since the socket is buffered this means
    that the processes are, in effect, prefetching tasks.

    This benefits performance but it also means that other tasks may be stuck
    waiting for a long running task to complete::

        -> send T1 to Process A
        # A executes T1
        -> send T2 to Process B
        # B executes T2
        <- T2 complete

        -> send T3 to Process A
        # A still executing T1, T3 stuck in local buffer and
        # will not start until T1 returns

    The buffer size varies based on the operating system: some may
    have a buffer as small as 64kb but on recent Linux versions the buffer
    size is 1MB (can only be changed system wide).

    You can disable this prefetching behavior by enabling the :option:`-Ofair`
    worker option:

    .. code-block:: bash

        $ celery -A proj worker -l info -Ofair

    With this option enabled the worker will only write to workers that are
    available for work, disabling the prefetch behavior.

.. topic:: Max tasks per child

    If a process exits and pool prefetch is enabled the worker may have
    already written many tasks to the process inqueue, and these tasks
    must then be moved back and rewritten to a new process.

    This is very expensive if you have ``--maxtasksperchild`` set to a low
    value (e.g. less than 10), so if you need to enable this option
    you should also enable ``-Ofair`` to turn off the prefetching behavior.

Django supported out of the box
-------------------------------

Celery 3.0 introduced a shiny new API, but sadly did not
have a solution for Django users.

The situation changes with this version as Django is now supported
in core and new Django users coming to Celery are now expected
to use the new API directly.

The Django community has a convention where there's a separate
django-x package for every library, acting like a bridge between
Django and the library.

Having a separate project for Django users has been a pain for Celery,
with multiple issue trackers and multiple documentation
sources, and then lastly since 3.0 we even had different APIs.

With this version we challenge that convention and Django users will
use the same library, the same API and the same documentation as
everyone else.

There is no rush to port your existing code to use the new API,
but if you would like to experiment with it you should know that:

- You need to use a Celery application instance.

    The new Celery API introduced in 3.0 requires users to instantiate the
    library by creating an application:

    .. code-block:: python

        from celery import Celery

        app = Celery()

- You need to explicitly integrate Celery with Django

    Celery will not automatically use the Django settings, so you can
    either configure Celery separately or you can tell it to use the Django
    settings with:

    .. code-block:: python

        app.config_from_object('django.conf:settings')

    Neither will it automatically traverse your installed apps to find task
    modules, but this still available as an option you must enable:

    .. code-block:: python

        from django.conf import settings
        app.autodiscover_tasks(settings.INSTALLED_APPS)

- You no longer use ``manage.py``

    Instead you use the :program:`celery` command directly:

    .. code-block:: bash

        celery -A proj worker -l info

    For this to work your app module must store the  :envvar:`DJANGO_SETTINGS_MODULE`
    environment variable, see the example in the :ref:`Django
    guide <django-first-steps>`.

To get started with the new API you should first read the :ref:`first-steps`
tutorial, and then you should read the Django specific instructions in
:ref:`django-first-steps`.

The fixes and improvements applied by the django-celery library are now
automatically applied by core Celery when it detects that
the :envvar:`DJANGO_SETTINGS_MODULE` environment variable is set.

The distribution ships with a new example project using Django
in :file:`examples/django`:

http://github.com/celery/celery/tree/3.1/examples/django

Some features still require the :mod:`django-celery` library:

    - Celery does not implement the Django database or cache result backends.
    - Celery does not ship with the database-based periodic task
        scheduler.

.. note::

    If you're still using the old API when you upgrade to Celery 3.1
    then you must make sure that your settings module contains
    the ``djcelery.setup_loader()`` line, since this will
    no longer happen as a side-effect of importing the :mod:`djcelery`
    module.

    New users (or if you have ported to the new API) don't need the ``setup_loader``
    line anymore, and must make sure to remove it.

Events are now ordered using logical time
-----------------------------------------

Keeping physical clocks in perfect sync is impossible, so using
timestamps to order events in a distributed system is not reliable.

Celery event messages have included a logical clock value for some time,
but starting with this version that field is also used to order them.

Also, events now record timezone information
by including a new ``utcoffset`` field in the event message.
This is a signed integer telling the difference from UTC time in hours,
so e.g. an even sent from the Europe/London timezone in daylight savings
time will have an offset of 1.

:class:`@events.Receiver` will automatically convert the timestamps
to the local timezone.

.. note::

    The logical clock is synchronized with other nodes
    in the same cluster (neighbors), so this means that the logical
    epoch will start at the point when the first worker in the cluster
    starts.

    If all of the workers are shutdown the clock value will be lost
    and reset to 0, to protect against this you should specify
    a :option:`--statedb` so that the worker can persist the clock
    value at shutdown.

    You may notice that the logical clock is an integer value and
    increases very rapidly.  Do not worry about the value overflowing
    though, as even in the most busy clusters it may take several
    millennia before the clock exceeds a 64 bits value.

New worker node name format (``name@host``)
-------------------------------------------

Node names are now constructed by two elements: name and hostname separated by '@'.

This change was made to more easily identify multiple instances running
on the same machine.

If a custom name is not specified then the
worker will use the name 'celery' by default, resulting in a
fully qualified node name of 'celery@hostname':

.. code-block:: bash

    $ celery worker -n example.com
    celery@example.com

To also set the name you must include the @:

.. code-block:: bash

    $ celery worker -n worker1@example.com
    worker1@example.com

The worker will identify itself using the fully qualified
node name in events and broadcast messages, so where before
a worker would identify itself as 'worker1.example.com', it will now
use 'celery@worker1.example.com'.

Remember that the ``-n`` argument also supports simple variable
substitutions, so if the current hostname is *george.example.com*
then the ``%h`` macro will expand into that:

.. code-block:: bash

    $ celery worker -n worker1@%h
    worker1@george.example.com

The available substitutions are as follows:

+---------------+---------------------------------------+
| Variable      | Substitution                          |
+===============+=======================================+
| ``%h``        | Full hostname (including domain name) |
+---------------+---------------------------------------+
| ``%d``        | Domain name only                      |
+---------------+---------------------------------------+
| ``%n``        | Hostname only (without domain name)   |
+---------------+---------------------------------------+
| ``%%``        | The character ``%``                   |
+---------------+---------------------------------------+

Bound tasks
-----------

The task decorator can now create "bound tasks", which means that the
task will receive the ``self`` argument.

.. code-block:: python

    @app.task(bind=True)
    def send_twitter_status(self, oauth, tweet):
        try:
            twitter = Twitter(oauth)
            twitter.update_status(tweet)
        except (Twitter.FailWhaleError, Twitter.LoginError) as exc:
            raise self.retry(exc=exc)

Using *bound tasks* is now the recommended approach whenever
you need access to the task instance or request context.
Previously one would have to refer to the name of the task
instead (``send_twitter_status.retry``), but this could lead to problems
in some configurations.

Mingle: Worker synchronization
------------------------------

The worker will now attempt to synchronize with other workers in
the same cluster.

Synchronized data currently includes revoked tasks and logical clock.

This only happens at startup and causes a one second startup delay
to collect broadcast responses from other workers.

You can disable this bootstep using the ``--without-mingle`` argument.

Gossip: Worker <-> Worker communication
---------------------------------------

Workers are now passively subscribing to worker related events like
heartbeats.

This means that a worker knows what other workers are doing and
can detect if they go offline.  Currently this is only used for clock
synchronization, but there are many possibilities for future additions
and you can write extensions that take advantage of this already.

Some ideas include consensus protocols, reroute task to best worker (based on
resource usage or data locality) or restarting workers when they crash.

We believe that this is a small addition but one that really opens
up for amazing possibilities.

You can disable this bootstep using the ``--without-gossip`` argument.

Bootsteps: Extending the worker
-------------------------------

By writing bootsteps you can now easily extend the consumer part
of the worker to add additional features, like custom message consumers.

The worker has been using bootsteps for some time, but these were never
documented.  In this version the consumer part of the worker
has also been rewritten to use bootsteps and the new :ref:`guide-extending`
guide documents examples extending the worker, including adding
custom message consumers.

See the :ref:`guide-extending` guide for more information.

.. note::

    Bootsteps written for older versions will not be compatible
    with this version, as the API has changed significantly.

    The old API was experimental and internal but should you be so unlucky
    to use it then please contact the mailing-list and we will help you port
    the bootstep to the new API.

New RPC result backend
----------------------

This new experimental version of the ``amqp`` result backend is a good
alternative to use in classical RPC scenarios, where the process that initiates
the task is always the process to retrieve the result.

It uses Kombu to send and retrieve results, and each client
uses a unique queue for replies to be sent to.  This avoids
the significant overhead of the original amqp backend which creates
one queue per task.

By default results sent using this backend will not persist, so they won't
survive a broker restart.  You can enable
the :setting:`CELERY_RESULT_PERSISTENT` setting to change that.

.. code-block:: python

    CELERY_RESULT_BACKEND = 'rpc'
    CELERY_RESULT_PERSISTENT = True

Note that chords are currently not supported by the RPC backend.

Time limits can now be set by the client
----------------------------------------

Two new options have been added to the Calling API: ``time_limit`` and
``soft_time_limit``:

.. code-block:: python

    >>> res = add.apply_async((2, 2), time_limit=10, soft_time_limit=8)

    >>> res = add.subtask((2, 2), time_limit=10, soft_time_limit=8).delay()

    >>> res = add.s(2, 2).set(time_limit=10, soft_time_limit=8).delay()

Contributed by Mher Movsisyan.

Redis: Broadcast messages and virtual hosts
-------------------------------------------

Broadcast messages are currently seen by all virtual hosts when
using the Redis transport.  You can now fix this by enabling a prefix to all channels
so that the messages are separated:

.. code-block:: python

    BROKER_TRANSPORT_OPTIONS = {'fanout_prefix': True}

Note that you'll not be able to communicate with workers running older
versions or workers that does not have this setting enabled.

This setting will be the default in a future version.

Related to Issue #1490.

:mod:`pytz` replaces ``python-dateutil`` dependency
---------------------------------------------------

Celery no longer depends on the ``python-dateutil`` library,
but instead a new dependency on the :mod:`pytz` library was added.

The :mod:`pytz` library was already recommended for accurate timezone support.

This also means that dependencies are the same for both Python 2 and
Python 3, and that the :file:`requirements/default-py3k.txt` file has
been removed.

Support for Setuptools extra requirements
-----------------------------------------

Pip now supports the :mod:`setuptools` extra requirements format,
so we have removed the old bundles concept, and instead specify
setuptools extras.

You install extras by specifying them inside brackets:

.. code-block:: bash

    $ pip install celery[redis,mongodb]

The above will install the dependencies for Redis and MongoDB.  You can list
as many extras as you want.


.. warning::

    You can't use the ``celery-with-*`` packages anymore, as these will not be
    updated to use Celery 3.1.

+-------------+-------------------------+---------------------------+
| Extension   | Requirement entry       | Type                      |
+=============+=========================+===========================+
| Redis       | ``celery[redis]``       | transport, result backend |
+-------------+-------------------------+---------------------------+
| MongoDB     | ``celery[mongodb]``     | transport, result backend |
+-------------+-------------------------+---------------------------+
| CouchDB     | ``celery[couchdb]``     | transport                 |
+-------------+-------------------------+---------------------------+
| Beanstalk   | ``celery[beanstalk]``   | transport                 |
+-------------+-------------------------+---------------------------+
| ZeroMQ      | ``celery[zeromq]``      | transport                 |
+-------------+-------------------------+---------------------------+
| Zookeeper   | ``celery[zookeeper]``   | transport                 |
+-------------+-------------------------+---------------------------+
| SQLAlchemy  | ``celery[sqlalchemy]``  | transport, result backend |
+-------------+-------------------------+---------------------------+
| librabbitmq | ``celery[librabbitmq]`` | transport (C amqp client) |
+-------------+-------------------------+---------------------------+

The complete list with examples is found in the :ref:`bundles` section.

``subtask.__call__()`` now executes the task directly
-----------------------------------------------------

A misunderstanding led to ``Signature.__call__`` being an alias of
``.delay`` but this does not conform to the calling API of ``Task`` which
calls the underlying task method.

This means that:

.. code-block:: python

    @app.task
    def add(x, y):
        return x + y

    add.s(2, 2)()

now does the same as calling the task directly:

.. code-block:: python

    add(2, 2)

In Other News
-------------

- Now depends on :ref:`Kombu 3.0 <kombu:version-3.0.0>`.

- Now depends on :mod:`billiard` version 3.3.

- Worker will now crash if running as the root user with pickle enabled.

- Canvas: ``group.apply_async`` and ``chain.apply_async`` no longer starts
  separate task.

    That the group and chord primitives supported the "calling API" like other
    subtasks was a nice idea, but it was useless in practice and often
    confused users.  If you still want this behavior you can define a
    task to do it for you.

- New method ``Signature.freeze()`` can be used to "finalize"
  signatures/subtask.

    Regular signature:

    .. code-block:: python

        >>> s = add.s(2, 2)
        >>> result = s.freeze()
        >>> result
        <AsyncResult: ffacf44b-f8a1-44e9-80a3-703150151ef2>
        >>> s.delay()
        <AsyncResult: ffacf44b-f8a1-44e9-80a3-703150151ef2>

    Group:

    .. code-block:: python

        >>> g = group(add.s(2, 2), add.s(4, 4))
        >>> result = g.freeze()
        <GroupResult: e1094b1d-08fc-4e14-838e-6d601b99da6d [
            70c0fb3d-b60e-4b22-8df7-aa25b9abc86d,
            58fcd260-2e32-4308-a2ea-f5be4a24f7f4]>
        >>> g()
        <GroupResult: e1094b1d-08fc-4e14-838e-6d601b99da6d [70c0fb3d-b60e-4b22-8df7-aa25b9abc86d, 58fcd260-2e32-4308-a2ea-f5be4a24f7f4]>

-  New ability to specify additional command line options
   to the worker and beat programs.

    The :attr:`@Celery.user_options` attribute can be used
    to add additional command-line arguments, and expects
    optparse-style options:

    .. code-block:: python

        from celery import Celery
        from celery.bin import Option

        app = Celery()
        app.user_options['worker'].add(
            Option('--my-argument'),
        )

    See the :ref:`guide-extending` guide for more information.

- All events now include a ``pid`` field, which is the process id of the
  process that sent the event.

- Event heartbeats are now calculated based on the time when the event
  was received by the monitor, and not the time reported by the worker.

    This means that a worker with an out-of-sync clock will no longer
    show as 'Offline' in monitors.

    A warning is now emitted if the difference between the senders
    time and the internal time is greater than 15 seconds, suggesting
    that the clocks are out of sync.

- Monotonic clock support.

    A monotonic clock is now used for timeouts and scheduling.

    The monotonic clock function is built-in starting from Python 3.4,
    but we also have fallback implementations for Linux and OS X.

- :program:`celery worker` now supports a ``--detach`` argument to start
  the worker as a daemon in the background.

- :class:`@events.Receiver` now sets a ``local_received`` field for incoming
  events, which is set to the time of when the event was received.

- :class:`@events.Dispatcher` now accepts a ``groups`` argument
  which decides a white-list of event groups that will be sent.

    The type of an event is a string separated by '-', where the part
    before the first '-' is the group.  Currently there are only
    two groups: ``worker`` and ``task``.

    A dispatcher instantiated as follows:

    .. code-block:: python

        app.events.Dispatcher(connection, groups=['worker'])

    will only send worker related events and silently drop any attempts
    to send events related to any other group.

- New :setting:`BROKER_FAILOVER_STRATEGY` setting.

    This setting can be used to change the transport failover strategy,
    can either be a callable returning an iterable or the name of a
    Kombu built-in failover strategy.  Default is "round-robin".

    Contributed by Matt Wise.

- ``Result.revoke`` will no longer wait for replies.

    You can add the ``reply=True`` argument if you really want to wait for
    responses from the workers.

- Better support for link and link_error tasks for chords.

    Contributed by Steeve Morin.

- Worker: Now emits warning if the :setting:`CELERYD_POOL` setting is set
  to enable the eventlet/gevent pools.

    The `-P` option should always be used to select the eventlet/gevent pool
    to ensure that the patches are applied as early as possible.

    If you start the worker in a wrapper (like Django's manage.py)
    then you must apply the patches manually, e.g. by creating an alternative
    wrapper that monkey patches at the start of the program before importing
    any other modules.

- There's a now an 'inspect clock' command which will collect the current
  logical clock value from workers.

- `celery inspect stats` now contains the process id of the worker's main
  process.

    Contributed by Mher Movsisyan.

- New remote control command to dump a workers configuration.

    Example:

    .. code-block:: bash

        $ celery inspect conf

    Configuration values will be converted to values supported by JSON
    where possible.

    Contributed by Mher Movisyan.

- New settings :setting:`CELERY_EVENT_QUEUE_TTL` and
  :setting:`CELERY_EVENT_QUEUE_EXPIRES`.

    These control when a monitors event queue is deleted, and for how long
    events published to that queue will be visible.  Only supported on
    RabbitMQ.

- New Couchbase result backend.

    This result backend enables you to store and retrieve task results
    using `Couchbase`_.

    See :ref:`conf-couchbase-result-backend` for more information
    about configuring this result backend.

    Contributed by Alain Masiero.

    .. _`Couchbase`: http://www.couchbase.com

- CentOS init script now supports starting multiple worker instances.

    See the script header for details.

    Contributed by Jonathan Jordan.

- ``AsyncResult.iter_native`` now sets default interval parameter to 0.5

    Fix contributed by Idan Kamara

- New setting :setting:`BROKER_LOGIN_METHOD`.

    This setting can be used to specify an alternate login method
    for the AMQP transports.

    Contributed by Adrien Guinet

- The ``dump_conf`` remote control command will now give the string
  representation for types that are not JSON compatible.

- Function `celery.security.setup_security` is now :func:`@setup_security`.

- Task retry now propagates the message expiry value (Issue #980).

    The value is forwarded at is, so the expiry time will not change.
    To update the expiry time you would have to pass a new expires
    argument to ``retry()``.

- Worker now crashes if a channel error occurs.

    Channel errors are transport specific and is the list of exceptions
    returned by ``Connection.channel_errors``.
    For RabbitMQ this means that Celery will crash if the equivalence
    checks for one of the queues in :setting:`CELERY_QUEUES` mismatches, which
    makes sense since this is a scenario where manual intervention is
    required.

- Calling ``AsyncResult.get()`` on a chain now propagates errors for previous
  tasks (Issue #1014).

- The parent attribute of ``AsyncResult`` is now reconstructed when using JSON
  serialization (Issue #1014).

- Worker disconnection logs are now logged with severity warning instead of
  error.

    Contributed by Chris Adams.

- ``events.State`` no longer crashes when it receives unknown event types.

- SQLAlchemy Result Backend: New :setting:`CELERY_RESULT_DB_TABLENAMES`
  setting can be used to change the name of the database tables used.

    Contributed by Ryan Petrello.

- SQLAlchemy Result Backend: Now calls ``enginge.dispose`` after fork
   (Issue #1564).

    If you create your own sqlalchemy engines then you must also
    make sure that these are closed after fork in the worker:

    .. code-block:: python

        from multiprocessing.util import register_after_fork

        engine = create_engine(…)
        register_after_fork(engine, engine.dispose)

- A stress test suite for the Celery worker has been written.

    This is located in the ``funtests/stress`` directory in the git
    repository. There's a README file there to get you started.

- The logger named ``celery.concurrency`` has been renamed to ``celery.pool``.

- New command line utility ``celery graph``.

    This utility creates graphs in GraphViz dot format.

    You can create graphs from the currently installed bootsteps:

    .. code-block:: bash

        # Create graph of currently installed bootsteps in both the worker
        # and consumer namespaces.
        $ celery graph bootsteps | dot -T png -o steps.png

        # Graph of the consumer namespace only.
        $ celery graph bootsteps consumer | dot -T png -o consumer_only.png

        # Graph of the worker namespace only.
        $ celery graph bootsteps worker | dot -T png -o worker_only.png

    Or graphs of workers in a cluster:

    .. code-block:: bash

        # Create graph from the current cluster
        $ celery graph workers | dot -T png -o workers.png

        # Create graph from a specified list of workers
        $ celery graph workers nodes:w1,w2,w3 | dot -T png workers.png

        # also specify the number of threads in each worker
        $ celery graph workers nodes:w1,w2,w3 threads:2,4,6

        # …also specify the broker and backend URLs shown in the graph
        $ celery graph workers broker:amqp:// backend:redis://

        # …also specify the max number of workers/threads shown (wmax/tmax),
        # enumerating anything that exceeds that number.
        $ celery graph workers wmax:10 tmax:3

- Changed the way that app instances are pickled.

    Apps can now define a ``__reduce_keys__`` method that is used instead
    of the old ``AppPickler`` attribute.  E.g. if your app defines a custom
    'foo' attribute that needs to be preserved when pickling you can define
    a ``__reduce_keys__`` as such:

    .. code-block:: python

        import celery

        class Celery(celery.Celery):

            def __init__(self, *args, **kwargs):
                super(Celery, self).__init__(*args, **kwargs)
                self.foo = kwargs.get('foo')

            def __reduce_keys__(self):
                return super(Celery, self).__reduce_keys__().update(
                    foo=self.foo,
                )

    This is a much more convenient way to add support for pickling custom
    attributes. The old ``AppPickler`` is still supported but its use is
    discouraged and we would like to remove it in a future version.

- Ability to trace imports for debugging purposes.

    The :envvar:`C_IMPDEBUG` can be set to trace imports as they
    occur:

    .. code-block:: bash

        $ C_IMDEBUG=1 celery worker -l info

    .. code-block:: bash

        $ C_IMPDEBUG=1 celery shell

- Message headers now available as part of the task request.

    Example adding and retrieving a header value:

    .. code-block:: python

        @app.task(bind=True)
        def t(self):
            return self.request.headers.get('sender')

        >>> t.apply_async(headers={'sender': 'George Costanza'})

- New :signal:`before_task_publish` signal dispatched before a task message
  is sent and can be used to modify the final message fields (Issue #1281).

- New :signal:`after_task_publish` signal replaces the old :signal:`task_sent`
  signal.

    The :signal:`task_sent` signal is now deprecated and should not be used.

- New :signal:`worker_process_shutdown` signal is dispatched in the
  prefork pool child processes as they exit.

    Contributed by Daniel M Taub.

- ``celery.platforms.PIDFile`` renamed to :class:`celery.platforms.Pidfile`.

- MongoDB Backend: Can now be configured using an URL:

    See :ref:`example-mongodb-result-config`.

- MongoDB Backend: No longer using deprecated ``pymongo.Connection``.

- MongoDB Backend: Now disables ``auto_start_request``.

- MongoDB Backend: Now enables ``use_greenlets`` when eventlet/gevent is used.

- ``subtask()`` / ``maybe_subtask()`` renamed to
  ``signature()``/``maybe_signature()``.

    Aliases still available for backwards compatibility.

- The ``correlation_id`` message property is now automatically set to the
  id of the task.

- The task message ``eta`` and ``expires`` fields now includes timezone
  information.

- All result backends ``store_result``/``mark_as_*`` methods must now accept
  a ``request`` keyword argument.

- Events now emit warning if the broken ``yajl`` library is used.

- The :signal:`celeryd_init` signal now takes an extra keyword argument:
  ``option``.

    This is the mapping of parsed command line arguments, and can be used to
    prepare new preload arguments (``app.user_options['preload']``).

- New callback: ``Celery.on_configure``.

    This callback is called when an app is about to be configured (a
    configuration key is required).

- Worker: No longer forks on :sig:`HUP`.

    This means that the worker will reuse the same pid for better
    support with external process supervisors.

    Contributed by Jameel Al-Aziz.

- Worker: The log message ``Got task from broker …`` was changed to
  ``Received task …``.

- Worker: The log message ``Skipping revoked task …`` was changed
  to ``Discarding revoked task …``.

- Optimization: Improved performance of ``ResultSet.join_native()``.

    Contributed by Stas Rudakou.

- The :signal:`task_revoked` signal now accepts new ``request`` argument
  (Issue #1555).

    The revoked signal is dispatched after the task request is removed from
    the stack, so it must instead use the :class:`~celery.worker.job.Request`
    object to get information about the task.

- Worker: New :option:`-X` command line argument to exclude queues
  (Issue #1399).

    The :option:`-X` argument is the inverse of the :option:`-Q` argument
    and accepts a list of queues to exclude (not consume from):

    .. code-block:: bash

        # Consume from all queues in CELERY_QUEUES, but not the 'foo' queue.
        $ celery worker -A proj -l info -X foo

- Adds :envvar:`C_FAKEFORK` envvar for simple init script/multi debugging.

    This means that you can now do:

    .. code-block:: bash

            $ C_FAKEFORK=1 celery multi start 10

    or:

    .. code-block:: bash

        $ C_FAKEFORK=1 /etc/init.d/celeryd start

    to avoid the daemonization step to see errors that are not visible
    due to missing stdout/stderr.

    A ``dryrun`` command has been added to the generic init script that
    enables this option.

- New public API to push and pop from the current task stack:

    :func:`celery.app.push_current_task` and
    :func:`celery.app.pop_current_task``.

- ``RetryTaskError`` has been renamed to :exc:`~celery.exceptions.Retry`.

    The old name is still available for backwards compatibility.

- New semi-predicate exception :exc:`~celery.exceptions.Reject`.

    This exception can be raised to ``reject``/``requeue`` the task message,
    see :ref:`task-semipred-reject` for examples.

- :ref:`Semipredicates <task-semipredicates>` documented: (Retry/Ignore/Reject).


.. _v310-removals:

Scheduled Removals
==================

- The ``BROKER_INSIST`` setting and the ``insist`` argument
  to ``~@connection`` is no longer supported.

- The ``CELERY_AMQP_TASK_RESULT_CONNECTION_MAX`` setting is no longer
  supported.

    Use :setting:`BROKER_POOL_LIMIT` instead.

- The ``CELERY_TASK_ERROR_WHITELIST`` setting is no longer supported.

    You should set the :class:`~celery.utils.mail.ErrorMail` attribute
    of the task class instead.  You can also do this using
    :setting:`CELERY_ANNOTATIONS`:

        .. code-block:: python

            from celery import Celery
            from celery.utils.mail import ErrorMail

            class MyErrorMail(ErrorMail):
                whitelist = (KeyError, ImportError)

                def should_send(self, context, exc):
                    return isinstance(exc, self.whitelist)

            app = Celery()
            app.conf.CELERY_ANNOTATIONS = {
                '*': {
                    'ErrorMail': MyErrorMails,
                }
            }

- Functions that creates a broker connections no longer
  supports the ``connect_timeout`` argument.

    This can now only be set using the :setting:`BROKER_CONNECTION_TIMEOUT`
    setting.  This is because functions no longer create connections
    directly, but instead get them from the connection pool.

- The ``CELERY_AMQP_TASK_RESULT_EXPIRES`` setting is no longer supported.

    Use :setting:`CELERY_TASK_RESULT_EXPIRES` instead.

.. _v310-deprecations:

Deprecations
============

See the :ref:`deprecation-timeline`.

.. _v310-fixes:

Fixes
=====

- AMQP Backend: join did not convert exceptions when using the json
  serializer.

- Non-abstract task classes are now shared between apps (Issue #1150).

    Note that non-abstract task classes should not be used in the
    new API.  You should only create custom task classes when you
    use them as a base class in the ``@task`` decorator.

    This fix ensure backwards compatibility with older Celery versions
    so that non-abstract task classes works even if a module is imported
    multiple times so that the app is also instantiated multiple times.

- Worker: Workaround for Unicode errors in logs (Issue #427).

- Task methods: ``.apply_async`` now works properly if args list is None
  (Issue #1459).

- Eventlet/gevent/solo/threads pools now properly handles :exc:`BaseException`
  errors raised by tasks.

- Autoscale and ``pool_grow``/``pool_shrink`` remote control commands
  will now also automatically increase and decrease the consumer prefetch count.

    Fix contributed by Daniel M. Taub.

- ``celery control pool_`` commands did not coerce string arguments to int.

- Redis/Cache chords: Callback result is now set to failure if the group
  disappeared from the database (Issue #1094).

- Worker: Now makes sure that the shutdown process is not initiated multiple
  times.

- Multi: Now properly handles both ``-f`` and ``--logfile`` options
  (Issue #1541).

.. _v310-internal:

Internal changes
================

- Module ``celery.task.trace`` has been renamed to :mod:`celery.app.trace`.

- Module ``celery.concurrency.processes`` has been renamed to
  :mod:`celery.concurrency.prefork`.

- Classes that no longer fall back to using the default app:

    - Result backends (:class:`celery.backends.base.BaseBackend`)
    - :class:`celery.worker.WorkController`
    - :class:`celery.worker.Consumer`
    - :class:`celery.worker.job.Request`

    This means that you have to pass a specific app when instantiating
    these classes.

- ``EventDispatcher.copy_buffer`` renamed to
  :meth:`@events.Dispatcher.extend_buffer`.

- Removed unused and never documented global instance
  ``celery.events.state.state``.

- :class:`@events.Receiver` is now a :class:`kombu.mixins.ConsumerMixin`
  subclass.

- :class:`celery.apps.worker.Worker` has been refactored as a subclass of
  :class:`celery.worker.WorkController`.

    This removes a lot of duplicate functionality.

- The ``Celery.with_default_connection`` method has been removed in favor
  of ``with app.connection_or_acquire``.

- The ``celery.results.BaseDictBackend`` class has been removed and is replaced by
  :class:`celery.results.BaseBackend`.
