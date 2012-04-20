.. _whatsnew-2.5:

==========================
 What's new in Celery 2.5
==========================

Celery aims to be a flexible and reliable, best-of-breed solution
to process vast amounts of messages in a distributed fashion, while
providing operations with the tools to maintain such a system.

Celery has a large and diverse community of users and contributors,
you should come join us :ref:`on IRC <irc-channel>`
or :ref:`our mailing-list <mailing-list>`.

To read more about Celery you should visit our `website`_.

While this version is backward compatible with previous versions
it is important that you read the following section.

If you use Celery in combination with Django you must also
read the `django-celery changelog`_ and upgrade to `django-celery 2.5`_.

This version is officially supported on CPython 2.5, 2.6, 2.7, 3.2 and 3.3,
as well as PyPy and Jython.


.. _`website`: http://celeryproject.org/
.. _`django-celery changelog`: http://bit.ly/djcelery-25-changelog
.. _`django-celery 2.5`: http://pypi.python.org/pypi/django-celery/

.. contents::
    :local:

.. _v250-important:

Important Notes
===============

Broker connection pool now enabled by default
---------------------------------------------

The default limit is 10 connections, if you have many threads/green-threads
using connections at the same time you may want to tweak this limit
to avoid contention.

See the :setting:`BROKER_POOL_LIMIT` setting for more information.

Also note that publishing tasks will be retried by default, to change
this default or the default retry policy see
:setting:`CELERY_TASK_PUBLISH_RETRY` and
:setting:`CELERY_TASK_PUBLISH_RETRY_POLICY`.

AMQP Result Backend: Exchange is no longer *auto delete*
--------------------------------------------------------

The exchange used for results used to have the *auto_delete* flag set,
that could result in a race condition leading to an annoying warning.

.. admonition:: For RabbitMQ users

    Old exchanges created with the *auto_delete* flag enabled has
    to be removed.

    The :program:`camqadm` command can be used to delete the
    previous exchange::

        $ camqadm exchange.delete celeryresults

    As an alternative to deleting the old exchange you can
    configure a new name for the exchange::

        CELERY_RESULT_EXCHANGE = "celeryresults2"

    But you have to make sure that all clients and workers
    use this new setting, so they are updated to use the same
    exchange name.

Solution for hanging workers (but must be manually enabled)
-----------------------------------------------------------

The :setting:`CELERYD_FORCE_EXECV` setting has been added to solve
a problem with deadlocks that originate when threads and fork is mixed
together:

.. code-block:: python

    CELERYD_FORCE_EXECV = True

This setting is recommended for all users using the processes pool,
but especially users also using time limits or a max tasks per child
setting.

- See `Python Issue 6721`_ to read more about this issue, and why
  resorting to :func:`~os.execv`` is the only safe solution.

Enabling this option will result in a slight performance penalty
when new child worker processes are started, and it will also increase
memory usage (but many platforms are optimized, so the impact may be
minimal).  Considering that it ensures reliability when replacing
lost worker processes, it should be worth it.

- It's already the default behavior on Windows.
- It will be the default behavior for all platforms in a future version.

.. _`Python Issue 6721`: http://bugs.python.org/issue6721#msg140215

.. _v250-optimizations:

Optimizations
=============

- The code path used when the worker executes a task has been heavily
  optimized, meaning the worker is able to process a great deal
  more tasks/second compared to previous versions.  As an example the solo
  pool can now process up to 15000 tasks/second on a 4 core MacBook Pro
  when using the `pylibrabbitmq`_ transport, where it previously
  could only do 5000 tasks/second.

- The task error tracebacks are now much shorter.

- Fixed a noticeable delay in task processing when rate limits are enabled.

.. _`pylibrabbitmq`: http://pypi.python.org/pylibrabbitmq/

.. _v250-deprecations:

Deprecations
============

Removals
--------

* The old :class:`TaskSet` signature of ``(task_name, list_of_tasks)``
  can no longer be used (originally scheduled for removal in 2.4).
  The deprecated ``.task_name`` and ``.task`` attributes has also been
  removed.

* The functions ``celery.execute.delay_task``, ``celery.execute.apply``,
  and ``celery.execute.apply_async`` has been removed (originally)
  scheduled for removal in 2.3).

* The built-in ``ping`` task has been removed (originally scheduled
  for removal in 2.3).  Please use the ping broadcast command
  instead.

* It is no longer possible to import ``subtask`` and ``TaskSet``
  from :mod:`celery.task.base`, please import them from :mod:`celery.task`
  instead (originally scheduled for removal in 2.4).

Deprecations
------------

* The :mod:`celery.decorators` module has changed status
  from pending deprecation to deprecated, and is scheduled for removal
  in version 3.0.  The ``celery.task`` module must be used instead.

.. _v250-news:

News
====

Timezone support
----------------

Celery can now be configured to treat all incoming and outgoing dates
as UTC, and the local timezone can be configured.

This is not yet enabled by default, since enabling
time zone support means workers running versions pre 2.5
will be out of sync with upgraded workers.

To enable UTC you have to set :setting:`CELERY_ENABLE_UTC`::

    CELERY_ENABLE_UTC = True

When UTC is enabled, dates and times in task messages will be
converted to UTC, and then converted back to the local timezone
when received by a worker.

You can change the local timezone using the :setting:`CELERY_TIMEZONE`
setting.  Installing the :mod:`pytz` library is recommended when
using a custom timezone, to keep timezone definition up-to-date,
but it will fallback to a system definition of the timezone if available.

UTC will enabled by default in version 3.0.

.. note::

    django-celery will use the local timezone as specified by the
    ``TIME_ZONE`` setting, it will also honor the new `USE_TZ`_ setting
    introuced in Django 1.4.

.. _`USE_TZ`: https://docs.djangoproject.com/en/dev/topics/i18n/timezones/

New security serializer using cryptographic signing
---------------------------------------------------

A new serializer has been added that signs and verifies the signature
of messages.

The name of the new serializer is ``auth``, and needs additional
configuration to work (see :ref:`conf-security`).

.. seealso::

    :ref:`guide-security`

Contributed by Mher Movsisyan.

Experimental support for automatic module reloading
---------------------------------------------------

Starting :program:`celeryd` with the :option:`--autoreload` option will
enable the worker to watch for file system changes to all imported task
modules imported (and also any non-task modules added to the
:setting:`CELERY_IMPORTS` setting or the :option:`-I|--include` option).

This is an experimental feature intended for use in development only,
using auto-reload in production is discouraged as the behavior of reloading
a module in Python is undefined, and may cause hard to diagnose bugs and
crashes.  Celery uses the same approach as the auto-reloader found in e.g.
the Django ``runserver`` command.

When auto-reload is enabled the worker starts an additional thread
that watches for changes in the file system.  New modules are imported,
and already imported modules are reloaded whenever a change is detected,
and if the processes pool is used the child processes will finish the work
they are doing and exit, so that they can be replaced by fresh processes
effectively reloading the code.

File system notification backends are pluggable, and Celery comes with three
implementations:

* inotify (Linux)

    Used if the :mod:`pyinotify` library is installed.
    If you are running on Linux this is the recommended implementation,
    to install the :mod:`pyinotify` library you have to run the following
    command::

        $ pip install pyinotify

* kqueue (OS X/BSD)

* stat

    The fallback implementation simply polls the files using ``stat`` and is very
    expensive.

You can force an implementation by setting the :envvar:`CELERYD_FSNOTIFY`
environment variable::

    $ env CELERYD_FSNOTIFY=stat celeryd -l info --autoreload

Contributed by Mher Movsisyan.


New :setting:`CELERY_ANNOTATIONS` setting
-----------------------------------------

This new setting enables the configuration to modify task classes
and their attributes.

The setting can be a dict, or a list of annotation objects that filter
for tasks and return a map of attributes to change.

As an example, this is an annotation to change the ``rate_limit`` attribute
for the ``tasks.add`` task:

.. code-block:: python

    CELERY_ANNOTATIONS = {"tasks.add": {"rate_limit": "10/s"}}

or change the same for all tasks:

.. code-block:: python

   CELERY_ANNOTATIONS = {"*": {"rate_limit": "10/s"}}

You can change methods too, for example the ``on_failure`` handler:

.. code-block:: python

    def my_on_failure(self, exc, task_id, args, kwargs, einfo):
        print("Oh no! Task failed: %r" % (exc, ))

    CELERY_ANNOTATIONS = {"*": {"on_failure": my_on_failure}}

If you need more flexibility then you can also create objects
that filter for tasks to annotate:

.. code-block:: python

    class MyAnnotate(object):

        def annotate(self, task):
            if task.name.startswith("tasks."):
                return {"rate_limit": "10/s"}

    CELERY_ANNOTATIONS = (MyAnnotate(), {...})

``current`` provides the currently executing task
-------------------------------------------------

The new :data:`celery.task.current` proxy will always give the currently
executing task.

**Example**:

.. code-block:: python

    from celery.task import current, task

    @task
    def update_twitter_status(auth, message):
        twitter = Twitter(auth)
        try:
            twitter.update_status(message)
        except twitter.FailWhale, exc:
            # retry in 10 seconds.
            current.retry(countdown=10, exc=exc)

Previously you would have to type ``update_twitter_status.retry(...)``
here, which can be annoying for long task names.

.. note::
    This will not work if the task function is called directly, i.e:
    ``update_twitter_status(a, b)``. For that to work ``apply`` must
    be used: ``update_twitter_status.apply((a, b))``.

In Other News
-------------

- Now depends on Kombu 2.1.0.

- Efficient Chord support for the memcached backend (Issue #533)

    This means memcached joins Redis in the ability to do non-polling
    chords.

    Contributed by Dan McGee.

- Adds Chord support for the AMQP backend

    The AMQP backend can now use the fallback chord solution.

- Sending :sig:`QUIT` to celeryd will now cause it cold terminate.

    That is, it will not finish executing the tasks it is currently
    working on.

    Contributed by Alec Clowes.

- New "detailed" mode for the Cassandra backend.

    Allows to have a "detailed" mode for the Cassandra backend.
    Basically the idea is to keep all states using Cassandra wide columns.
    New states are then appended to the row as new columns, the last state
    being the last column.

    See the :setting:`CASSANDRA_DETAILED_MODE` setting.

    Contributed by Steeve Morin.

- The crontab parser now matches Vixie Cron behavior when parsing ranges
  with steps (e.g. 1-59/2).

    Contributed by Daniel Hepper.

- celerybeat can now be configured on the command line like celeryd.

  Additional configuration must be added at the end of the argument list
  followed by ``--``, for example::

    $ celerybeat -l info -- celerybeat.max_loop_interval=10.0

- Now limits the number of frames in a traceback so that celeryd does not
  crash on maximum recursion limit exceeded exceptions (Issue #615).

    The limit is set to the current recursion limit divided by 8 (which
    is 125 by default).

    To get or set the current recursion limit use
    :func:`sys.getrecursionlimit` and :func:`sys.setrecursionlimit`.

- More information is now preserved in the pickleable traceback.

    This has been added so that Sentry can show more details.

    Contributed by Sean O'Connor.

- CentOS init script has been updated and should be more flexible.

    Contributed by Andrew McFague.

- MongoDB result backend now supports ``forget()``.

    Contributed by Andrew McFague

- ``task.retry()`` now re-raises the original exception keeping
  the original stack trace.

    Suggested by ojii.

- The `--uid` argument to daemons now uses ``initgroups()`` to set
  groups to all the groups the user is a member of.

    Contributed by Łukasz Oleś.

- celeryctl: Added ``shell`` command.

    The shell will have the current_app (``celery``) and all tasks
    automatically added to locals.

- celeryctl: Added ``migrate`` command.

    The migrate command moves all tasks from one broker to another.
    Note that this is experimental and you should have a backup
    of the data before proceeding.

    **Examples**::

        $ celeryctl migrate redis://localhost amqp://localhost
        $ celeryctl migrate amqp://localhost//v1 amqp://localhost//v2
        $ python manage.py celeryctl migrate django:// redis://

* Routers can now override the ``exchange`` and ``routing_key`` used
  to create missing queues (Issue #577).

    By default this will always use the name of the queue,
    but you can now have a router return exchange and routing_key keys
    to set them.

    This is useful when using routing classes which decides a destination
    at runtime.

    Contributed by Akira Matsuzaki.

- Redis result backend: Adds support for a ``max_connections`` parameter.

    It is now possible to configure the maximum number of
    simultaneous connections in the Redis connection pool used for
    results.

    The default max connections setting can be configured using the
    :setting:`CELERY_REDIS_MAX_CONNECTIONS` setting,
    or it can be changed individually by ``RedisBackend(max_connections=int)``.

    Contributed by Steeve Morin.

- Redis result backend: Adds the ability to wait for results without polling.

    Contributed by Steeve Morin.

- MongoDB result backend: Now supports save and restore taskset.

    Contributed by Julien Poissonnier.

- There's a new :ref:`guide-security` guide in the documentation.

- The init scripts has been updated, and many bugs fixed.

    Contributed by Chris Streeter.

- User (tilde) is now expanded in command line arguments.

- Can now configure CELERYCTL envvar in :file:`/etc/default/celeryd`.

    While not necessary for operation, :program:`celeryctl` is used for the
    ``celeryd status`` command, and the path to :program:`celeryctl` must be
    configured for that to work.

    The daemonization cookbook contains examples.

    Contributed by Jude Nagurney.

- The MongoDB result backend can now use Replica Sets.

    Contributed by Ivan Metzlar.

- gevent: Now supports autoscaling (Issue #599).

    Contributed by Mark Lavin.

- multiprocessing: Mediator thread is now always enabled,
  even though rate limits are disabled, as the pool semaphore
  is known to block the main thread, causing broadcast commands and
  shutdown to depend on the semaphore being released.

Fixes
=====

- Exceptions that are re-raised with a new exception object now keeps
  the original stack trace.

- Windows: Fixed the ``no handlers found for multiprocessing`` warning.

- Windows: The ``celeryd`` program can now be used.

    Previously Windows users had to launch celeryd using
    ``python -m celery.bin.celeryd``.

- Redis result backend: Now uses ``SETEX`` command to set result key,
  and expiry atomically.

    Suggested by yaniv-aknin.

- celeryd: Fixed a problem where shutdown hanged when Ctrl+C was used to
  terminate.

- celeryd: No longer crashes when channel errors occur.

    Fix contributed by Roger Hu.

- Fixed memory leak in the eventlet pool, caused by the
  use of ``greenlet.getcurrent``.

    Fix contributed by Ignas Mikalajūnas.


- Cassandra backend: No longer uses :func:`pycassa.connect` which is
  deprecated since :mod:`pycassa` 1.4.

    Fix contributed by Jeff Terrace.

- Fixed unicode decode errors that could occur while sending error emails.

    Fix contributed by Seong Wun Mun.

- ``celery.bin`` programs now always defines ``__package__`` as recommended
  by PEP-366.

- ``send_task`` now emits a warning when used in combination with
  :setting:`CELERY_ALWAYS_EAGER` (Issue #581).

    Contributed by Mher Movsisyan.

- ``apply_async`` now forwards the original keyword arguments to ``apply``
  when :setting:`CELERY_ALWAYS_EAGER` is enabled.

- celeryev now tries to re-establish the connection if the connection
  to the broker is lost (Issue #574).

- celeryev: Fixed a crash occurring if a task has no associated worker
  information.

    Fix contributed by Matt Williamson.

- The current date and time is now consistently taken from the current loaders
  ``now`` method.

- Now shows helpful error message when given a config module ending in
  ``.py`` that can't be imported.

- celeryctl: The ``--expires`` and ``-eta`` arguments to the apply command
  can now be an ISO-8601 formatted string.

- celeryctl now exits with exit status ``EX_UNAVAILABLE`` (69) if no replies
  have been received.
