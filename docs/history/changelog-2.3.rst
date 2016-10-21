.. _changelog-2.3:

===============================
 Change history for Celery 2.3
===============================

.. contents::
    :local:

.. _version-2.3.4:

2.3.4
=====
:release-date: 2011-11-25 04:00 p.m. GMT
:release-by: Ask Solem

.. _v234-security-fixes:

Security Fixes
--------------

* [Security: `CELERYSA-0001`_] Daemons would set effective id's rather than
  real id's when the :option:`--uid <celery --uid>`/
  :option:`--gid <celery --gid>` arguments to :program:`celery multi`,
  :program:`celeryd_detach`, :program:`celery beat` and
  :program:`celery events` were used.

  This means privileges weren't properly dropped, and that it would
  be possible to regain supervisor privileges later.


.. _`CELERYSA-0001`:
    https://github.com/celery/celery/tree/master/docs/sec/CELERYSA-0001.txt

Fixes
-----

* Backported fix for #455 from 2.4 to 2.3.

* StateDB wasn't saved at shutdown.

* Fixes worker sometimes hanging when hard time limit exceeded.


.. _version-2.3.3:

2.3.3
=====
:release-date: 2011-16-09 05:00 p.m. BST
:release-by: Mher Movsisyan

* Monkey patching :attr:`sys.stdout` could result in the worker
  crashing if the replacing object didn't define :meth:`isatty`
  (Issue #477).

* ``CELERYD`` option in :file:`/etc/default/celeryd` shouldn't
  be used with generic init-scripts.


.. _version-2.3.2:

2.3.2
=====
:release-date: 2011-10-07 05:00 p.m. BST
:release-by: Ask Solem

.. _v232-news:

News
----

* Improved Contributing guide.

    If you'd like to contribute to Celery you should read the
    :ref:`Contributing Gudie <contributing>`.

    We're looking for contributors at all skill levels, so don't
    hesitate!

* Now depends on Kombu 1.3.1

* ``Task.request`` now contains the current worker host name (Issue #460).

    Available as ``task.request.hostname``.

* It's now easier for app subclasses to extend how they're pickled.
    (see :class:`celery.app.AppPickler`).

.. _v232-fixes:

Fixes
-----

* `purge/discard_all` wasn't working correctly (Issue #455).

* The coloring of log messages didn't handle non-ASCII data well
  (Issue #427).

* [Windows] the multiprocessing pool tried to import ``os.kill``
  even though this isn't available there (Issue #450).

* Fixes case where the worker could become unresponsive because of tasks
  exceeding the hard time limit.

* The :event:`task-sent` event was missing from the event reference.

* ``ResultSet.iterate`` now returns results as they finish (Issue #459).

    This wasn't the case previously, even though the documentation
    states this was the expected behavior.

* Retries will no longer be performed when tasks are called directly
  (using ``__call__``).

   Instead the exception passed to ``retry`` will be re-raised.

* Eventlet no longer crashes if autoscale is enabled.

    growing and shrinking eventlet pools is still not supported.

* ``py24`` target removed from :file:`tox.ini`.


.. _version-2.3.1:

2.3.1
=====
:release-date: 2011-08-07 08:00 p.m. BST
:release-by: Ask Solem

Fixes
-----

* The :setting:`CELERY_AMQP_TASK_RESULT_EXPIRES` setting didn't work,
  resulting in an AMQP related error about not being able to serialize
  floats while trying to publish task states (Issue #446).

.. _version-2.3.0:

2.3.0
=====
:release-date: 2011-08-05 12:00 p.m. BST
:tested: CPython: 2.5, 2.6, 2.7; PyPy: 1.5; Jython: 2.5.2
:release-by: Ask Solem

.. _v230-important:

Important Notes
---------------

* Now requires Kombu 1.2.1

* Results are now disabled by default.

    The AMQP backend wasn't a good default because often the users were
    not consuming the results, resulting in thousands of queues.

    While the queues can be configured to expire if left unused, it wasn't
    possible to enable this by default because this was only available in
    recent RabbitMQ versions (2.1.1+)

    With this change enabling a result backend will be a conscious choice,
    which will hopefully lead the user to read the documentation and be aware
    of any common pitfalls with the particular backend.

    The default backend is now a dummy backend
    (:class:`celery.backends.base.DisabledBackend`). Saving state is simply an
    no-op, and AsyncResult.wait(), .result, .state, etc. will raise
    a :exc:`NotImplementedError` telling the user to configure the result backend.

    For help choosing a backend please see :ref:`task-result-backends`.

    If you depend on the previous default which was the AMQP backend, then
    you have to set this explicitly before upgrading::

        CELERY_RESULT_BACKEND = 'amqp'

    .. note::

        For :pypi:`django-celery` users the default backend is
        still ``database``, and results are not disabled by default.

* The Debian init-scripts have been deprecated in favor of the generic-init.d
  init-scripts.

    In addition generic init-scripts for ``celerybeat`` and ``celeryev`` has
    been added.

.. _v230-news:

News
----

* Automatic connection pool support.

    The pool is used by everything that requires a broker connection, for
    example calling tasks, sending broadcast commands, retrieving results
    with the AMQP result backend, and so on.

    The pool is disabled by default, but you can enable it by configuring the
    :setting:`BROKER_POOL_LIMIT` setting::

        BROKER_POOL_LIMIT = 10

    A limit of 10 means a maximum of 10 simultaneous connections can co-exist.
    Only a single connection will ever be used in a single-thread
    environment, but in a concurrent environment (threads, greenlets, etc., but
    not processes) when the limit has been exceeded, any try to acquire a
    connection will block the thread and wait for a connection to be released.
    This is something to take into consideration when choosing a limit.

    A limit of :const:`None` or 0 means no limit, and connections will be
    established and closed every time.

* Introducing Chords (taskset callbacks).

    A chord is a task that only executes after all of the tasks in a taskset
    has finished executing. It's a fancy term for "taskset callbacks"
    adopted from
    `Cω  <http://research.microsoft.com/en-us/um/cambridge/projects/comega/>`_).

    It works with all result backends, but the best implementation is
    currently provided by the Redis result backend.

    Here's an example chord::

        >>> chord(add.subtask((i, i))
        ...         for i in xrange(100))(tsum.subtask()).get()
        9900

    Please read the :ref:`Chords section in the user guide <canvas-chord>`, if you
    want to know more.

* Time limits can now be set for individual tasks.

    To set the soft and hard time limits for a task use the ``time_limit``
    and ``soft_time_limit`` attributes:

    .. code-block:: python

        import time

        @task(time_limit=60, soft_time_limit=30)
        def sleeptask(seconds):
            time.sleep(seconds)

    If the attributes are not set, then the workers default time limits
    will be used.

    New in this version you can also change the time limits for a task
    at runtime using the :func:`time_limit` remote control command::

        >>> from celery.task import control
        >>> control.time_limit('tasks.sleeptask',
        ...                    soft=60, hard=120, reply=True)
        [{'worker1.example.com': {'ok': 'time limits set successfully'}}]

    Only tasks that starts executing after the time limit change will be affected.

    .. note::

        Soft time limits will still not work on Windows or other platforms
        that don't have the ``SIGUSR1`` signal.

* Redis backend configuration directive names changed to include the
   ``CELERY_`` prefix.


    =====================================  ===================================
    **Old setting name**                   **Replace with**
    =====================================  ===================================
    `REDIS_HOST`                           `CELERY_REDIS_HOST`
    `REDIS_PORT`                           `CELERY_REDIS_PORT`
    `REDIS_DB`                             `CELERY_REDIS_DB`
    `REDIS_PASSWORD`                       `CELERY_REDIS_PASSWORD`
    =====================================  ===================================

    The old names are still supported but pending deprecation.

* PyPy: The default pool implementation used is now multiprocessing
  if running on PyPy 1.5.

* multi: now supports "pass through" options.

    Pass through options makes it easier to use Celery without a
    configuration file, or just add last-minute options on the command
    line.

    Example use:

    .. code-block:: console

        $ celery multi start 4  -c 2  -- broker.host=amqp.example.com \
                                         broker.vhost=/               \
                                         celery.disable_rate_limits=yes

* ``celerybeat``: Now retries establishing the connection (Issue #419).

* ``celeryctl``: New ``list bindings`` command.

    Lists the current or all available bindings, depending on the
    broker transport used.

* Heartbeat is now sent every 30 seconds (previously every 2 minutes).

* ``ResultSet.join_native()`` and ``iter_native()`` is now supported by
  the Redis and Cache result backends.

    This is an optimized version of ``join()`` using the underlying
    backends ability to fetch multiple results at once.

* Can now use SSL when sending error e-mails by enabling the
  :setting:`EMAIL_USE_SSL` setting.

* ``events.default_dispatcher()``: Context manager to easily obtain
  an event dispatcher instance using the connection pool.

* Import errors in the configuration module won't be silenced anymore.

* ResultSet.iterate:  Now supports the ``timeout``, ``propagate`` and
  ``interval`` arguments.

* ``with_default_connection`` ->  ``with default_connection``

* TaskPool.apply_async:  Keyword arguments ``callbacks`` and ``errbacks``
  has been renamed to ``callback`` and ``errback`` and take a single scalar
  value instead of a list.

* No longer propagates errors occurring during process cleanup (Issue #365)

* Added ``TaskSetResult.delete()``, which will delete a previously
  saved taskset result.

* ``celerybeat`` now syncs every 3 minutes instead of only at
  shutdown (Issue #382).

* Monitors now properly handles unknown events, so user-defined events
  are displayed.

* Terminating a task on Windows now also terminates all of the tasks child
  processes (Issue #384).

* worker: ``-I|--include`` option now always searches the current directory
  to import the specified modules.

* Cassandra backend: Now expires results by using TTLs.

* Functional test suite in ``funtests`` is now actually working properly, and
  passing tests.

.. _v230-fixes:

Fixes
-----

* ``celeryev`` was trying to create the pidfile twice.

* celery.contrib.batches: Fixed problem where tasks failed
  silently (Issue #393).

* Fixed an issue where logging objects would give "<Unrepresentable",
  even though the objects were.

* ``CELERY_TASK_ERROR_WHITE_LIST`` is now properly initialized
  in all loaders.

* ``celeryd_detach`` now passes through command line configuration.

* Remote control command ``add_consumer`` now does nothing if the
  queue is already being consumed from.

