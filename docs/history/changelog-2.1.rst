.. _changelog-2.1:

===============================
 Change history for Celery 2.1
===============================

.. contents::
    :local:

.. _version-2.1.4:

2.1.4
=====
:release-date: 2010-12-03 12:00 p.m. CEST
:release-by: Ask Solem

.. _v214-fixes:

Fixes
-----

* Execution options to `apply_async` now takes precedence over options
  returned by active routers. This was a regression introduced recently
  (Issue #244).

* curses monitor: Long arguments are now truncated so curses
  doesn't crash with out of bounds errors (Issue #235).

* multi: Channel errors occurring while handling control commands no
  longer crash the worker but are instead logged with severity error.

* SQLAlchemy database backend: Fixed a race condition occurring when
  the client wrote the pending state. Just like the Django database backend,
  it does no longer save the pending state (Issue #261 + Issue #262).

* Error email body now uses `repr(exception)` instead of `str(exception)`,
  as the latter could result in Unicode decode errors (Issue #245).

* Error email timeout value is now configurable by using the
  :setting:`EMAIL_TIMEOUT` setting.

* `celeryev`: Now works on Windows (but the curses monitor won't work without
  having curses).

* Unit test output no longer emits non-standard characters.

* worker: The broadcast consumer is now closed if the connection is reset.

* worker: Now properly handles errors occurring while trying to acknowledge
  the message.

* `TaskRequest.on_failure` now encodes traceback using the current file-system
   encoding (Issue #286).

* `EagerResult` can now be pickled (Issue #288).

.. _v214-documentation:

Documentation
-------------

* Adding :ref:`contributing`.

* Added :ref:`guide-optimizing`.

* Added :ref:`faq-security` section to the FAQ.

.. _version-2.1.3:

2.1.3
=====
:release-date: 2010-11-09 05:00 p.m. CEST
:release-by: Ask Solem

.. _v213-fixes:

* Fixed deadlocks in `timer2` which could lead to `djcelerymon`/`celeryev -c`
  hanging.

* `EventReceiver`: now sends heartbeat request to find workers.

    This means :program:`celeryev` and friends finds workers immediately
    at start-up.

* ``celeryev`` curses monitor: Set screen_delay to 10ms, so the screen
  refreshes more often.

* Fixed pickling errors when pickling :class:`AsyncResult` on older Python
  versions.

* worker: prefetch count was decremented by ETA tasks even if there
  were no active prefetch limits.


.. _version-2.1.2:

2.1.2
=====
:release-data: TBA

.. _v212-fixes:

Fixes
-----

* worker: Now sends the :event:`task-retried` event for retried tasks.

* worker: Now honors ignore result for
  :exc:`~@WorkerLostError` and timeout errors.

* ``celerybeat``: Fixed :exc:`UnboundLocalError` in ``celerybeat`` logging
  when using logging setup signals.

* worker: All log messages now includes `exc_info`.

.. _version-2.1.1:

2.1.1
=====
:release-date: 2010-10-14 02:00 p.m. CEST
:release-by: Ask Solem

.. _v211-fixes:

Fixes
-----

* Now working on Windows again.

   Removed dependency on the :mod:`pwd`/:mod:`grp` modules.

* snapshots: Fixed race condition leading to loss of events.

* worker: Reject tasks with an ETA that cannot be converted to a time stamp.

    See issue #209

* concurrency.processes.pool: The semaphore was released twice for each task
  (both at ACK and result ready).

    This has been fixed, and it is now released only once per task.

* docs/configuration: Fixed typo `CELERYD_TASK_SOFT_TIME_LIMIT` ->
  :setting:`CELERYD_TASK_SOFT_TIME_LIMIT`.

    See issue #214

* control command `dump_scheduled`: was using old .info attribute

* multi: Fixed `set changed size during iteration` bug
    occurring in the restart command.

* worker: Accidentally tried to use additional command-line arguments.

   This would lead to an error like:

    `got multiple values for keyword argument 'concurrency'`.

    Additional command-line arguments are now ignored, and doesn't
    produce this error. However -- we do reserve the right to use
    positional arguments in the future, so please don't depend on this
    behavior.

* ``celerybeat``: Now respects routers and task execution options again.

* ``celerybeat``: Now reuses the publisher instead of the connection.

* Cache result backend: Using :class:`float` as the expires argument
  to `cache.set` is deprecated by the Memcached libraries,
  so we now automatically cast to :class:`int`.

* unit tests: No longer emits logging and warnings in test output.

.. _v211-news:

News
----

* Now depends on carrot version 0.10.7.

* Added :setting:`CELERY_REDIRECT_STDOUTS`, and
  :setting:`CELERYD_REDIRECT_STDOUTS_LEVEL` settings.

    :setting:`CELERY_REDIRECT_STDOUTS` is used by the worker and
    beat. All output to `stdout` and `stderr` will be
    redirected to the current logger if enabled.

    :setting:`CELERY_REDIRECT_STDOUTS_LEVEL` decides the log level used and is
    :const:`WARNING` by default.

* Added :setting:`CELERYBEAT_SCHEDULER` setting.

    This setting is used to define the default for the -S option to
    :program:`celerybeat`.

    Example:

    .. code-block:: python

        CELERYBEAT_SCHEDULER = 'djcelery.schedulers.DatabaseScheduler'

* Added Task.expires: Used to set default expiry time for tasks.

* New remote control commands: `add_consumer` and `cancel_consumer`.

    .. method:: add_consumer(queue, exchange, exchange_type, routing_key,
                             \*\*options)
        :module:

        Tells the worker to declare and consume from the specified
        declaration.

    .. method:: cancel_consumer(queue_name)
        :module:

        Tells the worker to stop consuming from queue (by queue name).


    Commands also added to :program:`celeryctl` and
    :class:`~celery.task.control.inspect`.


    Example using ``celeryctl`` to start consuming from queue "queue", in
    exchange "exchange", of type "direct" using binding key "key":

    .. code-block:: console

        $ celeryctl inspect add_consumer queue exchange direct key
        $ celeryctl inspect cancel_consumer queue

    See :ref:`monitoring-control` for more information about the
    :program:`celeryctl` program.


    Another example using :class:`~celery.task.control.inspect`:

    .. code-block:: pycon

        >>> from celery.task.control import inspect
        >>> inspect.add_consumer(queue='queue', exchange='exchange',
        ...                      exchange_type='direct',
        ...                      routing_key='key',
        ...                      durable=False,
        ...                      auto_delete=True)

        >>> inspect.cancel_consumer('queue')

* ``celerybeat``: Now logs the traceback if a message can't be sent.

* ``celerybeat``: Now enables a default socket timeout of 30 seconds.

* ``README``/introduction/homepage: Added link to `Flask-Celery`_.

.. _`Flask-Celery`: https://github.com/ask/flask-celery

.. _version-2.1.0:

2.1.0
=====
:release-date: 2010-10-08 12:00 p.m. CEST
:release-by: Ask Solem

.. _v210-important:

Important Notes
---------------

* Celery is now following the versioning semantics defined by `semver`_.

    This means we're no longer allowed to use odd/even versioning semantics
    By our previous versioning scheme this stable release should've
    been version 2.2.

.. _`semver`: http://semver.org

* Now depends on Carrot 0.10.7.

* No longer depends on SQLAlchemy, this needs to be installed separately
  if the database result backend is used.

* :pypi:`django-celery` now comes with a monitor for the Django Admin
  interface. This can also be used if you're not a Django user.
  (Update: Django-Admin monitor has been replaced with Flower, see the
  Monitoring guide).

* If you get an error after upgrading saying:
  `AttributeError: 'module' object has no attribute 'system'`,

    Then this is because the `celery.platform` module has been
    renamed to `celery.platforms` to not collide with the built-in
    :mod:`platform` module.

    You have to remove the old :file:`platform.py` (and maybe
    :file:`platform.pyc`) file from your previous Celery installation.

    To do this use :program:`python` to find the location
    of this module:

    .. code-block:: console

        $ python
        >>> import celery.platform
        >>> celery.platform
        <module 'celery.platform' from '/opt/devel/celery/celery/platform.pyc'>

    Here the compiled module is in :file:`/opt/devel/celery/celery/`,
    to remove the offending files do:

    .. code-block:: console

        $ rm -f /opt/devel/celery/celery/platform.py*

.. _v210-news:

News
----

* Added support for expiration of AMQP results (requires RabbitMQ 2.1.0)

    The new configuration option :setting:`CELERY_AMQP_TASK_RESULT_EXPIRES`
    sets the expiry time in seconds (can be int or float):

    .. code-block:: python

        CELERY_AMQP_TASK_RESULT_EXPIRES = 30 * 60  # 30 minutes.
        CELERY_AMQP_TASK_RESULT_EXPIRES = 0.80     # 800 ms.

* ``celeryev``: Event Snapshots

    If enabled, the worker sends messages about what the worker is doing.
    These messages are called "events".
    The events are used by real-time monitors to show what the
    cluster is doing, but they're not very useful for monitoring
    over a longer period of time. Snapshots
    lets you take "pictures" of the clusters state at regular intervals.
    This can then be stored in a database to generate statistics
    with, or even monitoring over longer time periods.

    :pypi:`django-celery` now comes with a Celery monitor for the Django
    Admin interface. To use this you need to run the :pypi:`django-celery`
    snapshot camera, which stores snapshots to the database at configurable
    intervals.

    To use the Django admin monitor you need to do the following:

    1. Create the new database tables:

        .. code-block:: console

            $ python manage.py syncdb

    2. Start the :pypi:`django-celery` snapshot camera:

        .. code-block:: console

            $ python manage.py celerycam

    3. Open up the django admin to monitor your cluster.

    The admin interface shows tasks, worker nodes, and even
    lets you perform some actions, like revoking and rate limiting tasks,
    and shutting down worker nodes.

    There's also a Debian init.d script for :mod:`~celery.bin.events` available,
    see :ref:`daemonizing` for more information.

    New command-line arguments to ``celeryev``:

        * :option:`celery events --camera`: Snapshot camera class to use.
        * :option:`celery events --logfile`: Log file
        * :option:`celery events --loglevel`: Log level
        * :option:`celery events --maxrate`: Shutter rate limit.
        * :option:`celery events --freq`: Shutter frequency

    The :option:`--camera <celery events --camera>` argument is the name
    of a class used to take snapshots with. It must support the interface
    defined by :class:`celery.events.snapshot.Polaroid`.

    Shutter frequency controls how often the camera thread wakes up,
    while the rate limit controls how often it will actually take
    a snapshot.
    The rate limit can be an integer (snapshots/s), or a rate limit string
    which has the same syntax as the task rate limit strings (`"200/m"`,
    `"10/s"`, `"1/h",` etc).

    For the Django camera case, this rate limit can be used to control
    how often the snapshots are written to the database, and the frequency
    used to control how often the thread wakes up to check if there's
    anything new.

    The rate limit is off by default, which means it will take a snapshot
    for every :option:`--frequency <celery events --frequency>` seconds.

* :func:`~celery.task.control.broadcast`: Added callback argument, this can be
  used to process replies immediately as they arrive.

* ``celeryctl``: New command line utility to manage and inspect worker nodes,
  apply tasks and inspect the results of tasks.

    .. seealso::

        The :ref:`monitoring-control` section in the :ref:`guide`.

    Some examples:

    .. code-block:: console

        $ celeryctl apply tasks.add -a '[2, 2]' --countdown=10

        $ celeryctl inspect active
        $ celeryctl inspect registered_tasks
        $ celeryctl inspect scheduled
        $ celeryctl inspect --help
        $ celeryctl apply --help

* Added the ability to set an expiry date and time for tasks.

    Example::

        >>> # Task expires after one minute from now.
        >>> task.apply_async(args, kwargs, expires=60)
        >>> # Also supports datetime
        >>> task.apply_async(args, kwargs,
        ...                  expires=datetime.now() + timedelta(days=1)

    When a worker receives a task that's been expired it will be
    marked as revoked (:exc:`~@TaskRevokedError`).

* Changed the way logging is configured.

    We now configure the root logger instead of only configuring
    our custom logger. In addition we don't hijack
    the multiprocessing logger anymore, but instead use a custom logger name
    for different applications:

    =====================================  =====================================
    **Application**                        **Logger Name**
    =====================================  =====================================
    ``celeryd``                            ``"celery"``
    ``celerybeat``                         ``"celery.beat"``
    ``celeryev``                           ``"celery.ev"``
    =====================================  =====================================

    This means that the `loglevel` and `logfile` arguments will
    affect all registered loggers (even those from third-party libraries).
    Unless you configure the loggers manually as shown below, that is.

    *Users can choose to configure logging by subscribing to the
    :signal:`~celery.signals.setup_logging` signal:*

    .. code-block:: python

        from logging.config import fileConfig
        from celery import signals

        @signals.setup_logging.connect
        def setup_logging(**kwargs):
            fileConfig('logging.conf')

    If there are no receivers for this signal, the logging subsystem
    will be configured using the
    :option:`--loglevel <celery worker --loglevel>`/
    :option:`--logfile <celery worker --logfile>`
    arguments, this will be used for *all defined loggers*.

    Remember that the worker also redirects stdout and stderr
    to the Celery logger, if manually configure logging
    you also need to redirect the standard outs manually:

    .. code-block:: python

        from logging.config import fileConfig
        from celery import log

       def setup_logging(**kwargs):
            import logging
            fileConfig('logging.conf')
            stdouts = logging.getLogger('mystdoutslogger')
            log.redirect_stdouts_to_logger(stdouts, loglevel=logging.WARNING)

* worker Added command line option
  :option:`--include <celery worker --include>`:

    A comma separated list of (task) modules to be imported.

    Example:

    .. code-block:: console

        $ celeryd -I app1.tasks,app2.tasks

* worker: now emits a warning if running as the root user (euid is 0).

* :func:`celery.messaging.establish_connection`: Ability to override defaults
  used using keyword argument "defaults".

* worker: Now uses `multiprocessing.freeze_support()` so that it should work
  with **py2exe**, **PyInstaller**, **cx_Freeze**, etc.

* worker: Now includes more meta-data for the :state:`STARTED` state: PID and
  host name of the worker that started the task.

    See issue #181

* subtask: Merge additional keyword arguments to `subtask()` into task keyword
  arguments.

    For example:

    .. code-block:: pycon

        >>> s = subtask((1, 2), {'foo': 'bar'}, baz=1)
        >>> s.args
        (1, 2)
        >>> s.kwargs
        {'foo': 'bar', 'baz': 1}

    See issue #182.

* worker: Now emits a warning if there's already a worker node using the same
  name running on the same virtual host.

* AMQP result backend: Sending of results are now retried if the connection
  is down.

* AMQP result backend: `result.get()`: Wait for next state if state isn't
    in :data:`~celery.states.READY_STATES`.

* TaskSetResult now supports subscription.

    ::

        >>> res = TaskSet(tasks).apply_async()
        >>> res[0].get()

* Added `Task.send_error_emails` + `Task.error_whitelist`, so these can
  be configured per task instead of just by the global setting.

* Added `Task.store_errors_even_if_ignored`, so it can be changed per Task,
  not just by the global setting.

* The Crontab scheduler no longer wakes up every second, but implements
  `remaining_estimate` (*Optimization*).

* worker:  Store :state:`FAILURE` result if the
   :exc:`~@WorkerLostError` exception occurs (worker process
   disappeared).

* worker: Store :state:`FAILURE` result if one of the `*TimeLimitExceeded`
  exceptions occurs.

* Refactored the periodic task responsible for cleaning up results.

    * The backend cleanup task is now only added to the schedule if
        :setting:`CELERY_TASK_RESULT_EXPIRES` is set.

    * If the schedule already contains a periodic task named
      "celery.backend_cleanup" it won't change it, so the behavior of the
      backend cleanup task can be easily changed.

    * The task is now run every day at 4:00 AM, rather than every day since
      the first time it was run (using Crontab schedule instead of
      `run_every`)

    * Renamed `celery.task.builtins.DeleteExpiredTaskMetaTask`
        -> :class:`celery.task.builtins.backend_cleanup`

    * The task itself has been renamed from "celery.delete_expired_task_meta"
      to "celery.backend_cleanup"

    See issue #134.

* Implemented `AsyncResult.forget` for SQLAlchemy/Memcached/Redis/Tokyo Tyrant
  backends (forget and remove task result).

    See issue #184.

* :meth:`TaskSetResult.join <celery.result.TaskSetResult.join>`:
  Added 'propagate=True' argument.

  When set to :const:`False` exceptions occurring in subtasks will
  not be re-raised.

* Added `Task.update_state(task_id, state, meta)`
  as a shortcut to `task.backend.store_result(task_id, meta, state)`.

    The backend interface is "private" and the terminology outdated,
    so better to move this to :class:`~celery.task.base.Task` so it can be
    used.

* timer2: Set `self.running=False` in
  :meth:`~celery.utils.timer2.Timer.stop` so it won't try to join again on
  subsequent calls to `stop()`.

* Log colors are now disabled by default on Windows.

* `celery.platform` renamed to :mod:`celery.platforms`, so it doesn't
  collide with the built-in :mod:`platform` module.

* Exceptions occurring in Mediator+Pool callbacks are now caught and logged
  instead of taking down the worker.

* Redis result backend: Now supports result expiration using the Redis
  `EXPIRE` command.

* unit tests: Don't leave threads running at tear down.

* worker: Task results shown in logs are now truncated to 46 chars.

* `Task.__name__` is now an alias to `self.__class__.__name__`.
   This way tasks introspects more like regular functions.

* `Task.retry`: Now raises :exc:`TypeError` if kwargs argument is empty.

    See issue #164.

* ``timedelta_seconds``: Use ``timedelta.total_seconds`` if running on Python 2.7

* :class:`~kombu.utils.limits.TokenBucket`: Generic Token Bucket algorithm

* :mod:`celery.events.state`: Recording of cluster state can now
  be paused and resumed, including support for buffering.


    .. method:: State.freeze(buffer=True)

        Pauses recording of the stream.

        If `buffer` is true, events received while being frozen will be
        buffered, and may be replayed later.

    .. method:: State.thaw(replay=True)

        Resumes recording of the stream.

        If `replay` is true, then the recorded buffer will be applied.

    .. method:: State.freeze_while(fun)

        With a function to apply, freezes the stream before,
        and replays the buffer after the function returns.

* :meth:`EventReceiver.capture <celery.events.EventReceiver.capture>`
  Now supports a timeout keyword argument.

* worker: The mediator thread is now disabled if
  :setting:`CELERY_RATE_LIMITS` is enabled, and tasks are directly sent to the
  pool without going through the ready queue (*Optimization*).

.. _v210-fixes:

Fixes
-----

* Pool: Process timed out by `TimeoutHandler` must be joined by the Supervisor,
  so don't remove it from the internal process list.

    See issue #192.

* `TaskPublisher.delay_task` now supports exchange argument, so exchange can be
  overridden when sending tasks in bulk using the same publisher

    See issue #187.

* the worker no longer marks tasks as revoked if :setting:`CELERY_IGNORE_RESULT`
  is enabled.

    See issue #207.

* AMQP Result backend: Fixed bug with `result.get()` if
  :setting:`CELERY_TRACK_STARTED` enabled.

    `result.get()` would stop consuming after receiving the
    :state:`STARTED` state.

* Fixed bug where new processes created by the pool supervisor becomes stuck
  while reading from the task Queue.

    See http://bugs.python.org/issue10037

* Fixed timing issue when declaring the remote control command reply queue

    This issue could result in replies being lost, but have now been fixed.

* Backward compatible `LoggerAdapter` implementation: Now works for Python 2.4.

    Also added support for several new methods:
    `fatal`, `makeRecord`, `_log`, `log`, `isEnabledFor`,
    `addHandler`, `removeHandler`.

.. _v210-experimental:

Experimental
------------

* multi: Added daemonization support.

    multi can now be used to start, stop and restart worker nodes:

    .. code-block:: console

        $ celeryd-multi start jerry elaine george kramer

    This also creates PID files and log files (:file:`celeryd@jerry.pid`,
    ..., :file:`celeryd@jerry.log`. To specify a location for these files
    use the `--pidfile` and `--logfile` arguments with the `%n`
    format:

    .. code-block:: console

        $ celeryd-multi start jerry elaine george kramer \
                        --logfile=/var/log/celeryd@%n.log \
                        --pidfile=/var/run/celeryd@%n.pid

    Stopping:

    .. code-block:: console

        $ celeryd-multi stop jerry elaine george kramer

    Restarting. The nodes will be restarted one by one as the old ones
    are shutdown:

    .. code-block:: console

        $ celeryd-multi restart jerry elaine george kramer

    Killing the nodes (**WARNING**: Will discard currently executing tasks):

    .. code-block:: console

        $ celeryd-multi kill jerry elaine george kramer

    See `celeryd-multi help` for help.

* multi: `start` command renamed to `show`.

    `celeryd-multi start` will now actually start and detach worker nodes.
    To just generate the commands you have to use `celeryd-multi show`.

* worker: Added `--pidfile` argument.

   The worker will write its pid when it starts. The worker will
   not be started if this file exists and the pid contained is still alive.

* Added generic init.d script using `celeryd-multi`

    https://github.com/celery/celery/tree/master/extra/generic-init.d/celeryd

.. _v210-documentation:

Documentation
-------------

* Added User guide section: Monitoring

* Added user guide section: Periodic Tasks

    Moved from `getting-started/periodic-tasks` and updated.

* tutorials/external moved to new section: "community".

* References has been added to all sections in the documentation.

    This makes it easier to link between documents.


