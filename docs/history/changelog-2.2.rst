.. _changelog-2.2:

===============================
 Change history for Celery 2.2
===============================

.. contents::
    :local:

.. _version-2.2.8:

2.2.8
=====
:release-date: 2011-11-25 04:00 p.m. GMT
:release-by: Ask Solem

.. _v228-security-fixes:

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

.. _version-2.2.7:

2.2.7
=====
:release-date: 2011-06-13 04:00 p.m. BST
:release-by: Ask Solem

* New signals: :signal:`after_setup_logger` and
  :signal:`after_setup_task_logger`

    These signals can be used to augment logging configuration
    after Celery has set up logging.

* Redis result backend now works with Redis 2.4.4.

* multi: The :option:`--gid <celery --gid>` option now works correctly.

* worker: Retry wrongfully used the repr of the traceback instead
  of the string representation.

* App.config_from_object: Now loads module, not attribute of module.

* Fixed issue where logging of objects would give "<Unrepresentable: ...>"

.. _version-2.2.6:

2.2.6
=====
:release-date: 2011-04-15 04:00 p.m. CEST
:release-by: Ask Solem

.. _v226-important:

Important Notes
---------------

* Now depends on :pypi:`Kombu` 1.1.2.

* Dependency lists now explicitly specifies that we don't want
  :pypi:`python-dateutil` 2.x, as this version only supports Python 3.

    If you have installed dateutil 2.0 by accident you should downgrade
    to the 1.5.0 version:

    .. code-block:: console

        $ pip install -U python-dateutil==1.5.0

    or by ``easy_install``:

    .. code-block:: console

        $ easy_install -U python-dateutil==1.5.0

.. _v226-fixes:

Fixes
-----

* The new ``WatchedFileHandler`` broke Python 2.5 support (Issue #367).

* Task: Don't use ``app.main`` if the task name is set explicitly.

* Sending emails didn't work on Python 2.5, due to a bug in
  the version detection code (Issue #378).

* Beat: Adds method ``ScheduleEntry._default_now``

    This method can be overridden to change the default value
    of ``last_run_at``.

* An error occurring in process cleanup could mask task errors.

  We no longer propagate errors happening at process cleanup,
  but log them instead. This way they won't interfere with publishing
  the task result (Issue #365).

* Defining tasks didn't work properly when using the Django
  ``shell_plus`` utility (Issue #366).

* ``AsyncResult.get`` didn't accept the ``interval`` and ``propagate``
   arguments.

* worker: Fixed a bug where the worker wouldn't shutdown if a
   :exc:`socket.error` was raised.

.. _version-2.2.5:

2.2.5
=====
:release-date: 2011-03-28 06:00 p.m. CEST
:release-by: Ask Solem

.. _v225-important:

Important Notes
---------------

* Now depends on Kombu 1.0.7

.. _v225-news:

News
----

* Our documentation is now hosted by Read The Docs
  (http://docs.celeryproject.org), and all links have been changed to point to
  the new URL.

* Logging: Now supports log rotation using external tools like `logrotate.d`_
  (Issue #321)

    This is accomplished by using the ``WatchedFileHandler``, which re-opens
    the file if it's renamed or deleted.

.. _`logrotate.d`:
    http://www.ducea.com/2006/06/06/rotating-linux-log-files-part-2-logrotate/

* ``otherqueues`` tutorial now documents how to configure Redis/Database result
   backends.

* gevent: Now supports ETA tasks.

    But gevent still needs ``CELERY_DISABLE_RATE_LIMITS=True`` to work.

* TaskSet User Guide: now contains TaskSet callback recipes.

* Eventlet: New signals:

    * ``eventlet_pool_started``
    * ``eventlet_pool_preshutdown``
    * ``eventlet_pool_postshutdown``
    * ``eventlet_pool_apply``

    See :mod:`celery.signals` for more information.

* New :setting:`BROKER_TRANSPORT_OPTIONS` setting can be used to pass
  additional arguments to a particular broker transport.

* worker: ``worker_pid`` is now part of the request info as returned by
  broadcast commands.

* TaskSet.apply/Taskset.apply_async now accepts an optional ``taskset_id``
  argument.

* The taskset_id (if any) is now available in the Task request context.

* SQLAlchemy result backend: taskset_id and taskset_id columns now have a
  unique constraint (tables need to recreated for this to take affect).

* Task user guide: Added section about choosing a result backend.

* Removed unused attribute ``AsyncResult.uuid``.

.. _v225-fixes:

Fixes
-----

* multiprocessing.Pool:  Fixes race condition when marking job with
  ``WorkerLostError`` (Issue #268).

    The process may have published a result before it was terminated,
    but we have no reliable way to detect that this is the case.

    So we have to wait for 10 seconds before marking the result with
    WorkerLostError. This gives the result handler a chance to retrieve the
    result.

* multiprocessing.Pool: Shutdown could hang if rate limits disabled.

    There was a race condition when the MainThread was waiting for the pool
    semaphore to be released. The ResultHandler now terminates after 5
    seconds if there are unacked jobs, but no worker processes left to start
    them  (it needs to timeout because there could still be an ack+result
    that we haven't consumed from the result queue. It
    is unlikely we'll receive any after 5 seconds with no worker processes).

* ``celerybeat``: Now creates pidfile even if the ``--detach`` option isn't set.

* eventlet/gevent: The broadcast command consumer is now running in a separate
  green-thread.

    This ensures broadcast commands will take priority even if there are many
    active tasks.

* Internal module ``celery.worker.controllers`` renamed to
  ``celery.worker.mediator``.

* worker: Threads now terminates the program by calling ``os._exit``, as it
  is the only way to ensure exit in the case of syntax errors, or other
  unrecoverable errors.

* Fixed typo in ``maybe_timedelta`` (Issue #352).

* worker: Broadcast commands now logs with loglevel debug instead of warning.

* AMQP Result Backend: Now resets cached channel if the connection is lost.

* Polling results with the AMQP result backend wasn't working properly.

* Rate limits: No longer sleeps if there are no tasks, but rather waits for
  the task received condition (Performance improvement).

* ConfigurationView: ``iter(dict)`` should return keys, not items (Issue #362).

* ``celerybeat``:  PersistentScheduler now automatically removes a corrupted
  schedule file (Issue #346).

* Programs that doesn't support positional command-line arguments now provides
  a user friendly error message.

* Programs no longer tries to load the configuration file when showing
  ``--version`` (Issue #347).

* Autoscaler: The "all processes busy" log message is now severity debug
  instead of error.

* worker: If the message body can't be decoded, it's now passed through
  ``safe_str`` when logging.

    This to ensure we don't get additional decoding errors when trying to log
    the failure.

* ``app.config_from_object``/``app.config_from_envvar`` now works for all
  loaders.

* Now emits a user-friendly error message if the result backend name is
  unknown (Issue #349).

* ``celery.contrib.batches``: Now sets loglevel and logfile in the task
  request so ``task.get_logger`` works with batch tasks (Issue #357).

* worker: An exception was raised if using the amqp transport and the prefetch
  count value exceeded 65535 (Issue #359).

    The prefetch count is incremented for every received task with an
    ETA/countdown defined. The prefetch count is a short, so can only support
    a maximum value of 65535. If the value exceeds the maximum value we now
    disable the prefetch count, it's re-enabled as soon as the value is below
    the limit again.

* ``cursesmon``: Fixed unbound local error (Issue #303).

* eventlet/gevent is now imported on demand so autodoc can import the modules
  without having eventlet/gevent installed.

* worker: Ack callback now properly handles ``AttributeError``.

* ``Task.after_return`` is now always called *after* the result has been
  written.

* Cassandra Result Backend: Should now work with the latest ``pycassa``
  version.

* multiprocessing.Pool: No longer cares if the ``putlock`` semaphore is released
  too many times (this can happen if one or more worker processes are
  killed).

* SQLAlchemy Result Backend: Now returns accidentally removed ``date_done`` again
  (Issue #325).

* Task.request context is now always initialized to ensure calling the task
  function directly works even if it actively uses the request context.

* Exception occurring when iterating over the result from ``TaskSet.apply``
  fixed.

* eventlet: Now properly schedules tasks with an ETA in the past.

.. _version-2.2.4:

2.2.4
=====
:release-date: 2011-02-19 00:00 AM CET
:release-by: Ask Solem

.. _v224-fixes:

Fixes
-----

* worker: 2.2.3 broke error logging, resulting in tracebacks not being logged.

* AMQP result backend: Polling task states didn't work properly if there were
  more than one result message in the queue.

* ``TaskSet.apply_async()`` and ``TaskSet.apply()`` now supports an optional
  ``taskset_id`` keyword argument (Issue #331).

* The current taskset id (if any) is now available in the task context as
  ``request.taskset`` (Issue #329).

* SQLAlchemy result backend: `date_done` was no longer part of the results as it had
  been accidentally removed. It's now available again (Issue #325).

* SQLAlchemy result backend: Added unique constraint on `Task.id` and
  `TaskSet.taskset_id`. Tables needs to be recreated for this to take effect.

* Fixed exception raised when iterating on the result of ``TaskSet.apply()``.

* Tasks user guide: Added section on choosing a result backend.

.. _version-2.2.3:

2.2.3
=====
:release-date: 2011-02-12 04:00 p.m. CET
:release-by: Ask Solem

.. _v223-fixes:

Fixes
-----

* Now depends on :pypi:`Kombu` 1.0.3

* Task.retry now supports a ``max_retries`` argument, used to change the
  default value.

* `multiprocessing.cpu_count` may raise :exc:`NotImplementedError` on
  platforms where this isn't supported (Issue #320).

* Coloring of log messages broke if the logged object wasn't a string.

* Fixed several typos in the init-script documentation.

* A regression caused `Task.exchange` and `Task.routing_key` to no longer
  have any effect. This is now fixed.

* Routing user guide: Fixes typo, routers in :setting:`CELERY_ROUTES` must be
  instances, not classes.

* :program:`celeryev` didn't create pidfile even though the
  :option:`--pidfile <celery events --pidfile>` argument was set.

* Task logger format was no longer used (Issue #317).

   The id and name of the task is now part of the log message again.

* A safe version of ``repr()`` is now used in strategic places to ensure
  objects with a broken ``__repr__`` doesn't crash the worker, or otherwise
  make errors hard to understand (Issue #298).

* Remote control command :control:`active_queues`: didn't account for queues added
  at runtime.

    In addition the dictionary replied by this command now has a different
    structure: the exchange key is now a dictionary containing the
    exchange declaration in full.

* The :option:`celery worker -Q` option removed unused queue
  declarations, so routing of tasks could fail.

    Queues are no longer removed, but rather `app.amqp.queues.consume_from()`
    is used as the list of queues to consume from.

    This ensures all queues are available for routing purposes.

* ``celeryctl``: Now supports the `inspect active_queues` command.

.. _version-2.2.2:

2.2.2
=====
:release-date: 2011-02-03 04:00 p.m. CET
:release-by: Ask Solem

.. _v222-fixes:

Fixes
-----

* ``celerybeat`` couldn't read the schedule properly, so entries in
  :setting:`CELERYBEAT_SCHEDULE` wouldn't be scheduled.

* Task error log message now includes `exc_info` again.

* The `eta` argument can now be used with `task.retry`.

    Previously it was overwritten by the countdown argument.

* ``celery multi``/``celeryd_detach``: Now logs errors occurring when executing
  the `celery worker` command.

* daemonizing tutorial: Fixed typo ``--time-limit 300`` ->
  ``--time-limit=300``

* Colors in logging broke non-string objects in log messages.

* ``setup_task_logger`` no longer makes assumptions about magic task kwargs.

.. _version-2.2.1:

2.2.1
=====
:release-date: 2011-02-02 04:00 p.m. CET
:release-by: Ask Solem

.. _v221-fixes:

Fixes
-----

* Eventlet pool was leaking memory (Issue #308).

* Deprecated function ``celery.execute.delay_task`` was accidentally removed,
  now available again.

* ``BasePool.on_terminate`` stub didn't exist

* ``celeryd_detach``: Adds readable error messages if user/group name
  doesn't exist.

* Smarter handling of unicode decode errors when logging errors.

.. _version-2.2.0:

2.2.0
=====
:release-date: 2011-02-01 10:00 AM CET
:release-by: Ask Solem

.. _v220-important:

Important Notes
---------------

* Carrot has been replaced with :pypi:`Kombu`

    Kombu is the next generation messaging library for Python,
    fixing several flaws present in Carrot that was hard to fix
    without breaking backwards compatibility.

    Also it adds:

    * First-class support for virtual transports; Redis, Django ORM,
      SQLAlchemy, Beanstalk, MongoDB, CouchDB and in-memory.
    * Consistent error handling with introspection,
    * The ability to ensure that an operation is performed by gracefully
      handling connection and channel errors,
    * Message compression (:mod:`zlib`, :mod:`bz2`, or custom compression schemes).

    This means that `ghettoq` is no longer needed as the
    functionality it provided is already available in Celery by default.
    The virtual transports are also more feature complete with support
    for exchanges (direct and topic). The Redis transport even supports
    fanout exchanges so it's able to perform worker remote control
    commands.

* Magic keyword arguments pending deprecation.

    The magic keyword arguments were responsible for many problems
    and quirks: notably issues with tasks and decorators, and name
    collisions in keyword arguments for the unaware.

    It wasn't easy to find a way to deprecate the magic keyword arguments,
    but we think this is a solution that makes sense and it won't
    have any adverse effects for existing code.

    The path to a magic keyword argument free world is:

        * the `celery.decorators` module is deprecated and the decorators
          can now be found in `celery.task`.
        * The decorators in `celery.task` disables keyword arguments by
          default
        * All examples in the documentation have been changed to use
          `celery.task`.

        This means that the following will have magic keyword arguments
        enabled (old style):

            .. code-block:: python

                from celery.decorators import task

                @task()
                def add(x, y, **kwargs):
                    print('In task %s' % kwargs['task_id'])
                    return x + y

        And this won't use magic keyword arguments (new style):

            .. code-block:: python

                from celery.task import task

                @task()
                def add(x, y):
                    print('In task %s' % add.request.id)
                    return x + y

    In addition, tasks can choose not to accept magic keyword arguments by
    setting the `task.accept_magic_kwargs` attribute.

    .. admonition:: Deprecation

        Using the decorators in :mod:`celery.decorators` emits a
        :class:`PendingDeprecationWarning` with a helpful message urging
        you to change your code, in version 2.4 this will be replaced with
        a :class:`DeprecationWarning`, and in version 4.0 the
        :mod:`celery.decorators` module will be removed and no longer exist.

        Similarly, the `task.accept_magic_kwargs` attribute will no
        longer have any effect starting from version 4.0.

* The magic keyword arguments are now available as `task.request`

    This is called *the context*. Using thread-local storage the
    context contains state that's related to the current request.

    It's mutable and you can add custom attributes that'll only be seen
    by the current task request.

    The following context attributes are always available:

    =====================================  ===================================
    **Magic Keyword Argument**             **Replace with**
    =====================================  ===================================
    `kwargs['task_id']`                    `self.request.id`
    `kwargs['delivery_info']`              `self.request.delivery_info`
    `kwargs['task_retries']`               `self.request.retries`
    `kwargs['logfile']`                    `self.request.logfile`
    `kwargs['loglevel']`                   `self.request.loglevel`
    `kwargs['task_is_eager']`              `self.request.is_eager`
    **NEW**                                `self.request.args`
    **NEW**                                `self.request.kwargs`
    =====================================  ===================================

    In addition, the following methods now automatically uses the current
    context, so you don't have to pass `kwargs` manually anymore:

        * `task.retry`
        * `task.get_logger`
        * `task.update_state`

* `Eventlet`_ support.

    This is great news for I/O-bound tasks!

    To change pool implementations you use the :option:`celery worker --pool`
    argument, or globally using the
    :setting:`CELERYD_POOL` setting. This can be the full name of a class,
    or one of the following aliases: `processes`, `eventlet`, `gevent`.

    For more information please see the :ref:`concurrency-eventlet` section
    in the User Guide.

    .. admonition:: Why not gevent?

        For our first alternative concurrency implementation we've focused
        on `Eventlet`_, but there's also an experimental `gevent`_ pool
        available. This is missing some features, notably the ability to
        schedule ETA tasks.

        Hopefully the `gevent`_ support will be feature complete by
        version 2.3, but this depends on user demand (and contributions).

.. _`Eventlet`: http://eventlet.net
.. _`gevent`: http://gevent.org

* Python 2.4 support deprecated!

    We're happy^H^H^H^H^Hsad to announce that this is the last version
    to support Python 2.4.

    You're urged to make some noise if you're currently stuck with
    Python 2.4. Complain to your package maintainers, sysadmins and bosses:
    tell them it's time to move on!

    Apart from wanting to take advantage of :keyword:`with` statements,
    coroutines, conditional expressions and enhanced :keyword:`try` blocks,
    the code base now contains so many 2.4 related hacks and workarounds
    it's no longer just a compromise, but a sacrifice.

    If it really isn't your choice, and you don't have the option to upgrade
    to a newer version of Python, you can just continue to use Celery 2.2.
    Important fixes can be back ported for as long as there's interest.

* worker: Now supports Autoscaling of child worker processes.

    The :option:`--autoscale <celery worker --autoscale>` option can be used
    to configure the minimum and maximum number of child worker processes:

    .. code-block:: text

        --autoscale=AUTOSCALE
             Enable autoscaling by providing
             max_concurrency,min_concurrency. Example:
               --autoscale=10,3 (always keep 3 processes, but grow to
              10 if necessary).

* Remote Debugging of Tasks

   ``celery.contrib.rdb`` is an extended version of :mod:`pdb` that
   enables remote debugging of processes that doesn't have terminal
   access.

   Example usage:

   .. code-block:: text

        from celery.contrib import rdb
        from celery.task import task

        @task()
        def add(x, y):
            result = x + y
            # set breakpoint
            rdb.set_trace()
            return result

    :func:`~celery.contrib.rdb.set_trace` sets a breakpoint at the current
    location and creates a socket you can telnet into to remotely debug
    your task.

    The debugger may be started by multiple processes at the same time,
    so rather than using a fixed port the debugger will search for an
    available port, starting from the base port (6900 by default).
    The base port can be changed using the environment variable
    :envvar:`CELERY_RDB_PORT`.

    By default the debugger will only be available from the local host,
    to enable access from the outside you have to set the environment
    variable :envvar:`CELERY_RDB_HOST`.

    When the worker encounters your breakpoint it will log the following
    information::

        [INFO/MainProcess] Received task:
            tasks.add[d7261c71-4962-47e5-b342-2448bedd20e8]
        [WARNING/PoolWorker-1] Remote Debugger:6900:
            Please telnet 127.0.0.1 6900.  Type `exit` in session to continue.
        [2011-01-18 14:25:44,119: WARNING/PoolWorker-1] Remote Debugger:6900:
            Waiting for client...

    If you telnet the port specified you'll be presented
    with a ``pdb`` shell:

    .. code-block:: console

        $ telnet localhost 6900
        Connected to localhost.
        Escape character is '^]'.
        > /opt/devel/demoapp/tasks.py(128)add()
        -> return result
        (Pdb)

    Enter ``help`` to get a list of available commands,
    It may be a good idea to read the `Python Debugger Manual`_ if
    you have never used `pdb` before.

.. _`Python Debugger Manual`: http://docs.python.org/library/pdb.html


* Events are now transient and is using a topic exchange (instead of direct).

    The `CELERYD_EVENT_EXCHANGE`, `CELERYD_EVENT_ROUTING_KEY`,
    `CELERYD_EVENT_EXCHANGE_TYPE` settings are no longer in use.

    This means events won't be stored until there's a consumer, and the
    events will be gone as soon as the consumer stops. Also it means there
    can be multiple monitors running at the same time.

    The routing key of an event is the type of event (e.g., `worker.started`,
    `worker.heartbeat`, `task.succeeded`, etc. This means a consumer can
    filter on specific types, to only be alerted of the events it cares about.

    Each consumer will create a unique queue, meaning it's in effect a
    broadcast exchange.

    This opens up a lot of possibilities, for example the workers could listen
    for worker events to know what workers are in the neighborhood, and even
    restart workers when they go down (or use this information to optimize
    tasks/autoscaling).

    .. note::

        The event exchange has been renamed from ``"celeryevent"``
        to ``"celeryev"`` so it doesn't collide with older versions.

        If you'd like to remove the old exchange you can do so
        by executing the following command:

        .. code-block:: console

            $ camqadm exchange.delete celeryevent

* The worker now starts without configuration, and configuration can be
  specified directly on the command-line.

  Configuration options must appear after the last argument, separated
  by two dashes:

  .. code-block:: console

      $ celery worker -l info -I tasks -- broker.host=localhost broker.vhost=/app

* Configuration is now an alias to the original configuration, so changes
  to the original will reflect Celery at runtime.

* `celery.conf` has been deprecated, and modifying `celery.conf.ALWAYS_EAGER`
  will no longer have any effect.

    The default configuration is now available in the
    :mod:`celery.app.defaults` module. The available configuration options
    and their types can now be introspected.

* Remote control commands are now provided by `kombu.pidbox`, the generic
  process mailbox.

* Internal module `celery.worker.listener` has been renamed to
  `celery.worker.consumer`, and `.CarrotListener` is now `.Consumer`.

* Previously deprecated modules `celery.models` and
  `celery.management.commands` have now been removed as per the deprecation
  time-line.

* [Security: Low severity] Removed `celery.task.RemoteExecuteTask` and
    accompanying functions: `dmap`, `dmap_async`, and `execute_remote`.

    Executing arbitrary code using pickle is a potential security issue if
    someone gains unrestricted access to the message broker.

    If you really need this functionality, then you'd've to add
    this to your own project.

* [Security: Low severity] The `stats` command no longer transmits the
  broker password.

    One would've needed an authenticated broker connection to receive
    this password in the first place, but sniffing the password at the
    wire level would've been possible if using unencrypted communication.

.. _v220-news:

News
----

* The internal module `celery.task.builtins` has been removed.

* The module `celery.task.schedules` is deprecated, and
  `celery.schedules` should be used instead.

    For example if you have::

        from celery.task.schedules import crontab

    You should replace that with::

        from celery.schedules import crontab

    The module needs to be renamed because it must be possible
    to import schedules without importing the `celery.task` module.

* The following functions have been deprecated and is scheduled for
  removal in version 2.3:

    * `celery.execute.apply_async`

        Use `task.apply_async()` instead.

    * `celery.execute.apply`

        Use `task.apply()` instead.

    * `celery.execute.delay_task`

        Use `registry.tasks[name].delay()` instead.

* Importing `TaskSet` from `celery.task.base` is now deprecated.

    You should use::

        >>> from celery.task import TaskSet

    instead.

* New remote control commands:

    * `active_queues`

        Returns the queue declarations a worker is currently consuming from.

* Added the ability to retry publishing the task message in
  the event of connection loss or failure.

    This is disabled by default but can be enabled using the
    :setting:`CELERY_TASK_PUBLISH_RETRY` setting, and tweaked by
    the :setting:`CELERY_TASK_PUBLISH_RETRY_POLICY` setting.

    In addition `retry`, and `retry_policy` keyword arguments have
    been added to `Task.apply_async`.

    .. note::

        Using the `retry` argument to `apply_async` requires you to
        handle the publisher/connection manually.

* Periodic Task classes (`@periodic_task`/`PeriodicTask`) will *not* be
  deprecated as previously indicated in the source code.

    But you're encouraged to use the more flexible
    :setting:`CELERYBEAT_SCHEDULE` setting.

* Built-in daemonization support of the worker using `celery multi`
  is no longer experimental and is considered production quality.

     See :ref:`daemon-generic` if you want to use the new generic init
     scripts.

* Added support for message compression using the
  :setting:`CELERY_MESSAGE_COMPRESSION` setting, or the `compression` argument
  to `apply_async`. This can also be set using routers.

* worker: Now logs stack-trace of all threads when receiving the
   `SIGUSR1` signal (doesn't work on CPython 2.4, Windows or Jython).

    Inspired by https://gist.github.com/737056

* Can now remotely terminate/kill the worker process currently processing
  a task.

    The `revoke` remote control command now supports a `terminate` argument
    Default signal is `TERM`, but can be specified using the `signal`
    argument. Signal can be the uppercase name of any signal defined
    in the :mod:`signal` module in the Python Standard Library.

    Terminating a task also revokes it.

    Example::

        >>> from celery.task.control import revoke

        >>> revoke(task_id, terminate=True)
        >>> revoke(task_id, terminate=True, signal='KILL')
        >>> revoke(task_id, terminate=True, signal='SIGKILL')

* `TaskSetResult.join_native`: Backend-optimized version of `join()`.

    If available, this version uses the backends ability to retrieve
    multiple results at once, unlike `join()` which fetches the results
    one by one.

    So far only supported by the AMQP result backend. Support for Memcached
    and Redis may be added later.

* Improved implementations of `TaskSetResult.join` and `AsyncResult.wait`.

   An `interval` keyword argument have been added to both so the
   polling interval can be specified (default interval is 0.5 seconds).

    A `propagate` keyword argument have been added to `result.wait()`,
    errors will be returned instead of raised if this is set to False.

    .. warning::

        You should decrease the polling interval when using the database
        result backend, as frequent polling can result in high database load.


* The PID of the child worker process accepting a task is now sent as a field
  with the :event:`task-started` event.

* The following fields have been added to all events in the worker class:

    * `sw_ident`: Name of worker software (e.g., ``"py-celery"``).
    * `sw_ver`: Software version (e.g., 2.2.0).
    * `sw_sys`: Operating System (e.g., Linux, Windows, Darwin).

* For better accuracy the start time reported by the multiprocessing worker
  process is used when calculating task duration.

    Previously the time reported by the accept callback was used.

* `celerybeat`: New built-in daemonization support using the `--detach`
   option.

* `celeryev`: New built-in daemonization support using the `--detach`
   option.

* `TaskSet.apply_async`: Now supports custom publishers by using the
  `publisher` argument.

* Added :setting:`CELERY_SEND_TASK_SENT_EVENT` setting.

    If enabled an event will be sent with every task, so monitors can
    track tasks before the workers receive them.

* `celerybeat`: Now reuses the broker connection when calling
   scheduled tasks.

* The configuration module and loader to use can now be specified on
  the command-line.

    For example:

    .. code-block:: console

        $ celery worker --config=celeryconfig.py --loader=myloader.Loader

* Added signals: `beat_init` and `beat_embedded_init`

    * :signal:`celery.signals.beat_init`

        Dispatched when :program:`celerybeat` starts (either standalone or
        embedded). Sender is the :class:`celery.beat.Service` instance.

    * :signal:`celery.signals.beat_embedded_init`

        Dispatched in addition to the :signal:`beat_init` signal when
        :program:`celerybeat` is started as an embedded process. Sender
        is the :class:`celery.beat.Service` instance.

* Redis result backend: Removed deprecated settings `REDIS_TIMEOUT` and
  `REDIS_CONNECT_RETRY`.

* CentOS init-script for :program:`celery worker` now available in `extra/centos`.

* Now depends on :pypi:`pyparsing` version 1.5.0 or higher.

    There have been reported issues using Celery with :pypi:`pyparsing` 1.4.x,
    so please upgrade to the latest version.

* Lots of new unit tests written, now with a total coverage of 95%.

.. _v220-fixes:

Fixes
-----

* `celeryev` Curses Monitor: Improved resize handling and UI layout
  (Issue #274 + Issue #276)

* AMQP Backend: Exceptions occurring while sending task results are now
  propagated instead of silenced.

    the worker will then show the full traceback of these errors in the log.

* AMQP Backend: No longer deletes the result queue after successful
  poll, as this should be handled by the
  :setting:`CELERY_AMQP_TASK_RESULT_EXPIRES` setting instead.

* AMQP Backend: Now ensures queues are declared before polling results.

* Windows: worker: Show error if running with `-B` option.

    Running ``celerybeat`` embedded is known not to work on Windows, so
    users are encouraged to run ``celerybeat`` as a separate service instead.

* Windows: Utilities no longer output ANSI color codes on Windows

* ``camqadm``: Now properly handles :kbd:`Control-c` by simply exiting instead
  of showing confusing traceback.

* Windows: All tests are now passing on Windows.

* Remove bin/ directory, and `scripts` section from :file:`setup.py`.

    This means we now rely completely on setuptools entry-points.

.. _v220-experimental:

Experimental
------------

* Jython: worker now runs on Jython using the threaded pool.

    All tests pass, but there may still be bugs lurking around the corners.

* PyPy: worker now runs on PyPy.

    It runs without any pool, so to get parallel execution you must start
    multiple instances (e.g., using :program:`multi`).

    Sadly an initial benchmark seems to show a 30% performance decrease on
    ``pypy-1.4.1`` + JIT. We would like to find out why this is, so stay tuned.

* :class:`PublisherPool`: Experimental pool of task publishers and
  connections to be used with the `retry` argument to `apply_async`.

  The example code below will re-use connections and channels, and
  retry sending of the task message if the connection is lost.

  .. code-block:: python

    from celery import current_app

    # Global pool
    pool = current_app().amqp.PublisherPool(limit=10)

    def my_view(request):
        with pool.acquire() as publisher:
            add.apply_async((2, 2), publisher=publisher, retry=True)



