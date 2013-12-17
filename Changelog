.. _changelog:

================
 Change history
================

This document contains change notes for bugfix releases in the 3.1.x series
(Cipater), please see :ref:`whatsnew-3.1` for an overview of what's
new in Celery 3.1.

.. _version-3.1.7:

3.1.7
=====
:release-date: 2013-12-17 06:00 P.M UTC
:release-by: Ask Solem

.. _v317-important:

Important Notes
---------------

Init script security improvements
---------------------------------

Where the generic init scripts (for ``celeryd``, and ``celerybeat``) before
delegated the responsibility of dropping privileges to the target application,
it will now use ``su`` instead, so that the Python program is not trusted
with superuser privileges.

This is not in reaction to any known exploit, but it will
limit the possibility of a privilege escalation bug being abused in the
future.

You have to upgrade the init scripts manually from this directory:
https://github.com/celery/celery/tree/3.1/extra/generic-init.d

AMQP result backend
~~~~~~~~~~~~~~~~~~~

The 3.1 release accidentally left the amqp backend configured to be
non-persistent by default.

Upgrading from 3.0 would give a "not equivalent" error when attempting to
set or retrieve results for a task.  That is unless you manually set the
persistence setting::

    CELERY_RESULT_PERSISTENT = True

This version restores the previous value so if you already forced
the upgrade by removing the existing exchange you must either
keep the configuration by setting ``CELERY_RESULT_PERSISTENT = False``
or delete the ``celeryresults`` exchange again.

Synchronous subtasks
~~~~~~~~~~~~~~~~~~~~

Tasks waiting for the result of a subtask will now emit
a :exc:`RuntimeWarning` warning when using the prefork pool,
and in 3.2 this will result in an exception being raised.

It's not legal for tasks to block by waiting for subtasks
as this is likely to lead to resource starvation and eventually
deadlock when using the prefork pool (see also :ref:`task-synchronous-subtasks`).

If you really know what you are doing you can avoid the warning (and
the future exception being raised) by moving the operation in a whitelist
block:

.. code-block:: python

    from celery.result import allow_join_result

    @app.task
    def misbehaving():
        result = other_task.delay()
        with allow_join_result():
            result.get()

Note also that if you wait for the result of a subtask in any form
when using the prefork pool you must also disable the pool prefetching
behavior with the worker :ref:`-Ofair option <prefork-pool-prefetch>`.

.. _v317-fixes:

Fixes
-----

- Now depends on :ref:`Kombu 3.0.8 <kombu:version-3.0.8>`.

- Now depends on :mod:`billiard` 3.3.0.13

- Events: Fixed compatibility with non-standard json libraries
  that sends float as :class:`decimal.Decimal` (Issue #1731)

- Events: State worker objects now always defines attributes:
  ``active``, ``processed``, ``loadavg``, ``sw_ident``, ``sw_ver``
  and ``sw_sys``.

- Worker: Now keeps count of the total number of tasks processed,
  not just by type (``all_active_count``).

- Init scripts:  Fixed problem with reading configuration file
  when the init script is symlinked to a runlevel (e.g. ``S02celeryd``).
  (Issue #1740).

    This also removed a rarely used feature where you can symlink the script
    to provide alternative configurations.  You instead copy the script
    and give it a new name, but perhaps a better solution is to provide
    arguments to ``CELERYD_OPTS`` to separate them:

    .. code-block:: bash

        CELERYD_NODES="X1 X2 Y1 Y2"
        CELERYD_OPTS="-A:X1 x -A:X2 x -A:Y1 y -A:Y2 y"

- Fallback chord unlock task is now always called after the chord header
  (Issue #1700).

    This means that the unlock task will not be started if there's
    an error sending the header.

- Celery command: Fixed problem with arguments for some control commands.

    Fix contributed by Konstantin Podshumok.

- Fixed bug in ``utcoffset`` where the offset when in DST would be
  completely wrong (Issue #1743).

- Worker: Errors occurring while attempting to serialize the result of a
  task will now cause the task to be marked with failure and a
  :class:`kombu.exceptions.EncodingError` error.

    Fix contributed by Ionel Cristian Mărieș.

- Worker with ``-B`` argument did not properly shut down the beat instance.

- Worker: The ``%n`` and ``%h`` formats are now also supported by the
  :option:`--logfile`, :option:`--pidfile` and :option:`--statedb` arguments.

    Example:

    .. code-block:: bash

        $ celery -A proj worker -n foo@%h --logfile=%n.log --statedb=%n.db

- Redis/Cache result backends: Will now timeout if keys evicted while trying
  to join a chord.

- The fallbock unlock chord task now raises :exc:`Retry` so that the
  retry even is properly logged by the worker.

- Multi: Will no longer apply Eventlet/gevent monkey patches (Issue #1717).

- Redis result backend: Now supports UNIX sockets.

    Like the Redis broker transport the result backend now also supports
    using ``redis+socket:///tmp/redis.sock`` URLs.

    Contributed by Alcides Viamontes Esquivel.

- Events: Events sent by clients was mistaken for worker related events
  (Issue #1714).

    For ``events.State`` the tasks now have a ``Task.client`` attribute
    that is set when a ``task-sent`` event is being received.

    Also, a clients logical clock is not in sync with the cluster so
    they live in a "time bubble".  So for this reason monitors will no
    longer attempt to merge with the clock of an event sent by a client,
    instead it will fake the value by using the current clock with
    a skew of -1.

- Prefork pool: The method used to find terminated processes was flawed
  in that it did not also take into account missing popen objects.

- Canvas: ``group`` and ``chord`` now works with anon signatures as long
  as the group/chord object is associated with an app instance (Issue #1744).

    You can pass the app by using ``group(..., app=app)``.

.. _version-3.1.6:

3.1.6
=====
:release-date: 2013-12-02 6:00 P.M UTC
:release-by: Ask Solem

- Now depends on :mod:`billiard` 3.3.0.10.

- Now depends on :ref:`Kombu 3.0.7 <kombu:version-3.0.7>`.

- Fixed problem where Mingle caused the worker to hang at startup
  (Issue #1686).

- Beat: Would attempt to drop privileges twice (Issue #1708).

- Windows: Fixed error with ``geteuid`` not being available (Issue #1676).

- Tasks can now provide a list of expected error classes (Issue #1682).

    The list should only include errors that the task is expected to raise
    during normal operation::

        @task(throws=(KeyError, HttpNotFound))

    What happens when an exceptions is raised depends on the type of error:

    - Expected errors (included in ``Task.throws``)

        Will be logged using severity ``INFO``, and traceback is excluded.

    - Unexpected errors

        Will be logged using severity ``ERROR``, with traceback included.

- Cache result backend now compatible with Python 3 (Issue #1697).

- CentOS init script: Now compatible with sys-v style init symlinks.

    Fix contributed by Jonathan Jordan.

- Events: Fixed problem when task name is not defined (Issue #1710).

    Fix contributed by Mher Movsisyan.

- Task: Fixed unbound local errors (Issue #1684).

    Fix contributed by Markus Ullmann.

- Canvas: Now unrolls groups with only one task (optimization) (Issue #1656).

- Task: Fixed problem with eta and timezones.

    Fix contributed by Alexander Koval.

- Django: Worker now performs model validation (Issue #1681).

- Task decorator now emits less confusing errors when used with
  incorrect arguments (Issue #1692).

- Task: New method ``Task.send_event`` can be used to send custom events
  to Flower and other monitors.

- Fixed a compatibility issue with non-abstract task classes

- Events from clients now uses new node name format (``gen<pid>@<hostname>``).

- Fixed rare bug with Callable not being defined at interpreter shutdown
  (Issue #1678).

    Fix contributed by Nick Johnson.

- Fixed Python 2.6 compatibility (Issue #1679).

.. _version-3.1.5:

3.1.5
=====
:release-date: 2013-11-21 6:20 P.M UTC
:release-by: Ask Solem

- Now depends on :ref:`Kombu 3.0.6 <kombu:version-3.0.6>`.

- Now depends on :mod:`billiard` 3.3.0.8

- App: ``config_from_object`` is now lazy (Issue #1665).

- App: ``autodiscover_tasks`` is now lazy.

    Django users should now wrap access to the settings object
    in a lambda::

        app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

    this ensures that the settings object is not prepared
    prematurely.

- Fixed regression for ``--app`` argument experienced by
  some users (Issue #1653).

- Worker: Now respects the ``--uid`` and ``--gid`` arguments
  even if ``--detach`` is not enabled.

- Beat: Now respects the ``--uid`` and ``--gid`` arguments
  even if ``--detach`` is not enabled.

- Python 3: Fixed unorderable error occuring with the worker ``-B``
  argument enabled.

- ``celery.VERSION`` is now a named tuple.

- ``maybe_signature(list)`` is now applied recursively (Issue #1645).

- ``celery shell`` command: Fixed ``IPython.frontend`` deprecation warning.

- The default app no longer includes the builtin fixups.

    This fixes a bug where ``celery multi`` would attempt
    to load the Django settings module before entering
    the target working directory.

- The Django daemonization tutorial was changed.

    Users no longer have to explicitly export ``DJANGO_SETTINGS_MODULE``
    in :file:`/etc/default/celeryd` when the new project layout is used.

- Redis result backend: expiry value can now be 0 (Issue #1661).

- Censoring settings now accounts for non-string keys (Issue #1663).

- App: New ``autofinalize`` option.

    Apps are automatically finalized when the task registry is accessed.
    You can now disable this behavior so that an exception is raised
    instead.

    Example:

    .. code-block:: python

        app = Celery(autofinalize=False)

        # raises RuntimeError
        tasks = app.tasks

        @app.task
        def add(x, y):
            return x + y

        # raises RuntimeError
        add.delay(2, 2)

        app.finalize()
        # no longer raises:
        tasks = app.tasks
        add.delay(2, 2)

- The worker did not send monitoring events during shutdown.

- Worker: Mingle and gossip is now automatically disabled when
  used with an unsupported transport (Issue #1664).

- ``celery`` command:  Preload options now supports
  the rare ``--opt value`` format (Issue #1668).

- ``celery`` command: Accidentally removed options
  appearing before the subcommand, these are now moved to the end
  instead.

- Worker now properly responds to ``inspect stats`` commands
  even if received before startup is complete (Issue #1659).

- :signal:`task_postrun` is now sent within a finally block, to make
  sure the signal is always sent.

- Beat: Fixed syntax error in string formatting.

    Contributed by nadad.

- Fixed typos in the documentation.

    Fixes contributed by Loic Bistuer, sunfinite.

- Nested chains now works properly when constructed using the
  ``chain`` type instead of the ``|`` operator (Issue #1656).

.. _version-3.1.4:

3.1.4
=====
:release-date: 2013-11-15 11:40 P.M UTC
:release-by: Ask Solem

- Now depends on :ref:`Kombu 3.0.5 <kombu:version-3.0.5>`.

- Now depends on :mod:`billiard` 3.3.0.7

- Worker accidentally set a default socket timeout of 5 seconds.

- Django: Fixup now sets the default app so that threads will use
  the same app instance (e.g. for manage.py runserver).

- Worker: Fixed Unicode error crash at startup experienced by some users.

- Calling ``.apply_async`` on an empty chain now works again (Issue #1650).

- The ``celery multi show`` command now generates the same arguments
  as the start command does.

- The ``--app`` argument could end up using a module object instead
  of an app instance (with a resulting crash).

- Fixed a syntax error problem in the celerybeat init script.

    Fix contributed by Vsevolod.

- Tests now passing on PyPy 2.1 and 2.2.

.. _version-3.1.3:

3.1.3
=====
:release-date: 2013-11-13 12:55 A.M UTC
:release-by: Ask Solem

- Fixed compatibility problem with Python 2.7.0 - 2.7.5 (Issue #1637)

    ``unpack_from`` started supporting ``memoryview`` arguments
    in Python 2.7.6.

- Worker: :option:`-B` argument accidentally closed files used
  for logging.

- Task decorated tasks now keep their docstring (Issue #1636)

.. _version-3.1.2:

3.1.2
=====
:release-date: 2013-11-12 08:00 P.M UTC
:release-by: Ask Solem

- Now depends on :mod:`billiard` 3.3.0.6

- No longer needs the billiard C extension to be installed.

- The worker silently ignored task errors.

- Django: Fixed ``ImproperlyConfigured`` error raised
  when no database backend specified.

    Fix contributed by j0hnsmith

- Prefork pool: Now using ``_multiprocessing.read`` with ``memoryview``
  if available.

- ``close_open_fds`` now uses ``os.closerange`` if available.

- ``get_fdmax`` now takes value from ``sysconfig`` if possible.

.. _version-3.1.1:

3.1.1
=====
:release-date: 2013-11-11 06:30 P.M UTC
:release-by: Ask Solem

- Now depends on :mod:`billiard` 3.3.0.4.

- Python 3: Fixed compatibility issues.

- Windows:  Accidentally showed warning that the billiard C extension
  was not installed (Issue #1630).

- Django: Tutorial updated with a solution that sets a default
  :envvar:`DJANGO_SETTINGS_MODULE` so that it doesn't have to be typed
  in with the :program:`celery` command.

    Also fixed typos in the tutorial, and added the settings
    required to use the Django database backend.

    Thanks to Chris Ward, orarbel.

- Django: Fixed a problem when using the Django settings in Django 1.6.

- Django: Fixup should not be applied if the django loader is active.

- Worker:  Fixed attribute error for ``human_write_stats`` when using the
  compatibility prefork pool implementation.

- Worker: Fixed compatibility with billiard without C extension.

- Inspect.conf: Now supports a ``with_defaults`` argument.

- Group.restore: The backend argument was not respected.

.. _version-3.1.0:

3.1.0
=======
:release-date: 2013-11-09 11:00 P.M UTC
:release-by: Ask Solem

See :ref:`whatsnew-3.1`.
