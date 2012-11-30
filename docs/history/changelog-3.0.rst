.. _changelog-3.0:

================
 Change history
================

This document contains change notes for bugfix releases in the 3.0.x series
(Chiastic Slide), please see :ref:`whatsnew-3.0` for an overview of what's
new in Celery 3.0.

If you're looking for versions prior to 3.0.x you should go to :ref:`history`.

.. contents::
    :local:

.. _version-3.0.13:

3.0.13
======
:release-date: 2012-11-30 XX:XX:XX X.X UTC

- Fixed a deadlock issue that could occur when the producer pool
  inherited the connection pool instance of the parent process.

- The :option:`--loader` option now works again (Issue #1066).

- :program:`celery` umbrella command: All subcommands now supports
  the :option:`--workdir` option (Issue #1063).

- Groups included in chains now give GroupResults (Issue #1057)

    Previously it would incorrectly add a regular result instead of a group
    result, but now this works:

    .. code-block:: python

        # [4 + 4, 4 + 8, 16 + 8]
        >>> res = (add.s(2, 2) | group(add.s(4), add.s(8), add.s(16)))()
        >>> res
        <GroupResult: a0acf905-c704-499e-b03a-8d445e6398f7 [
            4346501c-cb99-4ad8-8577-12256c7a22b1,
            b12ead10-a622-4d44-86e9-3193a778f345,
            26c7a420-11f3-4b33-8fac-66cd3b62abfd]>


- Chains can now chain other chains and use partial arguments (Issue #1057).

    Example:

    .. code-block:: python

        >>> c1 = (add.s(2) | add.s(4))
        >>> c2 = (add.s(8) | add.s(16))

        >>> c3 = (c1 | c2)

        # 8 + 2 + 4 + 8 + 16
        >>> assert c3(8).get() == 38

- Subtasks can now be used with unregistered tasks.

    You can specify subtasks even if you just have the name::

        >>> s = subtask(task_name, args=(), kwargs=())
        >>> s.delay()

- The :program:`celery shell` command now always adds the current
  directory to the module path.

- The worker will now properly handle the :exc:`pytz.AmbiguousTimeError`
  exception raised when an ETA/countdown is prepared while being in DST
  transition (Issue #1061).

- force_execv: Now makes sure that task symbols in the original
  task modules will always use the correct app instance (Issue #1072).

- AMQP Backend: Now republishes result messages that have been polled
  (using ``result.ready()`` and friends, ``result.get()`` will not do this
  in this version).

- Handling of ETA/countdown fixed when the :setting:`CELERY_ENABLE_UTC`
   setting is disabled (Issue #1065).

- A number of uneeded properties were included in messages,
  caused by accidentally passing ``Queue.as_dict`` as message properties.

- Fixed a typo in the broadcast routing documentation (Issue #1026).

- Rewrote confusing section about idempotence in the task user guide.

- Fixed typo in the daemonization tutorial (Issue #1055).

- Fixed several typos in the documentation.

    Contributed by Marius Gedminas.

- Batches: Now works when using the eventlet pool.

    Fix contributed by Thomas Grainger.

- Batches: Added example sending results to :mod:`celery.contrib.batches`.

    Contributed by Thomas Grainger.

- Fixed problem when using earlier versions of :mod:`pytz`.

    Fix contributed by Vlad.

- Docs updated to include the default value for the
  :setting:`CELERY_TASK_RESULT_EXPIRES` setting.

- Improvements to the django-celery tutorial.

    Contributed by Locker537.

- The ``add_consumer`` control command did not properly persist
  the addition of new queues so that they survived connection failure
  (Issue #1079).

.. _version-3.0.12:

3.0.12
======
:release-date: 2012-11-06 02:00 P.M UTC

- Now depends on kombu 2.4.8

    - [Redis] New and improved fair queue cycle algorithm (Kevin McCarthy).
    - [Redis] Now uses a Redis-based mutex when restoring messages.
    - [Redis] Number of messages that can be restored in one interval is no
              longer limited (but can be set using the
              ``unacked_restore_limit``
              :setting:`transport option <BROKER_TRANSPORT_OPTIONS>`.)
    - Heartbeat value can be specified in broker URLs (Mher Movsisyan).
    - Fixed problem with msgpack on Python 3 (Jasper Bryant-Greene).

- Now depends on billiard 2.7.3.18

- Celery can now be used with static analysis tools like PyDev/PyCharm/pylint
  etc.

- Development documentation has moved to Read The Docs.

    The new URL is: http://docs.celeryproject.org/en/master

- New :setting:`CELERY_QUEUE_HA_POLICY` setting used to set the default
  HA policy for queues when using RabbitMQ.

- New method ``Task.subtask_from_request`` returns a subtask using the current
  request.

- Results get_many method did not respect timeout argument.

    Fix contributed by Remigiusz Modrzejewski

- generic_init.d scripts now support setting :envvar:`CELERY_CREATE_DIRS` to
  always create log and pid directories (Issue #1045).

    This can be set in your :file:`/etc/default/celeryd`.

- Fixed strange kombu import problem on Python 3.2 (Issue #1034).

- Worker: ETA scheduler now uses millisecond precision (Issue #1040).

- The ``--config`` argument to programs is now supported by all loaders.

- The :setting:`CASSANDRA_OPTIONS` setting has now been documented.

    Contributed by Jared Biel.

- Task methods (:mod:`celery.contrib.methods`) cannot be used with the old
  task base class, the task decorator in that module now inherits from the new.

- An optimization was too eager and caused some logging messages to never emit.

- :mod:`celery.contrib.batches` now works again.

- Fixed missing whitespace in ``bdist_rpm`` requirements (Issue #1046).

- Event state's ``tasks_by_name`` applied limit before filtering by name.

    Fix contributed by Alexander A. Sosnovskiy.

.. _version-3.0.11:

3.0.11
======
:release-date: 2012-09-26 04:00 P.M UTC

- [security:low] generic-init.d scripts changed permissions of /var/log & /var/run

    In the daemonization tutorial the recommended directories were as follows:

    .. code-block:: bash

        CELERYD_LOG_FILE="/var/log/celery/%n.log"
        CELERYD_PID_FILE="/var/run/celery/%n.pid"

    But in the scripts themselves the default files were ``/var/log/celery%n.log``
    and ``/var/run/celery%n.pid``, so if the user did not change the location
    by configuration, the directories ``/var/log`` and ``/var/run`` would be
    created - and worse have their permissions and owners changed.

    This change means that:

        - Default pid file is ``/var/run/celery/%n.pid``
        - Default log file is ``/var/log/celery/%n.log``

        - The directories are only created and have their permissions
          changed if *no custom locations are set*.

    Users can force paths to be created by calling the ``create-paths``
    subcommand:

    .. code-block:: bash

        $ sudo /etc/init.d/celeryd create-paths

    .. admonition:: Upgrading Celery will not update init scripts

        To update the init scripts you have to re-download
        the files from source control and update them manually.
        You can find the init scripts for version 3.0.x at:

            http://github.com/celery/celery/tree/3.0/extra/generic-init.d

- Now depends on billiard 2.7.3.17

- Fixes request stack protection when app is initialized more than
  once (Issue #1003).

- ETA tasks now properly works when system timezone is not the same
  as the configured timezone (Issue #1004).

- Terminating a task now works if the task has been sent to the
  pool but not yet acknowledged by a pool process (Issue #1007).

    Fix contributed by Alexey Zatelepin

- Terminating a task now properly updates the state of the task to revoked,
  and sends a ``task-revoked`` event.

- Generic worker init script now waits for workers to shutdown by default.

- Multi: No longer parses --app option (Issue #1008).

- Multi: stop_verify command renamed to stopwait.

- Daemonization: Now delays trying to create pidfile/logfile until after
  the working directory has been changed into.

- :program:`celery worker` and :program:`celery beat` commands now respects
  the :option:`--no-color` option (Issue #999).

- Fixed typos in eventlet examples (Issue #1000)

    Fix contributed by Bryan Bishop.
    Congratulations on opening bug #1000!

- Tasks that raise :exc:`~celery.exceptions.Ignore` are now acknowledged.

- Beat: Now shows the name of the entry in ``sending due task`` logs.

.. _version-3.0.10:

3.0.10
======
:release-date: 2012-09-20 05:30 P.M BST

- Now depends on kombu 2.4.7

- Now depends on billiard 2.7.3.14

    - Fixes crash at startup when using Django and pre-1.4 projects
      (setup_environ).

    - Hard time limits now sends the KILL signal shortly after TERM,
      to terminate processes that have signal handlers blocked by C extensions.

    - Billiard now installs even if the C extension cannot be built.

        It's still recommended to build the C extension if you are using
        a transport other than rabbitmq/redis (or use forced execv for some
        other reason).

    - Pool now sets a ``current_process().index`` attribute that can be used to create
      as many log files as there are processes in the pool.

- Canvas: chord/group/chain no longer modifies the state when called

    Previously calling a chord/group/chain would modify the ids of subtasks
    so that:

    .. code-block:: python

        >>> c = chord([add.s(2, 2), add.s(4, 4)], xsum.s())
        >>> c()
        >>> c() <-- call again

    at the second time the ids for the tasks would be the same as in the
    previous invocation.  This is now fixed, so that calling a subtask
    won't mutate any options.

- Canvas: Chaining a chord to another task now works (Issue #965).

- Worker: Fixed a bug where the request stack could be corrupted if
  relative imports are used.

    Problem usually manifested itself as an exception while trying to
    send a failed task result (``NoneType does not have id attribute``).

    Fix contributed by Sam Cooke.

- Tasks can now raise :exc:`~celery.exceptions.Ignore` to skip updating states
  or events after return.

    Example:

    .. code-block:: python

        from celery.exceptions import Ignore

        @task
        def custom_revokes():
            if redis.sismember('tasks.revoked', custom_revokes.request.id):
                raise Ignore()

- The worker now makes sure the request/task stacks are not modified
  by the initial ``Task.__call__``.

    This would previously be a problem if a custom task class defined
    ``__call__`` and also called ``super()``.

- Because of problems the fast local optimization has been disabled,
  and can only be enabled by setting the :envvar:`USE_FAST_LOCALS` attribute.

- Worker: Now sets a default socket timeout of 5 seconds at shutdown
  so that broken socket reads do not hinder proper shutdown (Issue #975).

- More fixes related to late eventlet/gevent patching.

- Documentation for settings out of sync with reality:

    - :setting:`CELERY_TASK_PUBLISH_RETRY`

        Documented as disabled by default, but it was enabled by default
        since 2.5 as stated by the 2.5 changelog.

    - :setting:`CELERY_TASK_PUBLISH_RETRY_POLICY`

        The default max_retries had been set to 100, but documented as being
        3, and the interval_max was set to 1 but documented as 0.2.
        The default setting are now set to 3 and 0.2 as it was originally
        documented.

    Fix contributed by Matt Long.

- Worker: Log messages when connection established and lost have been improved.

- The repr of a crontab schedule value of '0' should be '*'  (Issue #972).

- Revoked tasks are now removed from reserved/active state in the worker
  (Issue #969)

    Fix contributed by Alexey Zatelepin.

- gevent: Now supports hard time limits using ``gevent.Timeout``.

- Documentation: Links to init scripts now point to the 3.0 branch instead
  of the development branch (master).

- Documentation: Fixed typo in signals user guide (Issue #986).

    ``instance.app.queues`` -> ``instance.app.amqp.queues``.

- Eventlet/gevent: The worker did not properly set the custom app
  for new greenlets.

- Eventlet/gevent: Fixed a bug where the worker could not recover
  from connection loss (Issue #959).

    Also, because of a suspected bug in gevent the
    :setting:`BROKER_CONNECTION_TIMEOUT` setting has been disabled
    when using gevent

3.0.9
=====
:release-date: 2012-08-31 06:00 P.M BST

- Important note for users of Django and the database scheduler!

    Recently a timezone issue has been fixed for periodic tasks,
    but erroneous timezones could have already been stored in the
    database, so for the fix to work you need to reset
    the ``last_run_at`` fields.

    You can do this by executing the following command:

    .. code-block:: bash

        $ python manage.py shell
        >>> from djcelery.models import PeriodicTask
        >>> PeriodicTask.objects.update(last_run_at=None)

    You also have to do this if you change the timezone or
    :setting:`CELERY_ENABLE_UTC` setting.

- Note about the :setting:`CELERY_ENABLE_UTC` setting.

    If you previously disabled this just to force periodic tasks to work with
    your timezone, then you are now *encouraged to re-enable it*.

- Now depends on Kombu 2.4.5 which fixes PyPy + Jython installation.

- Fixed bug with timezones when :setting:`CELERY_ENABLE_UTC` is disabled
  (Issue #952).

- Fixed a typo in the celerybeat upgrade mechanism (Issue #951).

- Make sure the `exc_info` argument to logging is resolved (Issue #899).

- Fixed problem with Python 3.2 and thread join timeout overflow (Issue #796).

- A test case was occasionally broken for Python 2.5.

- Unit test suite now passes for PyPy 1.9.

- App instances now supports the with statement.

    This calls the new :meth:`~celery.Celery.close` method at exit, which
    cleans up after the app like closing pool connections.

    Note that this is only necessary when dynamically creating apps,
    e.g. for "temporary" apps.

- Support for piping a subtask to a chain.

    For example:

    .. code-block:: python

        pipe = sometask.s() | othertask.s()
        new_pipe = mytask.s() | pipe

    Contributed by Steve Morin.

- Fixed problem with group results on non-pickle serializers.

    Fix contributed by Steeve Morin.

.. _version-3.0.8:

3.0.8
=====
:release-date: 2012-08-29 05:00 P.M BST

- Now depends on Kombu 2.4.4

- Fixed problem with amqplib and receiving larger message payloads
  (Issue #922).

    The problem would manifest itself as either the worker hanging,
    or occasionally a ``Framing error`` exception appearing.

    Users of the new ``pyamqp://`` transport must upgrade to
    :mod:`amqp` 0.9.3.

- Beat: Fixed another timezone bug with interval and crontab schedules
  (Issue #943).

- Beat: The schedule file is now automatically cleared if the timezone
  is changed.

    The schedule is also cleared when you upgrade to 3.0.8 from an earlier
    version, this to register the initial timezone info.

- Events: The :event:`worker-heartbeat` event now include processed and active
  count fields.

    Contributed by Mher Movsisyan.

- Fixed error with error email and new task classes (Issue #931).

- ``BaseTask.__call__`` is no longer optimized away if it has been monkey
  patched.

- Fixed shutdown issue when using gevent (Issue #911 & Issue #936).

    Fix contributed by Thomas Meson.

.. _version-3.0.7:

3.0.7
=====
:release-date: 2012-08-24 05:00 P.M BST

- Fixes several problems with periodic tasks and timezones (Issue #937).

- Now depends on kombu 2.4.2

    - Redis: Fixes a race condition crash

    - Fixes an infinite loop that could happen when retrying establishing
      the broker connection.

- Daemons now redirect standard file descriptors to :file:`/dev/null`

    Though by default the standard outs are also redirected
    to the logger instead, but you can disable this by changing
    the :setting:`CELERY_REDIRECT_STDOUTS` setting.

- Fixes possible problems when eventlet/gevent is patched too late.

- ``LoggingProxy`` no longer defines ``fileno()`` (Issue #928).

- Results are now ignored for the chord unlock task.

    Fix contributed by Steeve Morin.

- Cassandra backend now works if result expiry is disabled.

    Fix contributed by Steeve Morin.

- The traceback object is now passed to signal handlers instead
  of the string representation.

    Fix contributed by Adam DePue.

- Celery command: Extensions are now sorted by name.

- A regression caused the :event:`task-failed` event to be sent
  with the exception object instead of its string representation.

- The worker daemon would try to create the pid file before daemonizing
  to catch errors, but this file was not immediately released (Issue #923).

- Fixes Jython compatibility.

- ``billiard.forking_enable`` was called by all pools not just the
  processes pool, which would result in a useless warning if the billiard
  C extensions were not installed.

.. _version-3.0.6:

3.0.6
=====
:release-date: 2012-08-17 11:00 P.M BST

- Now depends on kombu 2.4.0

- Now depends on billiard 2.7.3.12

- Redis: Celery now tries to restore messages whenever there are no messages
  in the queue.

- Crontab schedules now properly respects :setting:`CELERY_TIMEZONE` setting.

    It's important to note that crontab schedules uses UTC time by default
    unless this setting is set.

    Issue #904 and django-celery #150.

- ``billiard.enable_forking`` is now only set by the processes pool.

- The transport is now properly shown by :program:`celery report`
  (Issue #913).

- The `--app` argument now works if the last part is a module name
  (Issue #921).

- Fixed problem with unpickleable exceptions (billiard #12).

- Adds ``task_name`` attribute to ``EagerResult`` which is always
  :const:`None` (Issue #907).

- Old Task class in :mod:`celery.task` no longer accepts magic kwargs by
  default (Issue #918).

    A regression long ago disabled magic kwargs for these, and since
    no one has complained about it we don't have any incentive to fix it now.

- The ``inspect reserved`` control command did not work properly.

- Should now play better with static analyzation tools by explicitly
  specifying dynamically created attributes in the :mod:`celery` and
  :mod:`celery.task` modules.

- Terminating a task now results in
  :exc:`~celery.exceptions.RevokedTaskError` instead of a ``WorkerLostError``.

- ``AsyncResult.revoke`` now accepts ``terminate`` and ``signal`` arguments.

- The :event:`task-revoked` event now includes new fields: ``terminated``,
  ``signum``, and ``expired``.

- The argument to :class:`~celery.exceptions.TaskRevokedError` is now one
  of the reasons ``revoked``, ``expired`` or ``terminated``.

- Old Task class does no longer use classmethods for push_request and
  pop_request  (Issue #912).

- ``GroupResult`` now supports the ``children`` attribute (Issue #916).

- ``AsyncResult.collect`` now respects the ``intermediate`` argument
  (Issue #917).

- Fixes example task in documentation (Issue #902).

- Eventlet fixed so that the environment is patched as soon as possible.

- eventlet: Now warns if celery related modules that depends on threads
  are imported before eventlet is patched.

- Improved event and camera examples in the monitoring guide.

- Disables celery command setuptools entrypoints if the command can't be
  loaded.

- Fixed broken ``dump_request`` example in the tasks guide.

.. _version-3.0.5:

3.0.5
=====
:release-date: 2012-08-01 04:00 P.M BST

- Now depends on kombu 2.3.1 + billiard 2.7.3.11

- Fixed a bug with the -B option (``cannot pickle thread.lock objects``)
  (Issue #894 + Issue #892, + django-celery #154).

- The :control:`restart_pool` control command now requires the
  :setting:`CELERYD_POOL_RESTARTS` setting to be enabled

    This change was necessary as the multiprocessing event that the restart
    command depends on is responsible for creating many semaphores/file
    descriptors, resulting in problems in some environments.

- ``chain.apply`` now passes args to the first task (Issue #889).

- Documented previously secret options to the Django-Celery monitor
  in the monitoring userguide (Issue #396).

- Old changelog are now organized in separate documents for each series,
  see :ref:`history`.

.. _version-3.0.4:

3.0.4
=====
:release-date: 2012-07-26 07:00 P.M BST

- Now depends on Kombu 2.3

- New experimental standalone Celery monitor: Flower

    See :ref:`monitoring-flower` to read more about it!

    Contributed by Mher Movsisyan.

- Now supports AMQP heartbeats if using the new ``pyamqp://`` transport.

    - The py-amqp transport requires the :mod:`amqp` library to be installed::

        $ pip install amqp

    - Then you need to set the transport URL prefix to ``pyamqp://``.

    - The default heartbeat value is 10 seconds, but this can be changed using
      the :setting:`BROKER_HEARTBEAT` setting::

        BROKER_HEARTBEAT = 5.0

    - If the broker heartbeat is set to 10 seconds, the heartbeats will be
      monitored every 5 seconds (double the hertbeat rate).

    See the `Kombu 2.3 changelog`_ for more information.

.. _`Kombu 2.3 changelog`:
    http://kombu.readthedocs.org/en/latest/changelog.html#version-2-3-0

- Now supports RabbitMQ Consumer Cancel Notifications, using the ``pyamqp://``
  transport.

    This is essential when running RabbitMQ in a cluster.

    See the `Kombu 2.3 changelog`_ for more information.

- Delivery info is no longer passed directly through.

    It was discovered that the SQS transport adds objects that can't
    be pickled to the delivery info mapping, so we had to go back
    to using the whitelist again.

    Fixing this bug also means that the SQS transport is now working again.

- The semaphore was not properly released when a task was revoked (Issue #877).

    This could lead to tasks being swallowed and not released until a worker
    restart.

    Thanks to Hynek Schlawack for debugging the issue.

- Retrying a task now also forwards any linked tasks.

    This means that if a task is part of a chain (or linked in some other
    way) and that even if the task is retried, then the next task in the chain
    will be executed when the retry succeeds.

- Chords: Now supports setting the interval and other keyword arguments
  to the chord unlock task.

    - The interval can now be set as part of the chord subtasks kwargs::

        chord(header)(body, interval=10.0)

    - In addition the chord unlock task now honors the Task.default_retry_delay
      option, used when none is specified, which also means that the default
      interval can also be changed using annotations:

        .. code-block:: python

            CELERY_ANNOTATIONS = {
                'celery.chord_unlock': {
                    'default_retry_delay': 10.0,
                }
            }

- New :meth:`@Celery.add_defaults` method can add new default configuration
  dicts to the applications configuration.

    For example::

        config = {'FOO': 10}

        celery.add_defaults(config)

    is the same as ``celery.conf.update(config)`` except that data will not be
    copied, and that it will not be pickled when the worker spawns child
    processes.

    In addition the method accepts a callable::

        def initialize_config():
            # insert heavy stuff that can't be done at import time here.

        celery.add_defaults(initialize_config)

    which means the same as the above except that it will not happen
    until the celery configuration is actually used.

    As an example, Celery can lazily use the configuration of a Flask app::

        flask_app = Flask()
        celery = Celery()
        celery.add_defaults(lambda: flask_app.config)

- Revoked tasks were not marked as revoked in the result backend (Issue #871).

    Fix contributed by Hynek Schlawack.

- Eventloop now properly handles the case when the epoll poller object
  has been closed (Issue #882).

- Fixed syntax error in ``funtests/test_leak.py``

    Fix contributed by Catalin Iacob.

- group/chunks: Now accepts empty task list (Issue #873).

- New method names:

    - ``Celery.default_connection()`` ➠  :meth:`~@Celery.connection_or_acquire`.
    - ``Celery.default_producer()``   ➠  :meth:`~@Celery.producer_or_acquire`.

    The old names still work for backward compatibility.

.. _version-3.0.3:

3.0.3
=====
:release-date: 2012-07-20 09:17 P.M BST
:by: Ask Solem

- amqplib passes the channel object as part of the delivery_info
  and it's not pickleable, so we now remove it.

.. _version-3.0.2:

3.0.2
=====
:release-date: 2012-07-20 04:00 P.M BST
:by: Ask Solem

- A bug caused the following task options to not take defaults from the
   configuration (Issue #867 + Issue #858)

    The following settings were affected:

    - :setting:`CELERY_IGNORE_RESULT`
    - :setting:`CELERYD_SEND_TASK_ERROR_EMAILS`
    - :setting:`CELERY_TRACK_STARTED`
    - :setting:`CElERY_STORE_ERRORS_EVEN_IF_IGNORED`

    Fix contributed by John Watson.

- Task Request: ``delivery_info`` is now passed through as-is (Issue #807).

- The eta argument now supports datetime's with a timezone set (Issue #855).

- The worker's banner displayed the autoscale settings in the wrong order
  (Issue #859).

- Extension commands are now loaded after concurrency is set up
  so that they don't interfere with e.g. eventlet patching.

- Fixed bug in the threaded pool (Issue #863)

- The task failure handler mixed up the fields in :func:`sys.exc_info`.

    Fix contributed by Rinat Shigapov.

- Fixed typos and wording in the docs.

    Fix contributed by Paul McMillan

- New setting: :setting:`CELERY_WORKER_DIRECT`

    If enabled each worker will consume from their own dedicated queue
    which can be used to route tasks to specific workers.

- Fixed several edge case bugs in the add consumer remote control command.

- :mod:`~celery.contrib.migrate`: Can now filter and move tasks to specific
  workers if :setting:`CELERY_WORKER_DIRECT` is enabled.

    Among other improvements, the following functions have been added:

        * ``move_direct(filterfun, **opts)``
        * ``move_direct_by_id(task_id, worker_hostname, **opts)``
        * ``move_direct_by_idmap({task_id: worker_hostname, ...}, **opts)``
        * ``move_direct_by_taskmap({task_name: worker_hostname, ...}, **opts)``

- :meth:`~celery.Celery.default_connection` now accepts a pool argument that
  if set to false causes a new connection to be created instead of acquiring
  one from the pool.

- New signal: :signal:`celeryd_after_setup`.

- Default loader now keeps lowercase attributes from the configuration module.

.. _version-3.0.1:

3.0.1
=====
:release-date: 2012-07-10 06:00 P.M BST
:by: Ask Solem

- Now depends on kombu 2.2.5

- inspect now supports limit argument::

    myapp.control.inspect(limit=1).ping()

- Beat: now works with timezone aware datetime's.

- Task classes inheriting ``from celery import Task``
  mistakingly enabled ``accept_magic_kwargs``.

- Fixed bug in ``inspect scheduled`` (Issue #829).

- Beat: Now resets the schedule to upgrade to UTC.

- The :program:`celery worker` command now works with eventlet/gevent.

    Previously it would not patch the environment early enough.

- The :program:`celery` command now supports extension commands
  using setuptools entry-points.

    Libraries can add additional commands to the :program:`celery`
    command by adding an entry-point like::

        setup(
            entry_points=[
                'celery.commands': [
                    'foo = my.module:Command',
            ],
        ],
        ...)

    The command must then support the interface of
    :class:`celery.bin.base.Command`.

- contrib.migrate: New utilities to move tasks from one queue to another.

    - :func:`~celery.contrib.migrate.move_tasks`
    - :func:`~celery.contrib.migrate.move_task_by_id`

- The :event:`task-sent` event now contains ``exchange`` and ``routing_key``
  fields.

- Fixes bug with installing on Python 3.

    Fix contributed by Jed Smith.

.. _version-3.0.0:

3.0.0 (Chiastic Slide)
======================
:release-date: 2012-07-07 01:30 P.M BST
:by: Ask Solem

See :ref:`whatsnew-3.0`.
