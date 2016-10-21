.. _changelog-3.1:

================
 Change history
================

This document contains change notes for bugfix releases in the 3.1.x series
(Cipater), please see :ref:`whatsnew-3.1` for an overview of what's
new in Celery 3.1.

.. _version-3.1.21:

3.1.21
======
:release-date: 2016-03-04 11:16 a.m. PST
:release-by: Ask Solem

- **Requirements**

    - Now depends on :ref:`Kombu 3.0.34 <kombu:version-3.0.34>`.

    - Now depends on :mod:`billiard` 3.3.0.23.

- **Prefork pool**: Fixes 100% CPU loop on Linux :manpage:`epoll`
  (Issue #1845).

    Also potential fix for: Issue #2142, Issue #2606

- **Prefork pool**: Fixes memory leak related to processes exiting
  (Issue #2927).

- **Worker**: Fixes crash at start-up when trying to censor passwords
  in MongoDB and Cache result backend URLs (Issue #3079, Issue #3045,
  Issue #3049, Issue #3068, Issue #3073).

    Fix contributed by Maxime Verger.

- **Task**: An exception is now raised if countdown/expires is less
  than -2147483648 (Issue #3078).

- **Programs**: :program:`celery shell --ipython` now compatible with newer
  :pypi:`IPython` versions.

- **Programs**: The DuplicateNodeName warning emitted by inspect/control
  now includes a list of the node names returned.

    Contributed by Sebastian Kalinowski.

- **Utils**: The ``.discard(item)`` method of
  :class:`~celery.utils.collections.LimitedSet` didn't actually remove the item
  (Issue #3087).

    Fix contributed by Dave Smith.

- **Worker**: Node name formatting now emits less confusing error message
  for unmatched format keys (Issue #3016).

- **Results**: RPC/AMQP backends: Fixed deserialization of JSON exceptions
  (Issue #2518).

    Fix contributed by Allard Hoeve.

- **Prefork pool**: The `process inqueue damaged` error message now includes
  the original exception raised.

- **Documentation**: Includes improvements by:

    - Jeff Widman.

.. _version-3.1.20:

3.1.20
======
:release-date: 2016-01-22 06:50 p.m. UTC
:release-by: Ask Solem

- **Requirements**

    - Now depends on :ref:`Kombu 3.0.33 <kombu:version-3.0.33>`.

    - Now depends on :mod:`billiard` 3.3.0.22.

        Includes binary wheels for Microsoft Windows x86 and x86_64!

- **Task**: Error emails now uses ``utf-8`` character set by default
  (Issue #2737).

- **Task**: Retry now forwards original message headers (Issue #3017).

- **Worker**: Bootsteps can now hook into ``on_node_join``/``leave``/``lost``.

    See :ref:`extending-consumer-attributes` for an example.

- **Events**: Fixed handling of DST timezones (Issue #2983).

- **Results**: Redis backend stopped respecting certain settings.

    Contributed by Jeremy Llewellyn.

- **Results**: Database backend now properly supports JSON exceptions
  (Issue #2441).

- **Results**: Redis ``new_join`` didn't properly call task errbacks on chord
  error (Issue #2796).

- **Results**: Restores Redis compatibility with Python :pypi:`redis` < 2.10.0
  (Issue #2903).

- **Results**: Fixed rare issue with chord error handling (Issue #2409).

- **Tasks**: Using queue-name values in :setting:`CELERY_ROUTES` now works
  again (Issue #2987).

- **General**: Result backend password now sanitized in report output
  (Issue #2812, Issue #2004).

- **Configuration**: Now gives helpful error message when the result backend
  configuration points to a module, and not a class (Issue #2945).

- **Results**: Exceptions sent by JSON serialized workers are now properly
  handled by pickle configured workers.

- **Programs**: ``celery control autoscale`` now works (Issue #2950).

- **Programs**: ``celery beat --detached`` now runs after fork callbacks.

- **General**: Fix for LRU cache implementation on Python 3.5 (Issue #2897).

    Contributed by Dennis Brakhane.

    Python 3.5's ``OrderedDict`` doesn't allow mutation while it is being
    iterated over. This breaks "update" if it is called with a dict
    larger than the maximum size.

    This commit changes the code to a version that doesn't iterate over
    the dict, and should also be a little bit faster.

- **Init-scripts**: The beat init-script now properly reports service as down
  when no pid file can be found.

    Eric Zarowny

- **Beat**: Added cleaning of corrupted scheduler files for some storage
  backend errors (Issue #2985).

    Fix contributed by Aleksandr Kuznetsov.

- **Beat**: Now syncs the schedule even if the schedule is empty.

    Fix contributed by Colin McIntosh.

- **Supervisord**: Set higher process priority in the :pypi:`supervisord`
    example.

    Contributed by George Tantiras.

- **Documentation**: Includes improvements by:

    :github_user:`Bryson`
    Caleb Mingle
    Christopher Martin
    Dieter Adriaenssens
    Jason Veatch
    Jeremy Cline
    Juan Rossi
    Kevin Harvey
    Kevin McCarthy
    Kirill Pavlov
    Marco Buttu
    :github_user:`Mayflower`
    Mher Movsisyan
    Michael Floering
    :github_user:`michael-k`
    Nathaniel Varona
    Rudy Attias
    Ryan Luckie
    Steven Parker
    :github_user:`squfrans`
    Tadej Janež
    TakesxiSximada
    Tom S

.. _version-3.1.19:

3.1.19
======
:release-date: 2015-10-26 01:00 p.m. UTC
:release-by: Ask Solem

- **Requirements**

    - Now depends on :ref:`Kombu 3.0.29 <kombu:version-3.0.29>`.

    - Now depends on :mod:`billiard` 3.3.0.21.

-  **Results**: Fixed MongoDB result backend URL parsing problem
   (Issue celery/kombu#375).

- **Worker**: Task request now properly sets ``priority`` in delivery_info.

    Fix contributed by Gerald Manipon.

- **Beat**: PyPy shelve may raise ``KeyError`` when setting keys
  (Issue #2862).

- **Programs**: :program:`celery beat --deatched` now working on PyPy.

    Fix contributed by Krzysztof Bujniewicz.

- **Results**: Redis result backend now ensures all pipelines are cleaned up.

    Contributed by Justin Patrin.

- **Results**: Redis result backend now allows for timeout to be set in the
  query portion of the result backend URL.

    For example ``CELERY_RESULT_BACKEND = 'redis://?timeout=10'``

    Contributed by Justin Patrin.

- **Results**: ``result.get`` now properly handles failures where the
  exception value is set to :const:`None` (Issue #2560).

- **Prefork pool**: Fixed attribute error ``proc.dead``.

- **Worker**: Fixed worker hanging when gossip/heartbeat disabled
  (Issue #1847).

    Fix contributed by Aaron Webber and Bryan Helmig.

- **Results**: MongoDB result backend now supports pymongo 3.x
  (Issue #2744).

    Fix contributed by Sukrit Khera.

- **Results**: RPC/AMQP backends didn't deserialize exceptions properly
  (Issue #2691).

    Fix contributed by Sukrit Khera.

- **Programs**: Fixed problem with :program:`celery amqp`'s
  ``basic_publish`` (Issue #2013).

- **Worker**: Embedded beat now properly sets app for thread/process
  (Issue #2594).

- **Documentation**: Many improvements and typos fixed.

    Contributions by:

        Carlos Garcia-Dubus
        D. Yu
        :github_user:`jerry`
        Jocelyn Delalande
        Josh Kupershmidt
        Juan Rossi
        :github_user:`kanemra`
        Paul Pearce
        Pavel Savchenko
        Sean Wang
        Seungha Kim
        Zhaorong Ma

.. _version-3.1.18:

3.1.18
======
:release-date: 2015-04-22 05:30 p.m. UTC
:release-by: Ask Solem

- **Requirements**

    - Now depends on :ref:`Kombu 3.0.25 <kombu:version-3.0.25>`.

    - Now depends on :mod:`billiard` 3.3.0.20.

- **Django**: Now supports Django 1.8 (Issue #2536).

    Fix contributed by Bence Tamas and Mickaël Penhard.

- **Results**: MongoDB result backend now compatible with pymongo 3.0.

    Fix contributed by Fatih Sucu.

- **Tasks**: Fixed bug only happening when a task has multiple callbacks
  (Issue #2515).

    Fix contributed by NotSqrt.

- **Commands**: Preload options now support ``--arg value`` syntax.

    Fix contributed by John Anderson.

- **Compat**: A typo caused ``celery.log.setup_logging_subsystem`` to be
  undefined.

    Fix contributed by Gunnlaugur Thor Briem.

- **init-scripts**: The beat generic init-script now uses
  :file:`/bin/sh` instead of :command:`bash` (Issue #2496).

    Fix contributed by Jelle Verstraaten.

- **Django**: Fixed a :exc:`TypeError` sometimes occurring in logging
  when validating models.

    Fix contributed by Alexander.

- **Commands**: Worker now supports new
  :option:`--executable <celery worker --executable>` argument that can
  be used with :option:`celery worker --detach`.

    Contributed by Bert Vanderbauwhede.

- **Canvas**: Fixed crash in chord unlock fallback task (Issue #2404).

- **Worker**: Fixed rare crash occurring with
  :option:`--autoscale <celery worker --autoscale>` enabled (Issue #2411).

- **Django**: Properly recycle worker Django database connections when the
  Django ``CONN_MAX_AGE`` setting is enabled (Issue #2453).

    Fix contributed by Luke Burden.

.. _version-3.1.17:

3.1.17
======
:release-date: 2014-11-19 03:30 p.m. UTC
:release-by: Ask Solem

.. admonition:: Don't enable the `CELERYD_FORCE_EXECV` setting!

    Please review your configuration and disable this option if you're using the
    RabbitMQ or Redis transport.

    Keeping this option enabled after 3.1 means the async based prefork pool will
    be disabled, which can easily cause instability.

- **Requirements**

    - Now depends on :ref:`Kombu 3.0.24 <kombu:version-3.0.24>`.

        Includes the new Qpid transport coming in Celery 3.2, backported to
        support those who may still require Python 2.6 compatibility.

    - Now depends on :mod:`billiard` 3.3.0.19.

    - ``celery[librabbitmq]`` now depends on librabbitmq 1.6.1.

- **Task**: The timing of ETA/countdown tasks were off after the example ``LocalTimezone``
  implementation in the Python documentation no longer works in Python 3.4.
  (Issue #2306).

- **Task**: Raising :exc:`~celery.exceptions.Ignore` no longer sends
  ``task-failed`` event (Issue #2365).

- **Redis result backend**: Fixed unbound local errors.

    Fix contributed by Thomas French.

- **Task**: Callbacks wasn't called properly if ``link`` was a list of
  signatures (Issue #2350).

- **Canvas**: chain and group now handles json serialized signatures
  (Issue #2076).

- **Results**: ``.join_native()`` would accidentally treat the ``STARTED``
  state as being ready (Issue #2326).

    This could lead to the chord callback being called with invalid arguments
    when using chords with the :setting:`CELERY_TRACK_STARTED` setting
    enabled.

- **Canvas**: The ``chord_size`` attribute is now set for all canvas primitives,
  making sure more combinations will work with the ``new_join`` optimization
  for Redis (Issue #2339).

- **Task**: Fixed problem with app not being properly propagated to
  ``trace_task`` in all cases.

    Fix contributed by :github_user:`kristaps`.

- **Worker**: Expires from task message now associated with a timezone.

    Fix contributed by Albert Wang.

- **Cassandra result backend**: Fixed problems when using detailed mode.

    When using the Cassandra backend in detailed mode, a regression
    caused errors when attempting to retrieve results.

    Fix contributed by Gino Ledesma.

- **Mongodb Result backend**: Pickling the backend instance will now include
  the original URL (Issue #2347).

    Fix contributed by Sukrit Khera.

- **Task**: Exception info wasn't properly set for tasks raising
  :exc:`~celery.exceptions.Reject` (Issue #2043).

- **Worker**: Duplicates are now removed when loading the set of revoked tasks
  from the worker state database (Issue #2336).

- **celery.contrib.rdb**: Fixed problems with ``rdb.set_trace`` calling stop
  from the wrong frame.

    Fix contributed by :github_user:`llllllllll`.

- **Canvas**: ``chain`` and ``chord`` can now be immutable.

- **Canvas**: ``chord.apply_async`` will now keep partial args set in
  ``self.args`` (Issue #2299).

- **Results**: Small refactoring so that results are decoded the same way in
  all result backends.

- **Logging**: The ``processName`` format was introduced in Python 2.6.2 so for
  compatibility this format is now excluded when using earlier versions
  (Issue #1644).

.. _version-3.1.16:

3.1.16
======
:release-date: 2014-10-03 06:00 p.m. UTC
:release-by: Ask Solem

- **Worker**: 3.1.15 broke :option:`-Ofair <celery worker -O>`
  behavior (Issue #2286).

    This regression could result in all tasks executing
    in a single child process if ``-Ofair`` was enabled.

- **Canvas**: ``celery.signature`` now properly forwards app argument
  in all cases.

- **Task**: ``.retry()`` didn't raise the exception correctly
  when called without a current exception.

    Fix contributed by Andrea Rabbaglietti.

- **Worker**: The ``enable_events`` remote control command
  disabled worker-related events by mistake (Issue #2272).

    Fix contributed by Konstantinos Koukopoulos.

- **Django**: Adds support for Django 1.7 class names in INSTALLED_APPS
  when using ``app.autodiscover_tasks()``  (Issue #2248).

- **Sphinx**: ``celery.contrib.sphinx`` now uses ``getfullargspec``
  on Python 3 (Issue #2302).

- **Redis/Cache Backends**: Chords will now run at most once if one or more tasks
  in the chord are executed multiple times for some reason.

.. _version-3.1.15:

3.1.15
======
:release-date: 2014-09-14 11:00 p.m. UTC
:release-by: Ask Solem

- **Django**: Now makes sure ``django.setup()`` is called
  before importing any task modules (Django 1.7 compatibility, Issue #2227)

- **Results**: ``result.get()`` was misbehaving by calling
  ``backend.get_task_meta`` in a :keyword:`finally` call leading to
  AMQP result backend queues not being properly cleaned up (Issue #2245).

.. _version-3.1.14:

3.1.14
======
:release-date: 2014-09-08 03:00 p.m. UTC
:release-by: Ask Solem

- **Requirements**

    - Now depends on :ref:`Kombu 3.0.22 <kombu:version-3.0.22>`.

- **Init-scripts**: The generic worker init-scripts ``status`` command
  now gets an accurate pidfile list (Issue #1942).

- **Init-scripts**: The generic beat script now implements the ``status``
   command.

    Contributed by John Whitlock.

- **Commands**: Multi now writes informational output to stdout instead of stderr.

- **Worker**: Now ignores not implemented error for ``pool.restart``
  (Issue #2153).

- **Task**: Retry no longer raises retry exception when executed in eager
  mode (Issue #2164).

- **AMQP Result backend**: Now ensured ``on_interval`` is called at least
  every second for blocking calls to properly propagate parent errors.

- **Django**: Compatibility with Django 1.7 on Windows (Issue #2126).

- **Programs**: :option:`--umask <celery --umask>` argument can now be
  specified in both octal (if starting with 0) or decimal.


.. _version-3.1.13:

3.1.13
======

Security Fixes
--------------

* [Security: `CELERYSA-0002`_] Insecure default umask.

    The built-in utility used to daemonize the Celery worker service sets
    an insecure umask by default (umask 0).

    This means that any files or directories created by the worker will
    end up having world-writable permissions.

    Special thanks to Red Hat for originally discovering and reporting the
    issue!

    This version will no longer set a default umask by default, so if unset
    the umask of the parent process will be used.

.. _`CELERYSA-0002`:
    https://github.com/celery/celery/tree/master/docs/sec/CELERYSA-0002.txt

News
----

- **Requirements**

    - Now depends on :ref:`Kombu 3.0.21 <kombu:version-3.0.21>`.

    - Now depends on :mod:`billiard` 3.3.0.18.


- **App**: ``backend`` argument now also sets the :setting:`CELERY_RESULT_BACKEND`
  setting.

- **Task**: ``signature_from_request`` now propagates ``reply_to`` so that
  the RPC backend works with retried tasks (Issue #2113).

- **Task**: ``retry`` will no longer attempt to re-queue the task if sending
  the retry message fails.

    Unrelated exceptions being raised could cause a message loop, so it was
    better to remove this behavior.

- **Beat**: Accounts for standard 1ms drift by always waking up 0.010s
  earlier.

    This will adjust the latency so that the periodic tasks won't move
    1ms after every invocation.

- Documentation fixes

    Contributed by Yuval Greenfield, Lucas Wiman, :github_user:`nicholsonjf`.

- **Worker**: Removed an outdated assert statement that could lead to errors
  being masked (Issue #2086).



.. _version-3.1.12:

3.1.12
======
:release-date: 2014-06-09 10:12 p.m. UTC
:release-by: Ask Solem

- **Requirements**

    Now depends on :ref:`Kombu 3.0.19 <kombu:version-3.0.19>`.

- **App**: Connections weren't being closed after fork due to an error in the
  after fork handler (Issue #2055).

    This could manifest itself by causing framing errors when using RabbitMQ.
    (``Unexpected frame``).

- **Django**: ``django.setup()`` was being called too late when
  using Django 1.7 (Issue #1802).

- **Django**: Fixed problems with event timezones when using Django
  (``Substantial drift``).

    Celery didn't take into account that Django modifies the
    ``time.timeone`` attributes and friends.

- **Canvas**: ``Signature.link`` now works when the link option is a scalar
  value (Issue #2019).

- **Prefork pool**: Fixed race conditions for when file descriptors are
  removed from the event loop.

    Fix contributed by Roger Hu.

- **Prefork pool**: Improved solution for dividing tasks between child
  processes.

    This change should improve performance when there are many child
    processes, and also decrease the chance that two subsequent tasks are
    written to the same child process.

- **Worker**: Now ignores unknown event types, instead of crashing.

    Fix contributed by Illes Solt.

- **Programs**: :program:`celery worker --detach` no longer closes open file
  descriptors when :envvar:`C_FAKEFORK` is used so that the workers output
  can be seen.

- **Programs**: The default working directory for :program:`celery worker
  --detach` is now the current working directory, not ``/``.

- **Canvas**: ``signature(s, app=app)`` didn't upgrade serialized signatures
  to their original class (``subtask_type``) when the ``app`` keyword argument
  was used.

- **Control**: The ``duplicate nodename`` warning emitted by control commands
  now shows the duplicate node name.

- **Tasks**: Can now call ``ResultSet.get()`` on a result set without members.

    Fix contributed by Alexey Kotlyarov.

- **App**: Fixed strange traceback mangling issue for
  ``app.connection_or_acquire``.

- **Programs**: The :program:`celery multi stopwait` command is now documented
  in usage.

- **Other**: Fixed cleanup problem with ``PromiseProxy`` when an error is
  raised while trying to evaluate the promise.

- **Other**: The utility used to censor configuration values now handles
  non-string keys.

    Fix contributed by Luke Pomfrey.

- **Other**: The ``inspect conf`` command didn't handle non-string keys well.

    Fix contributed by Jay Farrimond.

- **Programs**: Fixed argument handling problem in
  :program:`celery worker --detach`.

    Fix contributed by Dmitry Malinovsky.

- **Programs**: :program:`celery worker --detach` didn't forward working
  directory option (Issue #2003).

- **Programs**: :program:`celery inspect registered` no longer includes
  the list of built-in tasks.

- **Worker**: The ``requires`` attribute for boot steps weren't being handled
  correctly (Issue #2002).

- **Eventlet**: The eventlet pool now supports the ``pool_grow`` and
  ``pool_shrink`` remote control commands.

    Contributed by Mher Movsisyan.

- **Eventlet**: The eventlet pool now implements statistics for
  :program:``celery inspect stats``.

    Contributed by Mher Movsisyan.

- **Documentation**: Clarified ``Task.rate_limit`` behavior.

    Contributed by Jonas Haag.

- **Documentation**: ``AbortableTask`` examples now updated to use the new
  API (Issue #1993).

- **Documentation**: The security documentation examples used an out of date
  import.

    Fix contributed by Ian Dees.

- **Init-scripts**: The CentOS init-scripts didn't quote
  :envvar:`CELERY_CHDIR`.

    Fix contributed by :github_user:`ffeast`.

.. _version-3.1.11:

3.1.11
======
:release-date: 2014-04-16 11:00 p.m. UTC
:release-by: Ask Solem

- **Now compatible with RabbitMQ 3.3.0**

    You need to run Celery 3.1.11 or later when using RabbitMQ 3.3,
    and if you use the ``librabbitmq`` module you also have to upgrade
    to librabbitmq 1.5.0:

    .. code-block:: bash

        $ pip install -U librabbitmq

- **Requirements**:

    - Now depends on :ref:`Kombu 3.0.15 <kombu:version-3.0.15>`.

    - Now depends on `billiard 3.3.0.17`_.

    - Bundle ``celery[librabbitmq]`` now depends on :mod:`librabbitmq` 1.5.0.

.. _`billiard 3.3.0.17`:
    https://github.com/celery/billiard/blob/master/CHANGES.txt

- **Tasks**: The :setting:`CELERY_DEFAULT_DELIVERY_MODE` setting was being
  ignored (Issue #1953).

- **Worker**: New :option:`celery worker --heartbeat-interval` can be used
  to change the time (in seconds) between sending event heartbeats.

    Contributed by Matthew Duggan and Craig Northway.

- **App**: Fixed memory leaks occurring when creating lots of temporary
  app instances (Issue #1949).

- **MongoDB**: SSL configuration with non-MongoDB transport breaks MongoDB
  results backend (Issue #1973).

    Fix contributed by Brian Bouterse.

- **Logging**: The color formatter accidentally modified ``record.msg``
  (Issue #1939).

- **Results**: Fixed problem with task trails being stored multiple times,
  causing ``result.collect()`` to hang (Issue #1936, Issue #1943).

- **Results**: ``ResultSet`` now implements a ``.backend`` attribute for
  compatibility with ``AsyncResult``.

- **Results**: ``.forget()`` now also clears the local cache.

- **Results**: Fixed problem with multiple calls to ``result._set_cache``
  (Issue #1940).

- **Results**: ``join_native`` populated result cache even if disabled.

- **Results**: The YAML result serializer should now be able to handle storing
  exceptions.

- **Worker**: No longer sends task error emails for expected errors (in
  ``@task(throws=(..., )))``.

- **Canvas**: Fixed problem with exception deserialization when using
  the JSON serializer (Issue #1987).

- **Eventlet**: Fixes crash when ``celery.contrib.batches`` attempted to
  cancel a non-existing timer (Issue #1984).

- Can now import ``celery.version_info_t``, and ``celery.five`` (Issue #1968).


.. _version-3.1.10:

3.1.10
======
:release-date: 2014-03-22 09:40 p.m. UTC
:release-by: Ask Solem

- **Requirements**:

    - Now depends on :ref:`Kombu 3.0.14 <kombu:version-3.0.14>`.

- **Results**:

    Reliability improvements to the SQLAlchemy database backend. Previously the
    connection from the MainProcess was improperly shared with the workers.
    (Issue #1786)

- **Redis:** Important note about events (Issue #1882).

    There's a new transport option for Redis that enables monitors
    to filter out unwanted events. Enabling this option in the workers
    will increase performance considerably:

    .. code-block:: python

        BROKER_TRANSPORT_OPTIONS = {'fanout_patterns': True}

    Enabling this option means that your workers won't be able to see
    workers with the option disabled (or is running an older version of
    Celery), so if you do enable it then make sure you do so on all
    nodes.

    See :ref:`redis-caveats`.

    This will be the default in Celery 3.2.

- **Results**: The :class:`@AsyncResult` object now keeps a local cache
  of the final state of the task.

    This means that the global result cache can finally be disabled,
    and you can do so by setting :setting:`CELERY_MAX_CACHED_RESULTS` to
    :const:`-1`. The lifetime of the cache will then be bound to the
    lifetime of the result object, which will be the default behavior
    in Celery 3.2.

- **Events**: The "Substantial drift" warning message is now logged once
  per node name only (Issue #1802).

- **Worker**: Ability to use one log file per child process when using the
  prefork pool.

    This can be enabled by using the new ``%i`` and ``%I`` format specifiers
    for the log file name. See :ref:`worker-files-process-index`.

- **Redis**: New experimental chord join implementation.

    This is an optimization for chords when using the Redis result backend,
    where the join operation is now considerably faster and using less
    resources than the previous strategy.

    The new option can be set in the result backend URL:

    .. code-block:: python

        CELERY_RESULT_BACKEND = 'redis://localhost?new_join=1'

    This must be enabled manually as it's incompatible
    with workers and clients not using it, so be sure to enable
    the option in all clients and workers if you decide to use it.

- **Multi**: With ``-opt:index`` (e.g., ``-c:1``) the index now always refers
  to the position of a node in the argument list.

    This means that referring to a number will work when specifying a list
    of node names and not just for a number range:

    .. code-block:: bash

        celery multi start A B C D -c:1 4 -c:2-4 8

    In this example ``1`` refers to node A (as it's the first node in the
    list).

- **Signals**: The sender argument to ``Signal.connect`` can now be a proxy
  object, which means that it can be used with the task decorator
  (Issue #1873).

- **Task**: A regression caused the ``queue`` argument to ``Task.retry`` to be
  ignored (Issue #1892).

- **App**: Fixed error message for :meth:`~@Celery.config_from_envvar`.

    Fix contributed by Dmitry Malinovsky.

- **Canvas**: Chords can now contain a group of other chords (Issue #1921).

- **Canvas**: Chords can now be combined when using the amqp result backend
  (a chord where the callback is also a chord).

- **Canvas**: Calling ``result.get()`` for a chain task will now complete
  even if one of the tasks in the chain is ``ignore_result=True``
  (Issue #1905).

- **Canvas**: Worker now also logs chord errors.

- **Canvas**: A chord task raising an exception will now result in
  any errbacks (``link_error``) to the chord callback to also be called.

- **Results**: Reliability improvements to the SQLAlchemy database backend
  (Issue #1786).

    Previously the connection from the ``MainProcess`` was improperly
    inherited by child processes.

    Fix contributed by Ionel Cristian Mărieș.

- **Task**: Task callbacks and errbacks are now called using the group
  primitive.

- **Task**: ``Task.apply`` now properly sets ``request.headers``
  (Issue #1874).

- **Worker**: Fixed :exc:`UnicodeEncodeError` occurring when worker is started
  by :pypi:`supervisor`.

    Fix contributed by Codeb Fan.

- **Beat**: No longer attempts to upgrade a newly created database file
  (Issue #1923).

- **Beat**: New setting :setting:``CELERYBEAT_SYNC_EVERY`` can be be used
  to control file sync by specifying the number of tasks to send between
  each sync.

    Contributed by Chris Clark.

- **Commands**: :program:`celery inspect memdump` no longer crashes
  if the :mod:`psutil` module isn't installed (Issue #1914).

- **Worker**: Remote control commands now always accepts json serialized
  messages (Issue #1870).

- **Worker**: Gossip will now drop any task related events it receives
  by mistake (Issue #1882).


.. _version-3.1.9:

3.1.9
=====
:release-date: 2014-02-10 06:43 p.m. UTC
:release-by: Ask Solem

- **Requirements**:

    - Now depends on :ref:`Kombu 3.0.12 <kombu:version-3.0.12>`.

- **Prefork pool**: Better handling of exiting child processes.

    Fix contributed by Ionel Cristian Mărieș.

- **Prefork pool**: Now makes sure all file descriptors are removed
  from the hub when a process is cleaned up.

    Fix contributed by Ionel Cristian Mărieș.

- **New Sphinx extension**: for autodoc documentation of tasks:
  :mod:`celery.contrib.spinx` (Issue #1833).

- **Django**: Now works with Django 1.7a1.

- **Task**: Task.backend is now a property that forwards to ``app.backend``
  if no custom backend has been specified for the task (Issue #1821).

- **Generic init-scripts**: Fixed bug in stop command.

    Fix contributed by Rinat Shigapov.

- **Generic init-scripts**: Fixed compatibility with GNU :manpage:`stat`.

    Fix contributed by Paul Kilgo.

- **Generic init-scripts**: Fixed compatibility with the minimal
  :program:`dash` shell (Issue #1815).

- **Commands**: The :program:`celery amqp basic.publish` command wasn't
  working properly.

    Fix contributed by Andrey Voronov.

- **Commands**: Did no longer emit an error message if the pidfile exists
  and the process is still alive (Issue #1855).

- **Commands**: Better error message for missing arguments to preload
  options (Issue #1860).

- **Commands**: :program:`celery -h` didn't work because of a bug in the
  argument parser (Issue #1849).

- **Worker**: Improved error message for message decoding errors.

- **Time**: Now properly parses the `Z` timezone specifier in ISO 8601 date
  strings.

    Fix contributed by Martin Davidsson.

- **Worker**: Now uses the *negotiated* heartbeat value to calculate
  how often to run the heartbeat checks.

- **Beat**: Fixed problem with beat hanging after the first schedule
  iteration (Issue #1822).

    Fix contributed by Roger Hu.

- **Signals**: The header argument to :signal:`before_task_publish` is now
  always a dictionary instance so that signal handlers can add headers.

- **Worker**: A list of message headers is now included in message related
  errors.

.. _version-3.1.8:

3.1.8
=====
:release-date: 2014-01-17 10:45 p.m. UTC
:release-by: Ask Solem

- **Requirements**:

    - Now depends on :ref:`Kombu 3.0.10 <kombu:version-3.0.10>`.

    - Now depends on `billiard 3.3.0.14`_.

.. _`billiard 3.3.0.14`:
    https://github.com/celery/billiard/blob/master/CHANGES.txt

- **Worker**: The event loop wasn't properly reinitialized at consumer restart
  which would force the worker to continue with a closed ``epoll`` instance on
  Linux, resulting in a crash.

- **Events:** Fixed issue with both heartbeats and task events that could
  result in the data not being kept in sorted order.

    As a result this would force the worker to log "heartbeat missed"
    events even though the remote node was sending heartbeats in a timely manner.

- **Results:** The pickle serializer no longer converts group results to tuples,
  and will keep the original type (*Issue #1750*).

- **Results:** ``ResultSet.iterate`` is now pending deprecation.

    The method will be deprecated in version 3.2 and removed in version 3.3.

    Use ``result.get(callback=)`` (or ``result.iter_native()`` where available)
    instead.

- **Worker**\|eventlet/gevent: A regression caused :kbd:`Control-c` to be
  ineffective for shutdown.

- **Redis result backend:** Now using a pipeline to store state changes
  for improved performance.

    Contributed by Pepijn de Vos.

- **Redis result backend:** Will now retry storing the result if disconnected.

- **Worker**\|gossip: Fixed attribute error occurring when another node leaves.

    Fix contributed by Brodie Rao.

- **Generic init-scripts:** Now runs a check at start-up to verify
  that any configuration scripts are owned by root and that they
  aren't world/group writable.

    The init-script configuration is a shell script executed by root,
    so this is a preventive measure to ensure that users don't
    leave this file vulnerable to changes by unprivileged users.

    .. note::

        Note that upgrading Celery won't update the init-scripts,
        instead you need to manually copy the improved versions from the
        source distribution:
        https://github.com/celery/celery/tree/3.1/extra/generic-init.d

- **Commands**: The :program:`celery purge` command now warns that the operation
  will delete all tasks and prompts the user for confirmation.

    A new :option:`-f <celery purge -f>` was added that can be used to disable
    interactive mode.

- **Task**: ``.retry()`` didn't raise the value provided in the ``exc`` argument
  when called outside of an error context (*Issue #1755*).

- **Commands:** The :program:`celery multi` command didn't forward command
  line configuration to the target workers.

    The change means that multi will forward the special ``--`` argument and
    configuration content at the end of the arguments line to the specified
    workers.

    Example using command-line configuration to set a broker heartbeat
    from :program:`celery multi`:

    .. code-block:: bash

        $ celery multi start 1 -c3 -- broker.heartbeat=30

    Fix contributed by Antoine Legrand.

- **Canvas:** ``chain.apply_async()`` now properly forwards execution options.

    Fix contributed by Konstantin Podshumok.

- **Redis result backend:** Now takes ``connection_pool`` argument that can be
  used to change the connection pool class/constructor.

- **Worker:** Now truncates very long arguments and keyword arguments logged by
  the pool at debug severity.

- **Worker:** The worker now closes all open files on :sig:`SIGHUP` (regression)
  (*Issue #1768*).

    Fix contributed by Brodie Rao

- **Worker:** Will no longer accept remote control commands while the
  worker start-up phase is incomplete (*Issue #1741*).

- **Commands:** The output of the event dump utility
  (:program:`celery events -d`) can now be piped into other commands.

- **Documentation:** The RabbitMQ installation instructions for macOS was
  updated to use modern Homebrew practices.

    Contributed by Jon Chen.

- **Commands:** The :program:`celery inspect conf` utility now works.

- **Commands:** The :option:`--no-color <celery --no-color>` argument was
  not respected by all commands (*Issue #1799*).

- **App:** Fixed rare bug with ``autodiscover_tasks()`` (*Issue #1797*).

- **Distribution:** The sphinx docs will now always add the parent directory
  to path so that the current Celery source code is used as a basis for
  API documentation (*Issue #1782*).

- **Documentation:** :pypi:`supervisor` examples contained an
  extraneous '-' in a :option:`--logfile <celery worker --logfile>` argument
  example.

    Fix contributed by Mohammad Almeer.

.. _version-3.1.7:

3.1.7
=====
:release-date: 2013-12-17 06:00 p.m. UTC
:release-by: Ask Solem

.. _v317-important:

Important Notes
---------------

Init-script security improvements
---------------------------------

Where the generic init-scripts (for ``celeryd``, and ``celerybeat``) before
delegated the responsibility of dropping privileges to the target application,
it will now use ``su`` instead, so that the Python program isn't trusted
with superuser privileges.

This isn't in reaction to any known exploit, but it will
limit the possibility of a privilege escalation bug being abused in the
future.

You have to upgrade the init-scripts manually from this directory:
https://github.com/celery/celery/tree/3.1/extra/generic-init.d

AMQP result backend
~~~~~~~~~~~~~~~~~~~

The 3.1 release accidentally left the amqp backend configured to be
non-persistent by default.

Upgrading from 3.0 would give a "not equivalent" error when attempting to
set or retrieve results for a task. That's unless you manually set the
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

If you really know what you're doing you can avoid the warning (and
the future exception being raised) by moving the operation in a
white-list block:

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

- Init-scripts:  Fixed problem with reading configuration file
  when the init-script is symlinked to a runlevel (e.g., ``S02celeryd``).
  (Issue #1740).

    This also removed a rarely used feature where you can symlink the script
    to provide alternative configurations. You instead copy the script
    and give it a new name, but perhaps a better solution is to provide
    arguments to ``CELERYD_OPTS`` to separate them:

    .. code-block:: bash

        CELERYD_NODES="X1 X2 Y1 Y2"
        CELERYD_OPTS="-A:X1 x -A:X2 x -A:Y1 y -A:Y2 y"

- Fallback chord unlock task is now always called after the chord header
  (Issue #1700).

    This means that the unlock task won't be started if there's
    an error sending the header.

- Celery command: Fixed problem with arguments for some control commands.

    Fix contributed by Konstantin Podshumok.

- Fixed bug in ``utcoffset`` where the offset when in DST would be
  completely wrong (Issue #1743).

- Worker: Errors occurring while attempting to serialize the result of a
  task will now cause the task to be marked with failure and a
  :class:`kombu.exceptions.EncodingError` error.

    Fix contributed by Ionel Cristian Mărieș.

- Worker with :option:`-B <celery worker -B>` argument didn't properly
  shut down the beat instance.

- Worker: The ``%n`` and ``%h`` formats are now also supported by the
  :option:`--logfile <celery worker --logfile>`,
  :option:`--pidfile <celery worker --pidfile>` and
  :option:`--statedb <celery worker --statedb>` arguments.

    Example:

    .. code-block:: bash

        $ celery -A proj worker -n foo@%h --logfile=%n.log --statedb=%n.db

- Redis/Cache result backends: Will now timeout if keys evicted while trying
  to join a chord.

- The fallback unlock chord task now raises :exc:`Retry` so that the
  retry even is properly logged by the worker.

- Multi: Will no longer apply Eventlet/gevent monkey patches (Issue #1717).

- Redis result backend: Now supports UNIX sockets.

    Like the Redis broker transport the result backend now also supports
    using ``redis+socket:///tmp/redis.sock`` URLs.

    Contributed by Alcides Viamontes Esquivel.

- Events: Events sent by clients was mistaken for worker related events
  (Issue #1714).

    For ``events.State`` the tasks now have a ``Task.client`` attribute
    that's set when a ``task-sent`` event is being received.

    Also, a clients logical clock isn't in sync with the cluster so
    they live in a "time bubble." So for this reason monitors will no
    longer attempt to merge with the clock of an event sent by a client,
    instead it will fake the value by using the current clock with
    a skew of -1.

- Prefork pool: The method used to find terminated processes was flawed
  in that it didn't also take into account missing ``popen`` objects.

- Canvas: ``group`` and ``chord`` now works with anon signatures as long
  as the group/chord object is associated with an app instance (Issue #1744).

    You can pass the app by using ``group(..., app=app)``.

.. _version-3.1.6:

3.1.6
=====
:release-date: 2013-12-02 06:00 p.m. UTC
:release-by: Ask Solem

- Now depends on :mod:`billiard` 3.3.0.10.

- Now depends on :ref:`Kombu 3.0.7 <kombu:version-3.0.7>`.

- Fixed problem where Mingle caused the worker to hang at start-up
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

- CentOS init-script: Now compatible with SysV style init symlinks.

    Fix contributed by Jonathan Jordan.

- Events: Fixed problem when task name isn't defined (Issue #1710).

    Fix contributed by Mher Movsisyan.

- Task: Fixed unbound local errors (Issue #1684).

    Fix contributed by Markus Ullmann.

- Canvas: Now unrolls groups with only one task (optimization) (Issue #1656).

- Task: Fixed problem with ETA and timezones.

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
:release-date: 2013-11-21 06:20 p.m. UTC
:release-by: Ask Solem

- Now depends on :ref:`Kombu 3.0.6 <kombu:version-3.0.6>`.

- Now depends on :mod:`billiard` 3.3.0.8

- App: ``config_from_object`` is now lazy (Issue #1665).

- App: ``autodiscover_tasks`` is now lazy.

    Django users should now wrap access to the settings object
    in a lambda::

        app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

    this ensures that the settings object isn't prepared
    prematurely.

- Fixed regression for :option:`--app <celery --app>` argument
  experienced by some users (Issue #1653).

- Worker: Now respects the :option:`--uid <celery worker --uid>` and
  :option:`--gid <celery worker --gid>` arguments even if
  :option:`--detach <celery worker --detach>` isn't enabled.

- Beat: Now respects the :option:`--uid <celery beat --uid>` and
  :option:`--gid <celery beat --gid>` arguments even if
  :option:`--detach <celery beat --detach>` isn't enabled.

- Python 3: Fixed unorderable error occurring with the worker
  :option:`-B <celery worker -B>` argument enabled.

- ``celery.VERSION`` is now a named tuple.

- ``maybe_signature(list)`` is now applied recursively (Issue #1645).

- ``celery shell`` command: Fixed ``IPython.frontend`` deprecation warning.

- The default app no longer includes the built-in fix-ups.

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

- The worker didn't send monitoring events during shutdown.

- Worker: Mingle and gossip is now automatically disabled when
  used with an unsupported transport (Issue #1664).

- ``celery`` command:  Preload options now supports
  the rare ``--opt value`` format (Issue #1668).

- ``celery`` command: Accidentally removed options
  appearing before the sub-command, these are now moved to the end
  instead.

- Worker now properly responds to ``inspect stats`` commands
  even if received before start-up is complete (Issue #1659).

- :signal:`task_postrun` is now sent within a :keyword:`finally` block,
  to make sure the signal is always sent.

- Beat: Fixed syntax error in string formatting.

    Contributed by :github_user:`nadad`.

- Fixed typos in the documentation.

    Fixes contributed by Loic Bistuer, :github_user:`sunfinite`.

- Nested chains now works properly when constructed using the
  ``chain`` type instead of the ``|`` operator (Issue #1656).

.. _version-3.1.4:

3.1.4
=====
:release-date: 2013-11-15 11:40 p.m. UTC
:release-by: Ask Solem

- Now depends on :ref:`Kombu 3.0.5 <kombu:version-3.0.5>`.

- Now depends on :mod:`billiard` 3.3.0.7

- Worker accidentally set a default socket timeout of 5 seconds.

- Django: Fix-up now sets the default app so that threads will use
  the same app instance (e.g., for :command:`manage.py runserver`).

- Worker: Fixed Unicode error crash at start-up experienced by some users.

- Calling ``.apply_async`` on an empty chain now works again (Issue #1650).

- The ``celery multi show`` command now generates the same arguments
  as the start command does.

- The :option:`--app <celery --app>` argument could end up using a module
  object instead of an app instance (with a resulting crash).

- Fixed a syntax error problem in the beat init-script.

    Fix contributed by Vsevolod.

- Tests now passing on PyPy 2.1 and 2.2.

.. _version-3.1.3:

3.1.3
=====
:release-date: 2013-11-13 00:55 a.m. UTC
:release-by: Ask Solem

- Fixed compatibility problem with Python 2.7.0 - 2.7.5 (Issue #1637)

    ``unpack_from`` started supporting ``memoryview`` arguments
    in Python 2.7.6.

- Worker: :option:`-B <celery worker -B>` argument accidentally closed
  files used for logging.

- Task decorated tasks now keep their docstring (Issue #1636)

.. _version-3.1.2:

3.1.2
=====
:release-date: 2013-11-12 08:00 p.m. UTC
:release-by: Ask Solem

- Now depends on :mod:`billiard` 3.3.0.6

- No longer needs the billiard C extension to be installed.

- The worker silently ignored task errors.

- Django: Fixed ``ImproperlyConfigured`` error raised
  when no database backend specified.

    Fix contributed by :github_user:`j0hnsmith`.

- Prefork pool: Now using ``_multiprocessing.read`` with ``memoryview``
  if available.

- ``close_open_fds`` now uses ``os.closerange`` if available.

- ``get_fdmax`` now takes value from ``sysconfig`` if possible.

.. _version-3.1.1:

3.1.1
=====
:release-date: 2013-11-11 06:30 p.m. UTC
:release-by: Ask Solem

- Now depends on :mod:`billiard` 3.3.0.4.

- Python 3: Fixed compatibility issues.

- Windows:  Accidentally showed warning that the billiard C extension
  wasn't installed (Issue #1630).

- Django: Tutorial updated with a solution that sets a default
  :envvar:`DJANGO_SETTINGS_MODULE` so that it doesn't have to be typed
  in with the :program:`celery` command.

    Also fixed typos in the tutorial, and added the settings
    required to use the Django database backend.

    Thanks to Chris Ward, :github_user:`orarbel`.

- Django: Fixed a problem when using the Django settings in Django 1.6.

- Django: Fix-up shouldn't be applied if the django loader is active.

- Worker:  Fixed attribute error for ``human_write_stats`` when using the
  compatibility prefork pool implementation.

- Worker: Fixed compatibility with billiard without C extension.

- Inspect.conf: Now supports a ``with_defaults`` argument.

- Group.restore: The backend argument wasn't respected.

.. _version-3.1.0:

3.1.0
=======
:release-date: 2013-11-09 11:00 p.m. UTC
:release-by: Ask Solem

See :ref:`whatsnew-3.1`.
