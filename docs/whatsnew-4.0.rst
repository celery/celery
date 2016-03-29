.. _whatsnew-4.0:

===========================================
 What's new in Celery 4.0 (0Today8)
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

This version is officially supported on CPython 2.7, 3.4 and 3.5.
and also supported on PyPy.

.. _`website`: http://celeryproject.org/

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 2

Preface
=======


.. _v400-important:

Important Notes
===============

Dropped support for Python 2.6
------------------------------

Celery now requires Python 2.7 or later,
and also drops support for Python 3.3 so supported versions are:

- CPython 2.7
- CPython 3.4
- CPython 3.5
- PyPy 4.0 (pypy2)
- PyPy 2.4 (pypy3)
- Jython 2.7.0

Lowercase setting names
-----------------------

In the pursuit of beauty all settings have been renamed to be in all
lowercase, and some setting names have been renamed for naming consistency.

This change is fully backwards compatible so you can still use the uppercase
setting names, but we would like you to upgrade as soon as possible and
you can even do so automatically using the :program:`celery upgrade settings`
command:

.. code-block:: console

    $ celery upgrade settings proj/settings.py

This command will modify your module in-place to use the new lower-case
names (if you want uppercase with a celery prefix see block below),
and save a backup in :file:`proj/settings.py.orig`.

.. admonition:: For Django users and others who want to keep uppercase names

    If you're loading Celery configuration from the Django settings module
    then you will want to keep using the uppercase names.

    You will also want to use a ``CELERY_`` prefix so that no Celery settings
    collide with Django settings used by other apps.

    To do this, you will first need to convert your settings file
    to use the new consistent naming scheme, and add the prefix to all
    Celery related settings:

    .. code-block:: console

        $ celery upgrade settings --django proj/settings.py

    After upgrading the settings file, you need to set the prefix explicitly
    in your ``proj/celery.py`` module:

    .. code-block:: python

        app.config_from_object('django.conf:settings', namespace='CELERY')

    You can find the most up to date Django celery integration example
    here: :ref:`django-first-steps`.

    Note that this will also add a prefix to settings that didn't previously
    have one, like ``BROKER_URL``.

    Luckily you don't have to manually change the files, as
    the :program:`celery upgrade settings --django` program should do the
    right thing.

The loader will try to detect if your configuration is using the new format,
and act accordingly, but this also means that you are not allowed to mix and
match new and old setting names, that is unless you provide a value for both
alternatives.

The major difference between previous versions, apart from the lower case
names, are the renaming of some prefixes, like ``celerybeat_`` to ``beat_``,
``celeryd_`` to ``worker_``.

The ``celery_`` prefix has also been removed, and task related settings
from this namespace is now prefixed by ``task_``, worker related settings
with ``worker_``.

Apart from this most of the settings will be the same in lowercase, apart from
a few special ones:

=====================================  ==========================================================
**Setting name**                       **Replace with**
=====================================  ==========================================================
``CELERY_MAX_CACHED_RESULTS``          :setting:`result_cache_max`
``CELERY_MESSAGE_COMPRESSION``         :setting:`result_compression`/:setting:`task_compression`.
``CELERY_TASK_RESULT_EXPIRES``         :setting:`result_expires`
``CELERY_RESULT_DBURI``                :setting:`sqlalchemy_dburi`
``CELERY_RESULT_ENGINE_OPTIONS``       :setting:`sqlalchemy_engine_options`
``-*-_DB_SHORT_LIVED_SESSIONS``        :setting:`sqlalchemy_short_lived_sessions`
``CELERY_RESULT_DB_TABLE_NAMES``       :setting:`sqlalchemy_db_names`
``CELERY_ACKS_LATE``                   :setting:`task_acks_late`
``CELERY_ALWAYS_EAGER``                :setting:`task_always_eager`
``CELERY_ANNOTATIONS``                 :setting:`task_annotations`
``CELERY_MESSAGE_COMPRESSION``         :setting:`task_compression`
``CELERY_CREATE_MISSING_QUEUES``       :setting:`task_create_missing_queues`
``CELERY_DEFAULT_DELIVERY_MODE``       :setting:`task_default_delivery_mode`
``CELERY_DEFAULT_EXCHANGE``            :setting:`task_default_exchange`
``CELERY_DEFAULT_EXCHANGE_TYPE``       :setting:`task_default_exchange_type`
``CELERY_DEFAULT_QUEUE``               :setting:`task_default_queue`
``CELERY_DEFAULT_RATE_LIMIT``          :setting:`task_default_rate_limit`
``CELERY_DEFAULT_ROUTING_KEY``         :setting:`task_default_routing_key`
``-"-_EAGER_PROPAGATES_EXCEPTIONS``    :setting:`task_eager_propagates`
``CELERY_IGNORE_RESULT``               :setting:`task_ignore_result`
``CELERY_TASK_PUBLISH_RETRY``          :setting:`task_publish_retry`
``CELERY_TASK_PUBLISH_RETRY_POLICY``   :setting:`task_publish_retry_policy`
``CELERY_QUEUES``                      :setting:`task_queues`
``CELERY_ROUTES``                      :setting:`task_routes`
``CELERY_SEND_TASK_ERROR_EMAILS``      :setting:`task_send_error_emails`
``CELERY_SEND_TASK_SENT_EVENT``        :setting:`task_send_sent_event`
``CELERY_TASK_SERIALIZER``             :setting:`task_serializer`
``CELERYD_TASK_SOFT_TIME_LIMIT``       :setting:`task_soft_time_limit`
``CELERYD_TASK_TIME_LIMIT``            :setting:`task_time_limit`
``CELERY_TRACK_STARTED``               :setting:`task_track_started`
``CELERY_DISABLE_RATE_LIMITS``         :setting:`worker_disable_rate_limits`
``CELERY_ENABLE_REMOTE_CONTROL``       :setting:`worker_enable_remote_control`
``CELERYD_SEND_EVENTS``                :setting:`worker_send_task_events`
=====================================  ==========================================================

You can see a full table of the changes in :ref:`conf-old-settings-map`.

JSON is now the default serializer
----------------------------------

The time has finally come to end the reign of :mod:`pickle` as the default
serialization mechanism, and json is the default serializer starting from this
version.

This change was :ref:`announced with the release of Celery 3.1
<last-version-to-enable-pickle>`.

If you're still depending on :mod:`pickle` being the default serializer,
then you have to configure your app before upgrading to 4.0:

.. code-block:: python

    task_serializer = 'pickle'
    result_serializer = 'pickle'
    accept_content = {'pickle'}

The Task base class no longer automatically register tasks
----------------------------------------------------------

The :class:`~@Task` class is no longer using a special metaclass
that automatically registers the task in the task registry.

Instead this is now handled by the :class:`@task` decorators.

If you're still using class based tasks, then you need to register
these manually:

.. code-block:: python

    class CustomTask(Task):
        def run(self):
            print('running')
    app.tasks.register(CustomTask())

The best practice is to use custom task classes only for overriding
general behavior, and then using the task decorator to realize the task:

.. code-block:: python

    @app.task(bind=True, base=CustomTask)
    def custom(self):
        print('running')

This change also means the ``abstract`` attribute of the task
no longer has any effect.

Task argument checking
----------------------

The arguments of the task is now verified when calling the task,
even asynchronously:

.. code-block:: pycon

    >>> @app.task
    ... def add(x, y):
    ...     return x + y

    >>> add.delay(8, 8)
    <AsyncResult: f59d71ca-1549-43e0-be41-4e8821a83c0c>

    >>> add.delay(8)
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "celery/app/task.py", line 376, in delay
        return self.apply_async(args, kwargs)
      File "celery/app/task.py", line 485, in apply_async
        check_arguments(*(args or ()), **(kwargs or {}))
    TypeError: add() takes exactly 2 arguments (1 given)

Redis Events not backward compatible
------------------------------------

The Redis ``fanout_patterns`` and ``fanout_prefix`` transport
options are now enabled by default, which means that workers
running 4.0 cannot see workers running 3.1 on the default configuration,
and vice versa.

This is only related to monitor event messages, the workers should still
execute tasks as normally.

You can avoid this situation by configuring the 3.1 workers (and clients)
to enable these settings, before upgrading to 4.0:

.. code-block:: python

    BROKER_TRANSPORT_OPTIONS = {
        'fanout_patterns': True,
        'fanout_prefix': True,
    }

Django: Autodiscover now supports Django app configs
----------------------------------------------------

The :meth:`@autodiscover` function can now be called without arguments,
and the Django handler will automatically find your installed apps:

.. code-block:: python

    app.autodiscover()

The Django integration :ref:`example in the documentation
<django-first-steps>` has been updated to use the argument-less call.


Old command-line programs removed
---------------------------------

Installing Celery will no longer install the ``celeryd``,
``celerybeat`` and ``celeryd-multi`` programs.

This was announced with the release of Celery 3.1, but you may still
have scripts pointing to the old names so make sure you update these
to use the new umbrella command:

+-------------------+--------------+-------------------------------------+
| Program           | New Status   | Replacement                         |
+===================+==============+=====================================+
| ``celeryd``       | **REMOVED**  | :program:`celery worker`            |
+-------------------+--------------+-------------------------------------+
| ``celerybeat``    | **REMOVED**  | :program:`celery beat`              |
+-------------------+--------------+-------------------------------------+
| ``celeryd-multi`` | **REMOVED**  | :program:`celery multi`             |
+-------------------+--------------+-------------------------------------+

.. _v400-news:

News
====

New Task Message Protocol
=========================
# e71652d384b1b5df2a4e6145df9f0efb456bc71c

This version introduces a brand new task message protocol,
the first major change to the protocol since the beginning of the project.

The new protocol is backwards incompatible, so you need to set
the :setting:`task_protocol` configuration option to ``2`` to take advantage:

.. code-block:: python

    app = Celery()
    app.conf.task_protocol = 2

Using the new protocol is recommended for everybody who don't
need backwards compatibility.

Once enabled task messages sent is unreadable to older versions of Celery.

New protocol highlights
-----------------------

The new protocol fixes many problems with the old one, and enables
some long-requested features:

- Most of the data are now sent as message headers, instead of being
  serialized with the message body.

    In version 1 of the protocol the worker always had to deserialize
    the message to be able to read task metadata like the task id,
    name, etc.  This also meant that the worker was forced to double-decode
    the data, first deserializing the message on receipt, serializing
    the message again to send to child process, then finally the child process
    deserializes the message again.

    Keeping the metadata fields in the message headers means the worker
    does not actually have to decode the payload before delivering
    the task to the child process, and also that it's now possible
    for the worker to reroute a task written in a language different
    from Python to a different worker.

- A new ``lang`` message header can be used to specify the programming
  language the task is written in.

- Worker stores results for internal errors like ``ContentDisallowed``,
  and other deserialization errors.

- Worker stores results and sends monitoring events for unregistered
  task errors.

- Worker calls callbacks/errbacks even when the result is sent by the
  parent process (e.g. :exc:`WorkerLostError` when a child process
  terminates, deserialization errors, unregistered tasks).

- A new ``origin`` header contains information about the process sending
  the task (worker nodename, or pid and hostname information).

- A new ``shadow`` header allows you to modify the task name used in logs.

    This is useful for dispatch like patterns, like a task that calls
    any function using pickle (don't do this at home):

    .. code-block:: python

        from celery import Task
        from celery.utils.imports import qualname

        class call_as_task(Task):

            def shadow_name(self, args, kwargs, options):
                return 'call_as_task:{0}'.format(qualname(args[0]))

            def run(self, fun, *args, **kwargs):
                return fun(*args, **kwargs)
        call_as_task = app.tasks.register(call_as_task())

- New ``argsrepr`` and ``kwargsrepr`` fields contain textual representations
  of the task arguments (possibly truncated) for use in logs, monitors, etc.

    This means the worker does not have to deserialize the message payload
    to display the task arguments for informational purposes.

- Chains now use a dedicated ``chain`` field enabling support for chains
  of thousands and more tasks.

- New ``parent_id`` and ``root_id`` headers adds information about
  a tasks relationship with other tasks.

    - ``parent_id`` is the task id of the task that called this task
    - ``root_id`` is the first task in the workflow.

    These fields can be used to improve monitors like flower to group
    related messages together (like chains, groups, chords, complete
    workflows, etc).

- ``app.TaskProducer`` replaced by :meth:`@amqp.create_task_message`` and
  :meth:`@amqp.send_task_message``.

    Dividing the responsibilities into creating and sending means that
    people who want to send messages using a Python amqp client directly,
    does not have to implement the protocol.

    The :meth:`@amqp.create_task_message` method calls either
    :meth:`@amqp.as_task_v2`, or :meth:`@amqp.as_task_v1` depending
    on the configured task protocol, and returns a special
    :class:`~celery.app.amqp.task_message` tuple containing the
    headers, properties and body of the task message.

.. seealso::

    The new task protocol is documented in full here:
    :ref:`message-protocol-task-v2`.


Prefork: Tasks now log from the child process
=============================================

Logging of task success/failure now happens from the child process
actually executing the task, which means that logging utilities
like Sentry can get full information about tasks that fail, including
variables in the traceback.

Prefork: One logfile per child process
======================================

Init scrips and :program:`celery multi` now uses the `%I` log file format
option (e.g. :file:`/var/log/celery/%n%I.log`).

This change was necessary to ensure each child
process has a separate log file after moving task logging
to the child process, as multiple processes writing to the same
log file can cause corruption.

You are encouraged to upgrade your init scripts and multi arguments
to use this new option.

Ability to configure separate broker urls for read/write
========================================================

New :setting:`broker_read_url` and :setting:`broker_write_url` settings
have been added so that separate broker urls can be provided
for connections used for consuming/publishing.

In addition to the configuration options, two new methods have been
added the app API:

    - ``app.connection_for_read()``
    - ``app.connection_for_write()``

These should now be used in place of ``app.connection()`` to specify
the intent of the required connection.

.. note::

    Two connection pools are available: ``app.pool`` (read), and
    ``app.producer_pool`` (write).  The latter does not actually give connections
    but full :class:`kombu.Producer` instances.

    .. code-block:: python

        def publish_some_message(app, producer=None):
            with app.producer_or_acquire(producer) as producer:
                ...

        def consume_messages(app, connection=None):
            with app.connection_or_acquire(connection) as connection:
                ...

Canvas Refactor
===============

# BLALBLABLA
d79dcd8e82c5e41f39abd07ffed81ca58052bcd2
1e9dd26592eb2b93f1cb16deb771cfc65ab79612
e442df61b2ff1fe855881c1e2ff9acc970090f54
0673da5c09ac22bdd49ba811c470b73a036ee776

- Now unrolls groups within groups into a single group (Issue #1509).
- chunks/map/starmap tasks now routes based on the target task
- chords and chains can now be immutable.
- Fixed bug where serialized signature were not converted back into
  signatures (Issue #2078)

    Fix contributed by Ross Deane.

- Fixed problem where chains and groups did not work when using JSON
  serialization (Issue #2076).

    Fix contributed by Ross Deane.

- Creating a chord no longer results in multiple values for keyword
  argument 'task_id' (Issue #2225).

    Fix contributed by Aneil Mallavarapu

- Fixed issue where the wrong result is returned when a chain
  contains a chord as the penultimate task.

    Fix contributed by Aneil Mallavarapu

- Special case of ``group(A.s() | group(B.s() | C.s()))`` now works.

- Chain: Fixed bug with incorrect id set when a subtask is also a chain.

- ``group | group`` is now flattened into a single group (Issue #2573).

- Fixed issue where ``group | task`` was not upgrading correctly
  to chord (Issue #2922).

Schedule tasks based on sunrise, sunset, dawn and dusk.
=======================================================

See :ref:`beat-solar` for more information.

Contributed by Mark Parncutt.

App can now configure periodic tasks
====================================

# bc18d0859c1570f5eb59f5a969d1d32c63af764b
# 132d8d94d38f4050db876f56a841d5a5e487b25b

RabbitMQ Priority queue support
===============================

# 1d4cbbcc921aa34975bde4b503b8df9c2f1816e0

Contributed by Gerald Manipon.

Incompatible: Worker direct queues are no longer using auto-delete.
===================================================================

Issue #2492.

Prefork: Limits for child process resident memory size.
=======================================================

This version introduces the new :setting:`worker_max_memory_per_child` setting,
which BLA BLA BLA

# 5cae0e754128750a893524dcba4ae030c414de33

Contributed by Dave Smith.

Redis: Result backend optimizations
===============================================

Pub/sub results
---------------

Contributed by Yaroslav Zhavoronkov and Ask Solem.

Chord join
----------

This was an experimental feature introduced in Celery 3.1,
but is now enabled by default.

?new_join BLABLABLA

Riak Result Backend
===================

Contributed by Gilles Dartiguelongue, Alman One and NoKriK.

Bla bla

- blah blah

CouchDB Result Backend
======================

Contributed by Nathan Van Gheem

New Cassandra Backend
=====================

The new Cassandra backend utilizes the python-driver library.
Old backend is deprecated and everyone using cassandra is required to upgrade
to be using the new driver.

# XXX What changed?


Elasticsearch Result Backend
============================

Contributed by Ahmet Demir.

Filesystem Result Backend
=========================

Contributed by Môshe van der Sterre.

Event Batching
==============

Events are now buffered in the worker and sent as a list, and
events are sent as transient messages by default so that they are not written
to disk by RabbitMQ.

03399b4d7c26fb593e61acf34f111b66b340ba4e


Task.replace
============

Task.replace changed, removes Task.replace_in_chord.

The two methods had almost the same functionality, but the old Task.replace
would force the new task to inherit the callbacks/errbacks of the existing
task.

If you replace a node in a tree, then you would not expect the new node to
inherit the children of the old node, so this seems like unexpected
behavior.

So self.replace(sig) now works for any task, in addition sig can now
be a group.

Groups are automatically converted to a chord, where the callback
will "accumulate" the results of the group tasks.

A new builtin task (`celery.accumulate` was added for this purpose)

Closes #817


Optimized Beat implementation
=============================

heapq
20340d79b55137643d5ac0df063614075385daaa

Contributed by Ask Solem and Alexander Koshelev.


Task Autoretry Decorator
========================

75246714dd11e6c463b9dc67f4311690643bff24

Contributed by Dmitry Malinovsky.


Async Result API
================

eventlet/gevent drainers, promises, BLA BLA

Closed issue #2529.


:setting:`task_routes` can now contain glob patterns and regexes.
=================================================================

See examples in :setting:`task_routes` and :ref:`routing-automatic`.

In Other News
-------------

- **Requirements**:

    - Now depends on :ref:`Kombu 4.0 <kombu:version-4.0>`.

    - Now depends on :pypi:`billiard` version 3.5.

    - No longer depends on :pypi:`anyjson` :sadface:


- **Tasks**: The "anon-exchange" is now used for simple name-name direct routing.

  This increases performance as it completely bypasses the routing table,
  in addition it also improves reliability for the Redis broker transport.

- **Eventlet/Gevent**: Fixed race condition leading to "simultaneous read"
  errors (Issue #2812).

- **Programs**: ``%n`` format for :program:`celery multi` is now synonym with
  ``%N`` to be consistent with :program:`celery worker`.

- **Programs**: celery inspect/control now supports ``--json`` argument to
  give output in json format.

- **Programs**: :program:`celery inspect registered` now ignores built-in
  tasks.

- **Programs**: New :program:`celery logtool`: Utility for filtering and parsing
  celery worker logfiles

- **Redis Transport**: The Redis transport now supports the
  :setting:`broker_use_ssl` option.

- **Worker**: Worker now only starts the remote control command consumer if the
  broker transport used actually supports them.

- **Worker**: Gossip now sets ``x-message-ttl`` for event queue to heartbeat_interval s.
  (Issue #2005).

- **Worker**: Now preserves exit code (Issue #2024).

- **Worker**: Loglevel for unrecoverable errors changed from ``error`` to
  ``critical``.

- **Worker**: Improved rate limiting accuracy.

- **Worker**: Account for missing timezone information in task expires field.

    Fix contributed by Albert Wang.

- **Worker**: The worker no longer has a ``Queues`` bootsteps, as it is now
    superfluous.

- **Tasks**: New :setting:`task_reject_on_worker_lost` setting, and
  :attr:`~@Task.reject_on_worker_lost` task attribute decides what happens
  when the child worker process executing a late ack task is terminated.

    Contributed by Michael Permana.

- **Worker**: Improvements and fixes for LimitedSet

    Getting rid of leaking memory + adding minlen size of the set
    minlen is minimal residual size of set after operating for long.
    Minlen items are kept, even if they should be expired by time, until
    we get newer items.

    Problems with older and even more old code:

    1)
       Heap would tend to grow in some scenarios
       (like adding an item multiple times).

    2) Adding many items fast would not clean them soon enough (if ever).

    3) When talking to other workers, revoked._data was sent, but
       it was processed on the other side as iterable.
       That means giving those keys new (current)
       timestamp. By doing this workers could recycle
       items forever. Combined with 1) and 2), this means that in
       large set of workers, you are getting out of memory soon.

    All those problems should be fixed now,
    also some new unittests are added.

    This should fix issues #3095, #3086.

    Contributed by David Pravec.

- **App**: New signals for app configuration/finalization:

    - :data:`app.on_configure <@on_configure>`
    - :data:`app.on_after_configure <@on_after_configure>`
    - :data:`app.on_after_finalize <@on_after_finalize>`

- **Task**: New task signals for rejected task messages:

    - :data:`celery.signals.task_rejected`.
    - :data:`celery.signals.task_unknown`.

- **Events**: Event messages now uses the RabbitMQ ``x-message-ttl`` option
    to ensure older event messages are discarded.

    The default is 5 seconds, but can be changed using the
    :setting:`event_queue_ttl` setting.

- **Events**: Event monitors now sets the :setting:`event_queue_expires`
  setting by default.

    The queues will now expire after 60 seconds after the monitor stops
    consuming from it.

- **Canvas**: ``chunks``/``map``/``starmap`` are now routed based on the target task.

- **Canvas**: ``Signature.link`` now works when argument is scalar (not a list)
    (Issue #2019).

- **App**: The application can now change how task names are generated using
    the :meth:`~@gen_task_name` method.

    Contributed by Dmitry Malinovsky.

- **App**: App has new ``app.current_worker_task`` property that
  returns the task that is currently being worked on (or :const:`None`).
  (Issue #2100).

- **Tasks**: ``Task.subtask`` renamed to ``Task.signature`` with alias.

- **Tasks**: ``Task.subtask_from_request`` renamed to
  ``Task.signature_from_request`` with alias.

- **Tasks**: The ``delivery_mode`` attribute for :class:`kombu.Queue` is now
  respected (Issue #1953).

- **Tasks**: Routes in :setting:`task-routes` can now specify a
  :class:`~kombu.Queue` instance directly.

    Example:

    .. code-block:: python

        task_routes = {'proj.tasks.add': {'queue': Queue('add')}}

- **Tasks**: ``AsyncResult`` now raises :exc:`ValueError` if task_id is None.
  (Issue #1996).

- **Tasks**: ``result.get()`` now supports an ``on_message`` argument to set a
  callback to be called for every message received.

- **Tasks**: New abstract classes added:

    - :class:`~celery.utils.abstract.CallableTask`

        Looks like a task.

    - :class:`~celery.utils.abstract.CallableSignature`

        Looks like a task signature.

- **Programs**: :program:`celery multi` now passes through `%i` and `%I` log
  file formats.

- **Programs**: ``%p`` can now be used to expand to the full worker nodename
  in logfile/pidfile arguments.

- **Programs**: A new command line option :option:``--executable`` is now
  available for daemonizing programs.

    Contributed by Bert Vanderbauwhede.

- **Programs**: :program:`celery worker` supports new
  :option:`--prefetch-multiplier` option.

    Contributed by Mickaël Penhard.

- **Deployment**: Generic init scripts now support
  :envvar:`CELERY_SU`` and :envvar:`CELERYD_SU_ARGS` environment variables
  to set the path and arguments for :command:`su` (:manpage:`su(1)`).

- **Prefork**: Prefork pool now uses ``poll`` instead of ``select`` where
  available (Issue #2373).

- **Eventlet**: Now returns pool size in :program:`celery inspect stats`
  command.

    Contributed by Alexander Oblovatniy.

- **Tasks**: New :setting:`email_charset` setting allows for changing
  the charset used for outgoing error emails.

    Contributed by Vladimir Gorbunov.

- **Worker**: Now respects :setting:`broker_connection_retry` setting.

    Fix contributed by Nat Williams.

- **Worker**: Autoscale did not always update keepalive when scaling down.

    Fix contributed by Philip Garnero.

- **General**: Dates are now always timezone aware even if
  :setting:`enable_utc` is disabled (Issue #943).

    Fix contributed by Omer Katz.

- **Result Backends**: The redis result backend now has a default socket
   timeout of 5 seconds.

    The default can be changed using the new :setting:`redis_socket_timeout`
    setting.

    Contributed by Raghuram Srinivasan.

- **Result Backends**: RPC Backend result queues are now auto delete by
  default (Issue #2001).

- **Result Backends**: MongoDB now supports setting the
  :setting:`result_serialzier` setting to ``bson`` to use the MongoDB
  libraries own serializer.

    Contributed by Davide Quarta.

- **Result Backends**: SQLAlchemy result backend now ignores all result
   engine options when using NullPool (Issue #1930).

- **Result Backends**: MongoDB URI handling has been improved to use
    database name, user and password from the URI if provided.

    Contributed by Samuel Jaillet.

- **Result Backends**: Fix problem with rpc/amqp backends where exception
    was not deserialized properly with the json serializer (Issue #2518).

    Fix contributed by Allard Hoeve.

- **Result Backends**: Database backend now sets max char size to 155 to deal
  with brain damaged MySQL unicode implementation (Issue #1748).

- **General**: All Celery exceptions/warnings now inherit from common
  :class:`~celery.exceptions.CeleryException`/:class:`~celery.exceptions.CeleryWarning`.
  (Issue #2643).

- **Tasks**: Task retry now also throws in eager mode.

    Fix contributed by Feanil Patel.

- **Tasks**: Task error email charset now set to ``utf-8`` by default
  (Issue #2737).

- Apps can now define how tasks are named (:meth:`@gen_task_name`).

    Contributed by Dmitry Malinovsky

- Module ``celery.worker.job`` renamed to :mod:`celery.worker.request`.

- Beat: ``Scheduler.Publisher``/``.publisher`` renamed to
  ``.Producer``/``.producer``.

Incompatible changes
====================

- Prefork: Calling ``result.get()`` or joining any result from within a task
  now raises :exc:`RuntimeError`.

    In previous versions this would emit a warning.

- :mod:`celery.worker.consumer` is now a package, not a module.

- Result: The task_name argument/attribute of :class:`@AsyncResult` was
  removed.

    This was historically a field used for :mod:`pickle` compatibility,
    but is no longer needed.

- Backends: Arguments named ``status`` renamed to ``state``.

- Backends: ``backend.get_status()`` renamed to ``backend.get_state()``.

Unscheduled Removals
====================

- The experimental :mod:`celery.contrib.methods` feature has been removed,
  as there were far many bugs in the implementation to be useful.

- The CentOS init scripts have been removed.

    These did not really add any features over the generic init scripts,
    so you are encouraged to use them instead, or something like
    ``supervisord``.


.. _v400-removals:

Scheduled Removals
==================

Modules
-------

- Module ``celery.worker.job`` has been renamed to :mod:`celery.worker.request`.

    This was an internal module so should not have any effect.
    It is now part of the public API so should not change again.

- Module ``celery.task.trace`` has been renamed to ``celery.app.trace``
  as the ``celery.task`` package is being phased out.  The compat module
  will be removed in version 4.0 so please change any import from::

    from celery.task.trace import …

  to::

    from celery.app.trace import …

- Old compatibility aliases in the :mod:`celery.loaders` module
  has been removed.

    - Removed ``celery.loaders.current_loader()``, use: ``current_app.loader``

    - Removed ``celery.loaders.load_settings()``, use: ``current_app.conf``

Result
------

- ``AsyncResult.serializable()`` and ``celery.result.from_serializable``
    has been removed:

    Use instead:

    .. code-block:: pycon

        >>> tup = result.as_tuple()
        >>> from celery.result import result_from_tuple
        >>> result = result_from_tuple(tup)

- Removed ``BaseAsyncResult``, use ``AsyncResult`` for instance checks
  instead.

- Removed ``TaskSetResult``, use ``GroupResult`` instead.

    - ``TaskSetResult.total`` -> ``len(GroupResult)``

    - ``TaskSetResult.taskset_id`` -> ``GroupResult.id``

- Removed ``ResultSet.subtasks``, use ``ResultSet.results`` instead.


TaskSet
-------

TaskSet has been renamed to group and TaskSet will be removed in version 4.0.

Old::

    >>> from celery.task import TaskSet

    >>> TaskSet(add.subtask((i, i)) for i in xrange(10)).apply_async()

New::

    >>> from celery import group
    >>> group(add.s(i, i) for i in xrange(10))()

Events
------

- Removals for class :class:`celery.events.state.Worker`:

    - ``Worker._defaults`` attribute.

        Use ``{k: getattr(worker, k) for k in worker._fields}``.

    - ``Worker.update_heartbeat``

        Use ``Worker.event(None, timestamp, received)``

    - ``Worker.on_online``

        Use ``Worker.event('online', timestamp, received, fields)``

    - ``Worker.on_offline``

        Use ``Worker.event('offline', timestamp, received, fields)``

    - ``Worker.on_heartbeat``

        Use ``Worker.event('heartbeat', timestamp, received, fields)``



- Removals for class :class:`celery.events.state.Task`:

    - ``Task._defaults`` attribute.

        Use ``{k: getattr(task, k) for k in task._fields}``.

    - ``Task.on_sent``

        Use ``Worker.event('sent', timestamp, received, fields)``

    - ``Task.on_received``

        Use ``Task.event('received', timestamp, received, fields)``

    - ``Task.on_started``

        Use ``Task.event('started', timestamp, received, fields)``

    - ``Task.on_failed``

        Use ``Task.event('failed', timestamp, received, fields)``

    - ``Task.on_retried``

        Use ``Task.event('retried', timestamp, received, fields)``

    - ``Task.on_succeeded``

        Use ``Task.event('succeeded', timestamp, received, fields)``

    - ``Task.on_revoked``

        Use ``Task.event('revoked', timestamp, received, fields)``

    - ``Task.on_unknown_event``

        Use ``Task.event(short_type, timestamp, received, fields)``

    - ``Task.update``

        Use ``Task.event(short_type, timestamp, received, fields)``

    - ``Task.merge``

        Contact us if you need this.

Magic keyword arguments
-----------------------

Support for the very old magic keyword arguments accepted by tasks has finally
been in 4.0.

If you are still using these you have to rewrite any task still
using the old ``celery.decorators`` module and depending
on keyword arguments being passed to the task,
for example::

    from celery.decorators import task

    @task()
    def add(x, y, task_id=None):
        print('My task id is %r' % (task_id,))

should be rewritten into::

    from celery import task

    @task(bind=True)
    def add(self, x, y):
        print('My task id is {0.request.id}'.format(self))

Settings
--------

The following settings have been removed, and is no longer supported:

Logging Settings
~~~~~~~~~~~~~~~~

=====================================  =====================================
**Setting name**                       **Replace with**
=====================================  =====================================
``CELERYD_LOG_LEVEL``                  :option:`--loglevel`
``CELERYD_LOG_FILE``                   :option:`--logfile``
``CELERYBEAT_LOG_LEVEL``               :option:`--loglevel`
``CELERYBEAT_LOG_FILE``                :option:`--loglevel``
``CELERYMON_LOG_LEVEL``                celerymon is deprecated, use flower.
``CELERYMON_LOG_FILE``                 celerymon is deprecated, use flower.
``CELERYMON_LOG_FORMAT``               celerymon is deprecated, use flower.
=====================================  =====================================

Task Settings
~~~~~~~~~~~~~~

=====================================  =====================================
**Setting name**                       **Replace with**
=====================================  =====================================
``CELERY_CHORD_PROPAGATES``            N/a
=====================================  =====================================

.. _v400-deprecations:

Deprecations
============

See the :ref:`deprecation-timeline`.

.. _v400-fixes:

Fixes
=====

