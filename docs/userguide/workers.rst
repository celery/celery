.. _guide-workers:

===============
 Workers Guide
===============

.. contents::
    :local:
    :depth: 1

.. _worker-starting:

Starting the worker
===================

.. sidebar:: Daemonizing

    You probably want to use a daemonization tool to start
    in the background.  See :ref:`daemonizing` for help
    detaching the worker using popular daemonization tools.

You can start the worker in the foreground by executing the command:

.. code-block:: bash

    $ celery --app=app worker -l info

For a full list of available command-line options see
:mod:`~celery.bin.worker`, or simply do:

.. code-block:: bash

    $ celery worker --help

You can also start multiple workers on the same machine. If you do so
be sure to give a unique name to each individual worker by specifying a
host name with the :option:`--hostname|-n` argument:

.. code-block:: bash

    $ celery worker --loglevel=INFO --concurrency=10 -n worker1.%h
    $ celery worker --loglevel=INFO --concurrency=10 -n worker2.%h
    $ celery worker --loglevel=INFO --concurrency=10 -n worker3.%h

The hostname argument can expand the following variables:

    - ``%h``:  Hostname including domain name.
    - ``%n``:  Hostname only.
    - ``%d``:  Domain name only.

E.g. if the current hostname is ``george.example.com`` then
these will expand to:

    - ``worker1.%h`` -> ``worker1.george.example.com``
    - ``worker1.%n`` -> ``worker1.george``
    - ``worker1.%d`` -> ``worker1.example.com``

.. _worker-stopping:

Stopping the worker
===================

Shutdown should be accomplished using the :sig:`TERM` signal.

When shutdown is initiated the worker will finish all currently executing
tasks before it actually terminates, so if these tasks are important you should
wait for it to finish before doing anything drastic (like sending the :sig:`KILL`
signal).

If the worker won't shutdown after considerate time, for example because
of tasks stuck in an infinite-loop, you can use the :sig:`KILL` signal to
force terminate the worker, but be aware that currently executing tasks will
be lost (unless the tasks have the :attr:`~@Task.acks_late`
option set).

Also as processes can't override the :sig:`KILL` signal, the worker will
not be able to reap its children, so make sure to do so manually.  This
command usually does the trick:

.. code-block:: bash

    $ ps auxww | grep 'celery worker' | awk '{print $2}' | xargs kill -9

.. _worker-restarting:

Restarting the worker
=====================

Other than stopping then starting the worker to restart, you can also
restart the worker using the :sig:`HUP` signal:

.. code-block:: bash

    $ kill -HUP $pid

The worker will then replace itself with a new instance using the same
arguments as it was started with.

.. note::

    Restarting by :sig:`HUP` only works if the worker is running
    in the background as a daemon (it does not have a controlling
    terminal).

    :sig:`HUP` is disabled on OS X because of a limitation on
    that platform.


.. _worker-process-signals:

Process Signals
===============

The worker's main process overrides the following signals:

+--------------+-------------------------------------------------+
| :sig:`TERM`  | Warm shutdown, wait for tasks to complete.      |
+--------------+-------------------------------------------------+
| :sig:`QUIT`  | Cold shutdown, terminate ASAP                   |
+--------------+-------------------------------------------------+
| :sig:`USR1`  | Dump traceback for all active threads.          |
+--------------+-------------------------------------------------+
| :sig:`USR2`  | Remote debug, see :mod:`celery.contrib.rdb`.    |
+--------------+-------------------------------------------------+

.. _worker-concurrency:

Concurrency
===========

By default multiprocessing is used to perform concurrent execution of tasks,
but you can also use :ref:`Eventlet <concurrency-eventlet>`.  The number
of worker processes/threads can be changed using the :option:`--concurrency`
argument and defaults to the number of CPUs available on the machine.

.. admonition:: Number of processes (multiprocessing/prefork pool)

    More pool processes are usually better, but there's a cut-off point where
    adding more pool processes affects performance in negative ways.
    There is even some evidence to support that having multiple worker
    instances running, may perform better than having a single worker.
    For example 3 workers with 10 pool processes each.  You need to experiment
    to find the numbers that works best for you, as this varies based on
    application, work load, task run times and other factors.

.. _worker-remote-control:

Remote control
==============

.. versionadded:: 2.0

.. sidebar:: The ``celery`` command

    The :program:`celery` program is used to execute remote control
    commands from the command-line.  It supports all of the commands
    listed below.  See :ref:`monitoring-control` for more information.

pool support: *prefork, eventlet, gevent*, blocking:*threads/solo* (see note)
broker support: *amqp, redis*

Workers have the ability to be remote controlled using a high-priority
broadcast message queue.  The commands can be directed to all, or a specific
list of workers.

Commands can also have replies.  The client can then wait for and collect
those replies.  Since there's no central authority to know how many
workers are available in the cluster, there is also no way to estimate
how many workers may send a reply, so the client has a configurable
timeout — the deadline in seconds for replies to arrive in.  This timeout
defaults to one second.  If the worker doesn't reply within the deadline
it doesn't necessarily mean the worker didn't reply, or worse is dead, but
may simply be caused by network latency or the worker being slow at processing
commands, so adjust the timeout accordingly.

In addition to timeouts, the client can specify the maximum number
of replies to wait for.  If a destination is specified, this limit is set
to the number of destination hosts.

.. note::

    The solo and threads pool supports remote control commands,
    but any task executing will block any waiting control command,
    so it is of limited use if the worker is very busy.  In that
    case you must increase the timeout waiting for replies in the client.

.. _worker-broadcast-fun:

The :meth:`~@control.broadcast` function.
----------------------------------------------------

This is the client function used to send commands to the workers.
Some remote control commands also have higher-level interfaces using
:meth:`~@control.broadcast` in the background, like
:meth:`~@control.rate_limit` and :meth:`~@control.ping`.

Sending the :control:`rate_limit` command and keyword arguments::

    >>> app.control.broadcast('rate_limit',
    ...                          arguments={'task_name': 'myapp.mytask',
    ...                                     'rate_limit': '200/m'})

This will send the command asynchronously, without waiting for a reply.
To request a reply you have to use the `reply` argument::

    >>> app.control.broadcast('rate_limit', {
    ...     'task_name': 'myapp.mytask', 'rate_limit': '200/m'}, reply=True)
    [{'worker1.example.com': 'New rate limit set successfully'},
     {'worker2.example.com': 'New rate limit set successfully'},
     {'worker3.example.com': 'New rate limit set successfully'}]

Using the `destination` argument you can specify a list of workers
to receive the command::

    >>> app.control.broadcast('rate_limit', {
    ...     'task_name': 'myapp.mytask',
    ...     'rate_limit': '200/m'}, reply=True,
    ...                             destination=['worker1@example.com'])
    [{'worker1.example.com': 'New rate limit set successfully'}]


Of course, using the higher-level interface to set rate limits is much
more convenient, but there are commands that can only be requested
using :meth:`~@control.broadcast`.

.. control:: revoke

Revoking tasks
==============
pool support: all
broker support: *amqp, redis*

All worker nodes keeps a memory of revoked task ids, either in-memory or
persistent on disk (see :ref:`worker-persistent-revokes`).

When a worker receives a revoke request it will skip executing
the task, but it won't terminate an already executing task unless
the `terminate` option is set.

.. note::

    The terminate option is a last resort for administrators when
    a task is stuck.  It's not for terminating the task,
    it's for terminating the process that is executing the task, and that
    process may have already started processing another task at the point
    when the signal is sent, so for this rason you must never call this
    programatically.

If `terminate` is set the worker child process processing the task
will be terminated.  The default signal sent is `TERM`, but you can
specify this using the `signal` argument.  Signal can be the uppercase name
of any signal defined in the :mod:`signal` module in the Python Standard
Library.

Terminating a task also revokes it.

**Example**

::

    >>> result.revoke()

    >>> AsyncResult(id).revoke()

    >>> app.control.revoke('d9078da5-9915-40a0-bfa1-392c7bde42ed')

    >>> app.control.revoke('d9078da5-9915-40a0-bfa1-392c7bde42ed',
    ...                    terminate=True)

    >>> app.control.revoke('d9078da5-9915-40a0-bfa1-392c7bde42ed',
    ...                    terminate=True, signal='SIGKILL')




Revoking multiple tasks
-----------------------

.. versionadded:: 3.1


The revoke method also accepts a list argument, where it will revoke
several tasks at once.

**Example**

::

    >>> app.control.revoke([
    ...    '7993b0aa-1f0b-4780-9af0-c47c0858b3f2',
    ...    'f565793e-b041-4b2b-9ca4-dca22762a55d',
    ...    'd9d35e03-2997-42d0-a13e-64a66b88a618',
    ])


The ``GroupResult.revoke`` method takes advantage of this since
version 3.1.

.. _worker-persistent-revokes:

Persistent revokes
------------------

Revoking tasks works by sending a broadcast message to all the workers,
the workers then keep a list of revoked tasks in memory.  When a worker starts
up it will synchronize revoked tasks with other workers in the cluster.

The list of revoked tasks is in-memory so if all workers restart the list
of revoked ids will also vanish.  If you want to preserve this list between
restarts you need to specify a file for these to be stored in by using the `--statedb`
argument to :program:`celery worker`:

.. code-block:: bash

    celery -A proj worker -l info --statedb=/var/run/celery/worker.state

or if you use :program:`celery multi` you will want to create one file per
worker instance so then you can use the `%n` format to expand the current node
name:

.. code-block:: bash

    celery multi start 2 -l info --statedb=/var/run/celery/%n.state


Note that remote control commands must be working for revokes to work.
Remote control commands are only supported by the RabbitMQ (amqp) and Redis
at this point.

.. _worker-time-limits:

Time Limits
===========

.. versionadded:: 2.0

pool support: *prefork/gevent*

.. sidebar:: Soft, or hard?

    The time limit is set in two values, `soft` and `hard`.
    The soft time limit allows the task to catch an exception
    to clean up before it is killed: the hard timeout is not catchable
    and force terminates the task.

A single task can potentially run forever, if you have lots of tasks
waiting for some event that will never happen you will block the worker
from processing new tasks indefinitely.  The best way to defend against
this scenario happening is enabling time limits.

The time limit (`--time-limit`) is the maximum number of seconds a task
may run before the process executing it is terminated and replaced by a
new process.  You can also enable a soft time limit (`--soft-time-limit`),
this raises an exception the task can catch to clean up before the hard
time limit kills it:

.. code-block:: python

    from myapp import app
    from celery.exceptions import SoftTimeLimitExceeded

    @app.task
    def mytask():
        try:
            do_work()
        except SoftTimeLimitExceeded:
            clean_up_in_a_hurry()

Time limits can also be set using the :setting:`CELERYD_TASK_TIME_LIMIT` /
:setting:`CELERYD_TASK_SOFT_TIME_LIMIT` settings.

.. note::

    Time limits do not currently work on Windows and other
    platforms that do not support the ``SIGUSR1`` signal.


Changing time limits at runtime
-------------------------------
.. versionadded:: 2.3

broker support: *amqp, redis*

There is a remote control command that enables you to change both soft
and hard time limits for a task — named ``time_limit``.

Example changing the time limit for the ``tasks.crawl_the_web`` task
to have a soft time limit of one minute, and a hard time limit of
two minutes::

    >>> app.control.time_limit('tasks.crawl_the_web',
                               soft=60, hard=120, reply=True)
    [{'worker1.example.com': {'ok': 'time limits set successfully'}}]

Only tasks that starts executing after the time limit change will be affected.

.. _worker-rate-limits:

Rate Limits
===========

.. control:: rate_limit

Changing rate-limits at runtime
-------------------------------

Example changing the rate limit for the `myapp.mytask` task to execute
at most 200 tasks of that type every minute:

.. code-block:: python

    >>> app.control.rate_limit('myapp.mytask', '200/m')

The above does not specify a destination, so the change request will affect
all worker instances in the cluster.  If you only want to affect a specific
list of workers you can include the ``destination`` argument:

.. code-block:: python

    >>> app.control.rate_limit('myapp.mytask', '200/m',
    ...            destination=['celery@worker1.example.com'])

.. warning::

    This won't affect workers with the
    :setting:`CELERY_DISABLE_RATE_LIMITS` setting enabled.

.. _worker-maxtasksperchild:

Max tasks per child setting
===========================

.. versionadded:: 2.0

pool support: *prefork*

With this option you can configure the maximum number of tasks
a worker can execute before it's replaced by a new process.

This is useful if you have memory leaks you have no control over
for example from closed source C extensions.

The option can be set using the workers `--maxtasksperchild` argument
or using the :setting:`CELERYD_MAX_TASKS_PER_CHILD` setting.

.. _worker-autoscaling:

Autoscaling
===========

.. versionadded:: 2.2

pool support: *prefork*, *gevent*

The *autoscaler* component is used to dynamically resize the pool
based on load:

- The autoscaler adds more pool processes when there is work to do,
    - and starts removing processes when the workload is low.

It's enabled by the :option:`--autoscale` option, which needs two
numbers: the maximum and minimum number of pool processes::

        --autoscale=AUTOSCALE
             Enable autoscaling by providing
             max_concurrency,min_concurrency.  Example:
               --autoscale=10,3 (always keep 3 processes, but grow to
              10 if necessary).

You can also define your own rules for the autoscaler by subclassing
:class:`~celery.worker.autoscaler.Autoscaler`.
Some ideas for metrics include load average or the amount of memory available.
You can specify a custom autoscaler with the :setting:`CELERYD_AUTOSCALER` setting.

.. _worker-queues:

Queues
======

A worker instance can consume from any number of queues.
By default it will consume from all queues defined in the
:setting:`CELERY_QUEUES` setting (which if not specified defaults to the
queue named ``celery``).

You can specify what queues to consume from at startup,
by giving a comma separated list of queues to the :option:`-Q` option:

.. code-block:: bash

    $ celery worker -l info -Q foo,bar,baz

If the queue name is defined in :setting:`CELERY_QUEUES` it will use that
configuration, but if it's not defined in the list of queues Celery will
automatically generate a new queue for you (depending on the
:setting:`CELERY_CREATE_MISSING_QUEUES` option).

You can also tell the worker to start and stop consuming from a queue at
runtime using the remote control commands :control:`add_consumer` and
:control:`cancel_consumer`.

.. control:: add_consumer

Queues: Adding consumers
------------------------

The :control:`add_consumer` control command will tell one or more workers
to start consuming from a queue. This operation is idempotent.

To tell all workers in the cluster to start consuming from a queue
named "``foo``" you can use the :program:`celery control` program:

.. code-block:: bash

    $ celery control add_consumer foo
    -> worker1.local: OK
        started consuming from u'foo'

If you want to specify a specific worker you can use the
:option:`--destination`` argument:

.. code-block:: bash

    $ celery control add_consumer foo -d worker1.local

The same can be accomplished dynamically using the :meth:`@control.add_consumer` method::

    >>> myapp.control.add_consumer('foo', reply=True)
    [{u'worker1.local': {u'ok': u"already consuming from u'foo'"}}]

    >>> myapp.control.add_consumer('foo', reply=True,
    ...                            destination=['worker1@example.com'])
    [{u'worker1.local': {u'ok': u"already consuming from u'foo'"}}]


By now I have only shown examples using automatic queues,
If you need more control you can also specify the exchange, routing_key and
even other options::

    >>> myapp.control.add_consumer(
    ...     queue='baz',
    ...     exchange='ex',
    ...     exchange_type='topic',
    ...     routing_key='media.*',
    ...     options={
    ...         'queue_durable': False,
    ...         'exchange_durable': False,
    ...     },
    ...     reply=True,
    ...     destination=['w1@example.com', 'w2@example.com'])


.. control:: cancel_consumer

Queues: Cancelling consumers
----------------------------

You can cancel a consumer by queue name using the :control:`cancel_consumer`
control command.

To force all workers in the cluster to cancel consuming from a queue
you can use the :program:`celery control` program:

.. code-block:: bash

    $ celery control cancel_consumer foo

The :option:`--destination` argument can be used to specify a worker, or a
list of workers, to act on the command:

.. code-block:: bash

    $ celery control cancel_consumer foo -d worker1.local


You can also cancel consumers programmatically using the
:meth:`@control.cancel_consumer` method:

.. code-block:: bash

    >>> myapp.control.cancel_consumer('foo', reply=True)
    [{u'worker1.local': {u'ok': u"no longer consuming from u'foo'"}}]

.. control:: active_queues

Queues: List of active queues
-----------------------------

You can get a list of queues that a worker consumes from by using
the :control:`active_queues` control command:

.. code-block:: bash

    $ celery inspect active_queues
    [...]

Like all other remote control commands this also supports the
:option:`--destination` argument used to specify which workers should
reply to the request:

.. code-block:: bash

    $ celery inspect active_queues -d worker1.local
    [...]


This can also be done programmatically by using the
:meth:`@control.inspect.active_queues` method::

    >>> myapp.inspect().active_queues()
    [...]

    >>> myapp.inspect(['worker1.local']).active_queues()
    [...]

.. _worker-autoreloading:

Autoreloading
=============

.. versionadded:: 2.5

pool support: *prefork, eventlet, gevent, threads, solo*

Starting :program:`celery worker` with the :option:`--autoreload` option will
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
and if the prefork pool is used the child processes will finish the work
they are doing and exit, so that they can be replaced by fresh processes
effectively reloading the code.

File system notification backends are pluggable, and it comes with three
implementations:

* inotify (Linux)

    Used if the :mod:`pyinotify` library is installed.
    If you are running on Linux this is the recommended implementation,
    to install the :mod:`pyinotify` library you have to run the following
    command:

    .. code-block:: bash

        $ pip install pyinotify

* kqueue (OS X/BSD)

* stat

    The fallback implementation simply polls the files using ``stat`` and is very
    expensive.

You can force an implementation by setting the :envvar:`CELERYD_FSNOTIFY`
environment variable:

.. code-block:: bash

    $ env CELERYD_FSNOTIFY=stat celery worker -l info --autoreload

.. _worker-autoreload:

.. control:: pool_restart

Pool Restart Command
--------------------

.. versionadded:: 2.5

Requires the :setting:`CELERYD_POOL_RESTARTS` setting to be enabled.

The remote control command :control:`pool_restart` sends restart requests to
the workers child processes.  It is particularly useful for forcing
the worker to import new modules, or for reloading already imported
modules.  This command does not interrupt executing tasks.

Example
~~~~~~~

Running the following command will result in the `foo` and `bar` modules
being imported by the worker processes:

.. code-block:: python

    >>> app.control.broadcast('pool_restart',
    ...                       arguments={'modules': ['foo', 'bar']})

Use the ``reload`` argument to reload modules it has already imported:

.. code-block:: python

    >>> app.control.broadcast('pool_restart',
    ...                       arguments={'modules': ['foo'],
    ...                                  'reload': True})

If you don't specify any modules then all known tasks modules will
be imported/reloaded:

.. code-block:: python

    >>> app.control.broadcast('pool_restart', arguments={'reload': True})

The ``modules`` argument is a list of modules to modify. ``reload``
specifies whether to reload modules if they have previously been imported.
By default ``reload`` is disabled. The `pool_restart` command uses the
Python :func:`reload` function to reload modules, or you can provide
your own custom reloader by passing the ``reloader`` argument.

.. note::

    Module reloading comes with caveats that are documented in :func:`reload`.
    Please read this documentation and make sure your modules are suitable
    for reloading.

.. seealso::

    - http://pyunit.sourceforge.net/notes/reloading.html
    - http://www.indelible.org/ink/python-reloading/
    - http://docs.python.org/library/functions.html#reload


.. _worker-inspect:

Inspecting workers
==================

:class:`@control.inspect` lets you inspect running workers.  It
uses remote control commands under the hood.

You can also use the ``celery`` command to inspect workers,
and it supports the same commands as the :class:`@Celery.control` interface.

.. code-block:: python

    # Inspect all nodes.
    >>> i = app.control.inspect()

    # Specify multiple nodes to inspect.
    >>> i = app.control.inspect(['worker1.example.com',
                                'worker2.example.com'])

    # Specify a single node to inspect.
    >>> i = app.control.inspect('worker1.example.com')

.. _worker-inspect-registered-tasks:

Dump of registered tasks
------------------------

You can get a list of tasks registered in the worker using the
:meth:`~@control.inspect.registered`::

    >>> i.registered()
    [{'worker1.example.com': ['tasks.add',
                              'tasks.sleeptask']}]

.. _worker-inspect-active-tasks:

Dump of currently executing tasks
---------------------------------

You can get a list of active tasks using
:meth:`~@control.inspect.active`::

    >>> i.active()
    [{'worker1.example.com':
        [{'name': 'tasks.sleeptask',
          'id': '32666e9b-809c-41fa-8e93-5ae0c80afbbf',
          'args': '(8,)',
          'kwargs': '{}'}]}]

.. _worker-inspect-eta-schedule:

Dump of scheduled (ETA) tasks
-----------------------------

You can get a list of tasks waiting to be scheduled by using
:meth:`~@control.inspect.scheduled`::

    >>> i.scheduled()
    [{'worker1.example.com':
        [{'eta': '2010-06-07 09:07:52', 'priority': 0,
          'request': {
            'name': 'tasks.sleeptask',
            'id': '1a7980ea-8b19-413e-91d2-0b74f3844c4d',
            'args': '[1]',
            'kwargs': '{}'}},
         {'eta': '2010-06-07 09:07:53', 'priority': 0,
          'request': {
            'name': 'tasks.sleeptask',
            'id': '49661b9a-aa22-4120-94b7-9ee8031d219d',
            'args': '[2]',
            'kwargs': '{}'}}]}]

.. note::

    These are tasks with an eta/countdown argument, not periodic tasks.

.. _worker-inspect-reserved:

Dump of reserved tasks
----------------------

Reserved tasks are tasks that has been received, but is still waiting to be
executed.

You can get a list of these using
:meth:`~@control.inspect.reserved`::

    >>> i.reserved()
    [{'worker1.example.com':
        [{'name': 'tasks.sleeptask',
          'id': '32666e9b-809c-41fa-8e93-5ae0c80afbbf',
          'args': '(8,)',
          'kwargs': '{}'}]}]


.. _worker-statistics:

Statistics
----------

The remote control command ``inspect stats`` (or
:meth:`~@control.inspect.stats`) will give you a long list of useful (or not
so useful) statistics about the worker:

.. code-block:: bash

    $ celery -A proj inspect stats

The output will include the following fields:

- ``broker``

    Section for broker information.

    * ``connect_timeout``

        Timeout in seconds (int/float) for establishing a new connection.

    * ``heartbeat``

        Current heartbeat value (set by client).

    * ``hostname``

        Hostname of the remote broker.

    * ``insist``

        No longer used.

    * ``login_method``

        Login method used to connect to the broker.

    * ``port``

        Port of the remote broker.

    * ``ssl``

        SSL enabled/disabled.

    * ``transport``

        Name of transport used (e.g. ``amqp`` or ``redis``)

    * ``transport_options``

        Options passed to transport.

    * ``uri_prefix``

        Some transports expects the host name to be an URL, this applies to
        for example SQLAlchemy where the host name part is the connection URI:

            redis+socket:///tmp/redis.sock

        In this example the uri prefix will be ``redis``.

    * ``userid``

        User id used to connect to the broker with.

    * ``virtual_host``

        Virtual host used.

- ``clock``

    Value of the workers logical clock.  This is a positive integer and should
    be increasing every time you receive statistics.

- ``pid``

    Process id of the worker instance (Main process).

- ``pool``

    Pool-specific section.

    * ``max-concurrency``

        Max number of processes/threads/green threads.

    * ``max-tasks-per-child``

        Max number of tasks a thread may execute before being recycled.

    * ``processes``

        List of pids (or thread-id's).

    * ``put-guarded-by-semaphore``

        Internal

    * ``timeouts``

        Default values for time limits.

    * ``writes``

        Specific to the prefork pool, this shows the distribution of writes
        to each process in the pool when using async I/O.

- ``prefetch_count``

    Current prefetch count value for the task consumer.

- ``rusage``

    System usage statistics.  The fields available may be different
    on your platform.

    From :manpage:`getrusage(2)`:

    * ``stime``

        Time spent in operating system code on behalf of this process.

    * ``utime``

        Time spent executing user instructions.

    * ``maxrss``

        The maximum resident size used by this process (in kilobytes).

    * ``idrss``

        Amount of unshared memory used for data (in kilobytes times ticks of
        execution)

    * ``isrss``

        Amount of unshared memory used for stack space (in kilobytes times
        ticks of execution)

    * ``ixrss``

        Amount of memory shared with other processes (in kilobytes times
        ticks of execution).

    * ``inblock``

        Number of times the file system had to read from the disk on behalf of
        this process.

    * ``oublock``

        Number of times the file system has to write to disk on behalf of
        this process.

    * ``majflt``

        Number of page faults which were serviced by doing I/O.

    * ``minflt``

        Number of page faults which were serviced without doing I/O.

    * ``msgrcv``

        Number of IPC messages received.

    * ``msgsnd``

        Number of IPC messages sent.

    * ``nvcsw``

        Number of times this process voluntarily invoked a context switch.

    * ``nivcsw``

        Number of times an involuntary context switch took place.

    * ``nsignals``

        Number of signals received.

    * ``nswap``

        The number of times this process was swapped entirely out of memory.


- ``total``

    List of task names and a total number of times that task have been
    executed since worker start.


Additional Commands
===================

.. control:: shutdown

Remote shutdown
---------------

This command will gracefully shut down the worker remotely:

.. code-block:: python

    >>> app.control.broadcast('shutdown') # shutdown all workers
    >>> app.control.broadcast('shutdown, destination="worker1@example.com")

.. control:: ping

Ping
----

This command requests a ping from alive workers.
The workers reply with the string 'pong', and that's just about it.
It will use the default one second timeout for replies unless you specify
a custom timeout:

.. code-block:: python

    >>> app.control.ping(timeout=0.5)
    [{'worker1.example.com': 'pong'},
     {'worker2.example.com': 'pong'},
     {'worker3.example.com': 'pong'}]

:meth:`~@control.ping` also supports the `destination` argument,
so you can specify which workers to ping::

    >>> ping(['worker2.example.com', 'worker3.example.com'])
    [{'worker2.example.com': 'pong'},
     {'worker3.example.com': 'pong'}]

.. _worker-enable-events:

.. control:: enable_events
.. control:: disable_events

Enable/disable events
---------------------

You can enable/disable events by using the `enable_events`,
`disable_events` commands.  This is useful to temporarily monitor
a worker using :program:`celery events`/:program:`celerymon`.

.. code-block:: python

    >>> app.control.enable_events()
    >>> app.control.disable_events()

.. _worker-custom-control-commands:

Writing your own remote control commands
========================================

Remote control commands are registered in the control panel and
they take a single argument: the current
:class:`~celery.worker.control.ControlDispatch` instance.
From there you have access to the active
:class:`~celery.worker.consumer.Consumer` if needed.

Here's an example control command that increments the task prefetch count:

.. code-block:: python

    from celery.worker.control import Panel

    @Panel.register
    def increase_prefetch_count(state, n=1):
        state.consumer.qos.increment_eventually(n)
        return {'ok': 'prefetch count incremented'}
