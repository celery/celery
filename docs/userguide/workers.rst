.. _guide-worker:

===============
 Workers Guide
===============

.. contents::
    :local:

.. _worker-starting:

Starting the worker
===================

You can start celeryd to run in the foreground by executing the command::

    $ celeryd --loglevel=INFO

You probably want to use a daemonization tool to start
`celeryd` in the background.  See :ref:`daemonizing` for help
using `celeryd` with popular daemonization tools.

For a full list of available command line options see
:mod:`~celery.bin.celeryd`, or simply do::

    $ celeryd --help

You can also start multiple workers on the same machine. If you do so
be sure to give a unique name to each individual worker by specifying a
host name with the :option:`--hostname|-n` argument::

    $ celeryd --loglevel=INFO --concurrency=10 -n worker1.example.com
    $ celeryd --loglevel=INFO --concurrency=10 -n worker2.example.com
    $ celeryd --loglevel=INFO --concurrency=10 -n worker3.example.com

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
be lost (unless the tasks have the :attr:`~celery.task.base.Task.acks_late`
option set).

Also as processes can't override the :sig:`KILL` signal, the worker will
not be able to reap its children, so make sure to do so manually.  This
command usually does the trick::

    $ ps auxww | grep celeryd | awk '{print $2}' | xargs kill -9

.. _worker-restarting:

Restarting the worker
=====================

Other than stopping then starting the worker to restart, you can also
restart the worker using the :sig:`HUP` signal::

    $ kill -HUP $pid

The worker will then replace itself with a new instance using the same
arguments as it was started with.

.. _worker-concurrency:

Concurrency
===========

By default multiprocessing is used to perform concurrent execution of tasks,
but you can also use :ref:`Eventlet <concurrency-eventlet>`.  The number
of worker processes/threads can be changed using the :option:`--concurrency`
argument and defaults to the number of CPUs available on the machine.

.. admonition:: Number of processes (multiprocessing)

    More worker processes are usually better, but there's a cut-off point where
    adding more processes affects performance in negative ways.
    There is even some evidence to support that having multiple celeryd's running,
    may perform better than having a single worker.  For example 3 celeryd's with
    10 worker processes each.  You need to experiment to find the numbers that
    works best for you, as this varies based on application, work load, task
    run times and other factors.

.. _worker-persistent-revokes:

Persistent revokes
==================

Revoking tasks works by sending a broadcast message to all the workers,
the workers then keep a list of revoked tasks in memory.

If you want tasks to remain revoked after worker restart you need to
specify a file for these to be stored in, either by using the `--statedb`
argument to :mod:`~celery.bin.celeryd` or the :setting:`CELERYD_STATE_DB`
setting.  See :setting:`CELERYD_STATE_DB` for more information.

.. _worker-time-limits:

Time limits
===========

.. versionadded:: 2.0

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

    from celery.task import task
    from celery.exceptions import SoftTimeLimitExceeded

    @task()
    def mytask():
        try:
            do_work()
        except SoftTimeLimitExceeded:
            clean_up_in_a_hurry()

Time limits can also be set using the :setting:`CELERYD_TASK_TIME_LIMIT` /
:setting:`CELERYD_SOFT_TASK_TIME_LIMIT` settings.

.. note::

    Time limits do not currently work on Windows.

.. _worker-maxtasksperchild:

Max tasks per child setting
===========================

.. versionadded: 2.0

With this option you can configure the maximum number of tasks
a worker can execute before it's replaced by a new process.

This is useful if you have memory leaks you have no control over
for example from closed source C extensions.

The option can be set using the `--maxtasksperchild` argument
to `celeryd` or using the :setting:`CELERYD_MAX_TASKS_PER_CHILD` setting.

.. _worker-remote-control:

Remote control
==============

.. versionadded:: 2.0

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

.. seealso::

    The :program:`celeryctl` program is used to execute remote control
    commands from the command line.  It supports all of the commands
    listed below.  See :ref:`monitoring-celeryctl` for more information.

.. _worker-broadcast-fun:

The :func:`~celery.task.control.broadcast` function.
----------------------------------------------------

This is the client function used to send commands to the workers.
Some remote control commands also have higher-level interfaces using
:func:`~celery.task.control.broadcast` in the background, like
:func:`~celery.task.control.rate_limit` and :func:`~celery.task.control.ping`.

Sending the :control:`rate_limit` command and keyword arguments::

    >>> from celery.task.control import broadcast
    >>> broadcast("rate_limit", arguments={"task_name": "myapp.mytask",
    ...                                    "rate_limit": "200/m"})

This will send the command asynchronously, without waiting for a reply.
To request a reply you have to use the `reply` argument::

    >>> broadcast("rate_limit", {"task_name": "myapp.mytask",
    ...                          "rate_limit": "200/m"}, reply=True)
    [{'worker1.example.com': 'New rate limit set successfully'},
     {'worker2.example.com': 'New rate limit set successfully'},
     {'worker3.example.com': 'New rate limit set successfully'}]

Using the `destination` argument you can specify a list of workers
to receive the command::

    >>> broadcast
    >>> broadcast("rate_limit", {"task_name": "myapp.mytask",
    ...                          "rate_limit": "200/m"}, reply=True,
    ...           destination=["worker1.example.com"])
    [{'worker1.example.com': 'New rate limit set successfully'}]


Of course, using the higher-level interface to set rate limits is much
more convenient, but there are commands that can only be requested
using :func:`~celery.task.control.broadcast`.

.. _worker-rate-limits:

.. control:: rate_limit

Rate limits
-----------

Example changing the rate limit for the `myapp.mytask` task to accept
200 tasks a minute on all servers::

    >>> from celery.task.control import rate_limit
    >>> rate_limit("myapp.mytask", "200/m")

Example changing the rate limit on a single host by specifying the
destination hostname::

    >>> rate_limit("myapp.mytask", "200/m",
    ...            destination=["worker1.example.com"])

.. warning::

    This won't affect workers with the
    :setting:`CELERY_DISABLE_RATE_LIMITS` setting on. To re-enable rate limits
    then you have to restart the worker.

.. control:: revoke

Revoking tasks
--------------

All worker nodes keeps a memory of revoked task ids, either in-memory or
persistent on disk (see :ref:`worker-persistent-revokes`).

When a worker receives a revoke request it will skip executing
the task, but it won't terminate an already executing task unless
the `terminate` option is set.

If `terminate` is set the worker child process processing the task
will be terminated.  The default signal sent is `TERM`, but you can
specify this using the `signal` argument.  Signal can be the uppercase name
of any signal defined in the :mod:`signal` module in the Python Standard
Library.

Terminating a task also revokes it.

**Example**

::

    >>> from celery.task.control import revoke
    >>> revoke("d9078da5-9915-40a0-bfa1-392c7bde42ed")

    >>> revoke("d9078da5-9915-40a0-bfa1-392c7bde42ed",
    ...        terminate=True)

    >>> revoke("d9078da5-9915-40a0-bfa1-392c7bde42ed",
    ...        terminate=True, signal="SIGKILL")

.. control:: shutdown

Remote shutdown
---------------

This command will gracefully shut down the worker remotely::

    >>> broadcast("shutdown") # shutdown all workers
    >>> broadcast("shutdown, destination="worker1.example.com")

.. control:: ping

Ping
----

This command requests a ping from alive workers.
The workers reply with the string 'pong', and that's just about it.
It will use the default one second timeout for replies unless you specify
a custom timeout::

    >>> from celery.task.control import ping
    >>> ping(timeout=0.5)
    [{'worker1.example.com': 'pong'},
     {'worker2.example.com': 'pong'},
     {'worker3.example.com': 'pong'}]

:func:`~celery.task.control.ping` also supports the `destination` argument,
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
a worker using :program:`celeryev`/:program:`celerymon`.

.. code-block:: python

    >>> broadcast("enable_events")
    >>> broadcast("disable_events")

.. _worker-custom-control-commands:

Writing your own remote control commands
----------------------------------------

Remote control commands are registered in the control panel and
they take a single argument: the current
:class:`~celery.worker.control.ControlDispatch` instance.
From there you have access to the active
:class:`~celery.worker.consumer.Consumer` if needed.

Here's an example control command that restarts the broker connection:

.. code-block:: python

    from celery.worker.control import Panel

    @Panel.register
    def reset_connection(panel):
        panel.logger.critical("Connection reset by remote control.")
        panel.consumer.reset_connection()
        return {"ok": "connection reset"}


These can be added to task modules, or you can keep them in their own module
then import them using the :setting:`CELERY_IMPORTS` setting::

    CELERY_IMPORTS = ("myapp.worker.control", )

.. _worker-inspect:

Inspecting workers
==================

:class:`celery.task.control.inspect` lets you inspect running workers.  It
uses remote control commands under the hood.

.. code-block:: python

    >>> from celery.task.control import inspect

    # Inspect all nodes.
    >>> i = inspect()

    # Specify multiple nodes to inspect.
    >>> i = inspect(["worker1.example.com", "worker2.example.com"])

    # Specify a single node to inspect.
    >>> i = inspect("worker1.example.com")


.. _worker-inspect-registered-tasks:

Dump of registered tasks
------------------------

You can get a list of tasks registered in the worker using the
:meth:`~celery.task.control.inspect.registered_tasks`::

    >>> i.registered_tasks()
    [{'worker1.example.com': ['celery.delete_expired_task_meta',
                              'celery.execute_remote',
                              'celery.map_async',
                              'celery.ping',
                              'celery.task.http.HttpDispatchTask',
                              'tasks.add',
                              'tasks.sleeptask']}]

.. _worker-inspect-active-tasks:

Dump of currently executing tasks
---------------------------------

You can get a list of active tasks using
:meth:`~celery.task.control.inspect.active`::

    >>> i.active()
    [{'worker1.example.com':
        [{"name": "tasks.sleeptask",
          "id": "32666e9b-809c-41fa-8e93-5ae0c80afbbf",
          "args": "(8,)",
          "kwargs": "{}"}]}]

.. _worker-inspect-eta-schedule:

Dump of scheduled (ETA) tasks
-----------------------------

You can get a list of tasks waiting to be scheduled by using
:meth:`~celery.task.control.inspect.scheduled`::

    >>> i.scheduled()
    [{'worker1.example.com':
        [{"eta": "2010-06-07 09:07:52", "priority": 0,
          "request": {
            "name": "tasks.sleeptask",
            "id": "1a7980ea-8b19-413e-91d2-0b74f3844c4d",
            "args": "[1]",
            "kwargs": "{}"}},
         {"eta": "2010-06-07 09:07:53", "priority": 0,
          "request": {
            "name": "tasks.sleeptask",
            "id": "49661b9a-aa22-4120-94b7-9ee8031d219d",
            "args": "[2]",
            "kwargs": "{}"}}]}]

Note that these are tasks with an eta/countdown argument, not periodic tasks.

.. _worker-inspect-reserved:

Dump of reserved tasks
----------------------

Reserved tasks are tasks that has been received, but is still waiting to be
executed.

You can get a list of these using
:meth:`~celery.task.control.inspect.reserved`::

    >>> i.reserved()
    [{'worker1.example.com':
        [{"name": "tasks.sleeptask",
          "id": "32666e9b-809c-41fa-8e93-5ae0c80afbbf",
          "args": "(8,)",
          "kwargs": "{}"}]}]
