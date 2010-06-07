===============
 Workers Guide
===============

Starting the worker
===================

Starting celeryd in the foreground::

    $ celeryd --loglevel=INFO

You probably want to use a daemonization tool to start and stop
``celeryd`` in the background, see :doc:`../cookbook/daemonizing` for help using
some of the most popular solutions.

For a full list of available command line options see :mod:`~celery.bin.celeryd`.

You can also start multiple celeryd's on the same machine, but if you do so
be sure to give a unique name to each individual worker by specifying the
``-hostname`` argument::

    $ celeryd --loglevel=INFO --concurrency=10 -n worker1.example.com
    $ celeryd --loglevel=INFO --concurrency=10 -n worker2.example.com
    $ celeryd --loglevel=INFO --concurrency=10 -n worker3.example.com

Stopping the worker
===================

Shutdown should be accomplished using the ``TERM`` signal (although ``INT``
also works).

When shutdown is initiated the worker will finish any tasks it's currently
executing before it terminates, so if these tasks are important you should
wait for it to finish before doing anything drastic (like sending the ``KILL``
signal).

If the worker won't shutdown after considerate time, you probably have hanging
tasks, in this case it's safe to use the ``KILL`` signal but be aware that
currently executing tasks will be lost (unless the tasks have the
:attr:`~celery.task.base.Task.acks_late` option set).

Also, since the ``KILL`` signal can't be catched by processes the worker will
not be able to reap its children, so make sure you do it manually. This
command usually does the trick::

    $ ps auxww | grep celeryd | awk '{print $2}' | xargs kill -KILL

Restarting the worker
=====================

Other than stopping then starting the worker to restart, you can also
restart the worker using the ``HUP`` signal::

    $ kill -HUP $pid

The worker will then replace itself using the same arguments as it was
started with.

Remote control
==============

Workers have the ability to be remote controlled using a broadcast message
queue. The commands can be directed to all, or a specific list of workers.

Commands can also have replies, the client can then wait for and collect
those replies, but since there's no central authority to know how many
workers are available in the cluster, there is also no way to estimate
how many workers may send a reply, therefore the client has a configurable
timeout - the deadline in seconds for replies to arrive in. This timeout
defaults to one second. If the worker didn't reply within the deadline,
it doesn't necessarily mean the worker didn't reply, or worse is dead, but
may just be caused by network latency or the worker being slow at processing
commands, so adjust the timeout accordingly.

In addition to timeouts, the client can specify the maximum number
of replies to wait for. If a destination is specified this limit is set
to the number of destinations.

The :func:`~celery.task.control.broadcast` function.
----------------------------------------------------

This is the client function used to send commands to the workers.
Some remote control commands also have higher-level interfaces using
:func:`~celery.task.control.broadcast` in the background, like
:func:`~celery.task.control.rate_limit` and :func:`~celery.task.control.ping`.

Sending the ``rate_limit`` command and keyword arguments::

    >>> from celery.task.control import broadcast
    >>> broadcast("rate_limit", arguments={"task_name": "myapp.mytask",
    ...                                    "rate_limit": "200/m"})

This will send the command asynchronously, without waiting for a reply.
To request a reply you have to use the ``reply`` argument::

    >>> broadcast("rate_limit", {"task_name": "myapp.mytask",
    ...                          "rate_limit": "200/m"}, reply=True)
    [{'worker1.example.com': 'New rate limit set successfully'},
     {'worker2.example.com': 'New rate limit set successfully'},
     {'worker3.example.com': 'New rate limit set successfully'}]

Using the ``destination`` argument you can specify a list of workers
to receive the command::

    >>> broadcast
    >>> broadcast("rate_limit", {"task_name": "myapp.mytask",
    ...                          "rate_limit": "200/m"}, reply=True,
    ...           destination=["worker1.example.com"])
    [{'worker1.example.com': 'New rate limit set successfully'}]


Of course, using the higher-level interface to set rate limits is much
more convenient, but there are commands that can only be requested
using :func:`~celery.task.control.broadcast`.

Rate limits
-----------

Example changing the rate limit for the ``myapp.mytask`` task to accept
200 tasks a minute on all servers:

    >>> from celery.task.control import rate_limit
    >>> rate_limit("myapp.mytask", "200/m")

Example changing the rate limit on a single host by specifying the
destination hostname::

    >>> rate_limit("myapp.mytask", "200/m",
    ...            destination=["worker1.example.com"])

**NOTE** This won't affect workers with the ``CELERY_DISABLE_RATE_LIMITS``
setting on. To re-enable rate limits you have to restart the worker.


Remote shutdown
---------------

This command will gracefully shut down the worker from remote.

    >>> broadcast("shutdown") # shutdown all workers
    >>> broadcast("shutdown, destination="worker1.example.com")

Ping
----

This command requests a ping from alive workers.
The workers reply with the string 'pong', and that's just about it.
It will use the default one second limit for replies unless you specify
a custom ``timeout``.

    >>> from celery.task.control import ping
    >>> ping()
    [{'worker1.example.com': 'pong'},
     {'worker2.example.com': 'pong'},
     {'worker3.example.com': 'pong'}]

:func:`~celery.task.control.ping` also supports the ``destination`` argument,
so you can specify which workers to ping::

    >>> ping(['worker2.example.com', 'worker3.example.com'])
    [{'worker2.example.com': 'pong'},
     {'worker3.example.com': 'pong'}]

Writing your own remote control commands
----------------------------------------

Remote control commands are registered in the control panel and
they take a single argument: the current
:class:`~celery.worker.control.ControlDispatch` instance.
From there you have access to the active
:class:`celery.worker.listener.CarrotListener` if needed.

Here's an example control command that restarts the broker connection:

.. code-block:: python

    from celery.worker.control import Panel

    @Panel.register
    def reset_connection(panel):
        panel.logger.critical("Connection reset by remote control.")
        panel.listener.reset_connection()
        return {"ok": "connection reset"}


These can be added to task modules, or you can keep them in their own module
then import them using the ``CELERY_IMPORTS`` setting::

    CELERY_IMPORTS = ("myapp.worker.control", )

Debugging
=========

Dump of registered tasks
------------------------

You can get a list of tasks registered in the worker using the
``dump_tasks`` remote control command::

    >>> broadcast("dump_tasks", reply=True)
    [{'worker1.example.com': ['celery.delete_expired_task_meta',
                              'celery.execute_remote',
                              'celery.map_async',
                              'celery.ping',
                              'celery.task.http.HttpDispatchTask',
                              'tasks.add',
                              'tasks.sleeptask']}]

Dump of scheduled (ETA) tasks
-----------------------------

You can get a list of tasks waiting to be scheduled by using
the ``dump_schedule`` remote control command.

    >>> broadcast("dump_schedule", reply=True)
    [{'worker1.example.com':
        ['0. 2010-06-07 09:07:52 pri0 <TaskRequest: {
            name:"tasks.sleeptask",
            id:"1a7980ea-8b19-413e-91d2-0b74f3844c4d",
            args:"[1]", kwargs:"{}"}>',
        '1. 2010-06-07 09:07:53 pri0 <TaskRequest: {
            name:"tasks.sleeptask",
            id:"49661b9a-aa22-4120-94b7-9ee8031d219d",
            args:"[2]",
            kwargs:"{}"}>',

The outputted fields are (in order): position, eta, priority, request.

Note that these are tasks with an eta/countdown argument, not periodic tasks.

Dump of reserved tasks
----------------------

Reserved tasks are tasks that has been received by the broker and is waiting
for immediate execution.

You can get a list of these using the ``dump_reserved`` remote control command.

    >>> broadcast("dump_reserved", reply=True)
    [{'worker1.example.com':
        ['<TaskRequest: {name:"tasks.sleeptask",
                         id:"32666e9b-809c-41fa-8e93-5ae0c80afbbf",
                         args:"(8,)", kwargs:"{}"}>']}]
