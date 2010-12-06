"""
celery.signals
==============

Signals allows decoupled applications receive notifications when actions
occur elsewhere in the framework.

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

.. contents::
    :local:

.. _signal-basics:

Basics
------

Several kinds of events trigger signals, you can connect to these signals
to perform actions as they trigger.

Example connecting to the :data:`task_sent` signal:

.. code-block:: python

    from celery.signals import task_sent

    def task_sent_handler(sender=None, task_id=None, task=None, args=None,
                          kwargs=None, **kwds):
        print("Got signal task_sent for task id %s" % (task_id, ))

    task_sent.connect(task_sent_handler)


Some signals also have a sender which you can filter by. For example the
:data:`task_sent` signal uses the task name as a sender, so you can
connect your handler to be called only when tasks with name `"tasks.add"`
has been sent by providing the `sender` argument to
:class:`~celery.utils.dispatch.signal.Signal.connect`:

.. code-block:: python

    task_sent.connect(task_sent_handler, sender="tasks.add")

.. _signal-ref:

Signals
-------

Task Signals
~~~~~~~~~~~~

.. data:: task_sent

    Dispatched when a task has been sent to the broker.
    Note that this is executed in the client process, the one sending
    the task, not in the worker.

    Sender is the name of the task being sent.

    Provides arguments:

    * task_id
        Id of the task to be executed.

    * task
        The task being executed.

    * args
        the tasks positional arguments.

    * kwargs
        The tasks keyword arguments.

    * eta
        The time to execute the task.

    * taskset
        Id of the taskset this task is part of (if any).

.. data:: task_prerun

    Dispatched before a task is executed.

    Sender is the task class being executed.

    Provides arguments:

    * task_id
        Id of the task to be executed.

    * task
        The task being executed.

    * args
        the tasks positional arguments.

    * kwargs
        The tasks keyword arguments.

.. data:: task_postrun

    Dispatched after a task has been executed.

    Sender is the task class executed.

    Provides arguments:

    * task_id
        Id of the task to be executed.

    * task
        The task being executed.

    * args
        The tasks positional arguments.

    * kwargs
        The tasks keyword arguments.

    * retval

        The return value of the task.

Worker Signals
~~~~~~~~~~~~~~

.. data:: worker_init

    Dispatched before the worker is started.

.. data:: worker_ready

    Dispatched when the worker is ready to accept work.

.. data:: worker_process_init

    Dispatched by each new pool worker process when it starts.

.. data:: worker_shutdown

    Dispatched when the worker is about to shut down.

"""
from celery.utils.dispatch import Signal

task_sent = Signal(providing_args=["task_id", "task",
                                   "args", "kwargs",
                                   "eta", "taskset"])

task_prerun = Signal(providing_args=["task_id", "task",
                                     "args", "kwargs"])

task_postrun = Signal(providing_args=["task_id", "task",
                                      "args", "kwargs", "retval"])

worker_init = Signal(providing_args=[])
worker_process_init = Signal(providing_args=[])
worker_ready = Signal(providing_args=[])
worker_shutdown = Signal(providing_args=[])

setup_logging = Signal(providing_args=["loglevel", "logfile",
                                       "format", "colorize"])
