.. _signals:

=======
Signals
=======


.. contents::
    :local:

Signals allows decoupled applications to receive notifications when
certain actions occur elsewhere in the application.

Celery ships with many signals that you application can hook into
to augment behavior of certain actions.

.. _signal-basics:

Basics
======

Several kinds of events trigger signals, you can connect to these signals
to perform actions as they trigger.

Example connecting to the :signal:`task_sent` signal:

.. code-block:: python

    from celery.signals import task_sent

    def task_sent_handler(sender=None, task_id=None, task=None, args=None,
                          kwargs=None, \*\*kwds):
        print("Got signal task_sent for task id %s" % (task_id, ))

    task_sent.connect(task_sent_handler)


Some signals also have a sender which you can filter by. For example the
:signal:`task_sent` signal uses the task name as a sender, so you can
connect your handler to be called only when tasks with name `"tasks.add"`
has been sent by providing the `sender` argument to
:class:`~celery.utils.dispatch.signal.Signal.connect`:

.. code-block:: python

    task_sent.connect(task_sent_handler, sender="tasks.add")

.. _signal-ref:

Signals
=======

Task Signals
------------

.. signal:: task_sent

task_sent
~~~~~~~~~

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

.. signal:: task_prerun

task_prerun
~~~~~~~~~~~

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

.. signal:: task_postrun

task_postrun
~~~~~~~~~~~~

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

.. signal:: task_failure

task_failure
~~~~~~~~~~~~

Dispatched when a task fails.

Sender is the task class executed.

Provides arguments:

* task_id
    Id of the task.

* exception
    Exception instance raised.

* args
    Positional arguments the task was called with.

* kwargs
    Keyword arguments the task was called with.

* traceback
    Stack trace object.

* einfo
    The :class:`celery.datastructures.ExceptionInfo` instance.

Worker Signals
--------------

.. signal:: worker_init

worker_init
~~~~~~~~~~~

Dispatched before the worker is started.

.. signal:: worker_ready

worker_ready
~~~~~~~~~~~~

Dispatched when the worker is ready to accept work.

.. signal:: worker_process_init

worker_process_init
~~~~~~~~~~~~~~~~~~~

Dispatched by each new pool worker process when it starts.

.. signal:: worker_shutdown

worker_shutdown
~~~~~~~~~~~~~~~

Dispatched when the worker is about to shut down.

Celerybeat Signals
------------------

.. signal:: beat_init

beat_init
~~~~~~~~~

Dispatched when celerybeat starts (either standalone or embedded).
Sender is the :class:`celery.beat.Service` instance.

.. signal:: beat_embedded_init

beat_embedded_init
~~~~~~~~~~~~~~~~~~

Dispatched in addition to the :signal:`beat_init` signal when celerybeat is
started as an embedded process.  Sender is the
:class:`celery.beat.Service` instance.

Eventlet Signals
----------------

.. signal:: eventlet_pool_started

eventlet_pool_started
~~~~~~~~~~~~~~~~~~~~~

Sent when the eventlet pool has been started.

Sender is the :class:`celery.concurrency.eventlet.TaskPool` instance.

.. signal:: eventlet_pool_preshutdown

eventlet_pool_preshutdown
~~~~~~~~~~~~~~~~~~~~~~~~~

Sent when the worker shutdown, just before the eventlet pool
is requested to wait for remaining workers.

Sender is the :class:`celery.concurrency.eventlet.TaskPool` instance.

.. signal:: eventlet_pool_postshutdown

eventlet_pool_postshutdown
~~~~~~~~~~~~~~~~~~~~~~~~~~

Sent when the pool has been joined and the worker is ready to shutdown.

Sender is the :class:`celery.concurrency.eventlet.TaskPool` instance.

.. signal:: eventlet_pool_apply

eventlet_pool_apply
~~~~~~~~~~~~~~~~~~~

Sent whenever a task is applied to the pool.

Sender is the :class:`celery.concurrency.eventlet.TaskPool` instance.

Provides arguments:

* target

    The target function.

* args

    Positional arguments.

* kwargs

    Keyword arguments.

Logging Signals
---------------

.. signal:: setup_logging

setup_logging
~~~~~~~~~~~~~

Celery won't configure the loggers if this signal is connected,
so you can use this to completely override the logging configuration
with your own.

If you would like to augment the logging configuration setup by
Celery then you can use the :signal:`after_setup_logger` and
:signal:`after_setup_task_logger` signals.

Provides arguments:

* loglevel
    The level of the logging object.

* logfile
    The name of the logfile.

* format
    The log format string.

* colorize
    Specify if log messages are colored or not.

.. signal:: after_setup_logger

after_setup_logger
~~~~~~~~~~~~~~~~~~

Sent after the setup of every global logger (not task loggers).
Used to augment logging configuration.

Provides arguments:

* logger
    The logger object.

* loglevel
    The level of the logging object.

* logfile
    The name of the logfile.

* format
    The log format string.

* colorize
    Specify if log messages are colored or not.

.. signal:: after_setup_task_logger

after_setup_task_logger
~~~~~~~~~~~~~~~~~~~~~~~

Sent after the setup of every single task logger.
Used to augment logging configuration.

Provides arguments:

* logger
    The logger object.

* loglevel
    The level of the logging object.

* logfile
    The name of the logfile.

* format
    The log format string.

* colorize
    Specify if log messages are colored or not.
