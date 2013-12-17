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

Example connecting to the :signal:`after_task_publish` signal:

.. code-block:: python

    from celery.signals import after_task_publish

    @after_task_publish.connect
    def task_sent_handler(sender=None, body=None, **kwargs):
        print('after_task_publish for task id {body[id]}'.format(
            body=body,
        ))


Some signals also have a sender which you can filter by. For example the
:signal:`after_task_publish` signal uses the task name as a sender, so by
providing the ``sender`` argument to
:class:`~celery.utils.dispatch.signal.Signal.connect` you can
connect your handler to be called every time a task with name `"proj.tasks.add"`
is published:

.. code-block:: python

    @after_task_publish.connect(sender='proj.tasks.add')
    def task_sent_handler(sender=None, body=None, **kwargs):
        print('after_task_publish for task id {body[id]}'.format(
            body=body,
        ))

Signals use the same implementation as django.core.dispatch. As a result other
keyword parameters (e.g. signal) are passed to all signal handlers by default.

The best practice for signal handlers is to accept arbitrary keyword
arguments (i.e. ``**kwargs``).  That way new celery versions can add additional
arguments without breaking user code.

.. _signal-ref:

Signals
=======

Task Signals
------------

.. signal:: before_task_publish

before_task_publish
~~~~~~~~~~~~~~~~~~~
.. versionadded:: 3.1

Dispatched before a task is published.
Note that this is executed in the process sending the task.

Sender is the name of the task being sent.

Provides arguements:

* body

    Task message body.

    This is a mapping containing the task message fields
    (see :ref:`task-message-protocol-v1`).

* exchange

    Name of the exchange to send to or a :class:`~kombu.Exchange` object.

* routing_key

    Routing key to use when sending the message.

* headers

    Application headers mapping (can be modified).

* properties

    Message properties (can be modified)

* declare

    List of entities (:class:`~kombu.Exchange`,
    :class:`~kombu.Queue` or :class:~`kombu.binding` to declare before
    publishing the message.  Can be modified.

* retry_policy

    Mapping of retry options.  Can be any argument to
    :meth:`kombu.Connection.ensure` and can be modified.

.. signal:: after_task_publish

after_task_publish
~~~~~~~~~~~~~~~~~~

Dispatched when a task has been sent to the broker.
Note that this is executed in the process that sent the task.

Sender is the name of the task being sent.

Provides arguments:

* body

    The task message body, see :ref:`task-message-protocol-v1`
    for a reference of possible fields that can be defined.

* exchange

    Name of the exchange or :class:`~kombu.Exchange` object used.

* routing_key

    Routing key used.

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

* state

    Name of the resulting state.

.. signal:: task_success

task_success
~~~~~~~~~~~~

Dispatched when a task succeeds.

Sender is the task class executed.

Provides arguments

* result
    Return value of the task.

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

.. signal:: task_revoked

task_revoked
~~~~~~~~~~~~

Dispatched when a task is revoked/terminated by the worker.

Sender is the task class revoked/terminated.

Provides arguments:

* request

    This is a :class:`~celery.worker.job.Request` instance, and not
    ``task.request``.   When using the prefork pool this signal
    is dispatched in the parent process, so ``task.request`` is not available
    and should not be used.  Use this object instead, which should have many
    of the same fields.

* terminated
    Set to :const:`True` if the task was terminated.

* signum
    Signal number used to terminate the task. If this is :const:`None` and
    terminated is :const:`True` then :sig:`TERM` should be assumed.

* expired
  Set to :const:`True` if the task expired.

App Signals
-----------

.. signal:: import_modules

import_modules
~~~~~~~~~~~~~~

This signal is sent when a program (worker, beat, shell) etc, asks
for modules in the :setting:`CELERY_INCLUDE` and :setting:`CELERY_IMPORTS`
settings to be imported.

Sender is the app instance.

Worker Signals
--------------

.. signal:: celeryd_after_setup

celeryd_after_setup
~~~~~~~~~~~~~~~~~~~

This signal is sent after the worker instance is set up,
but before it calls run.  This means that any queues from the :option:`-Q`
option is enabled, logging has been set up and so on.

It can be used to e.g. add custom queues that should always be consumed
from, disregarding the :option:`-Q` option.  Here's an example
that sets up a direct queue for each worker, these queues can then be
used to route a task to any specific worker:

.. code-block:: python

    from celery.signals import celeryd_after_setup

    @celeryd_after_setup.connect
    def setup_direct_queue(sender, instance, **kwargs):
        queue_name = '{0}.dq'.format(sender)  # sender is the nodename of the worker
        instance.app.amqp.queues.select_add(queue_name)

Provides arguments:

* sender
  Hostname of the worker.

* instance
    This is the :class:`celery.apps.worker.Worker` instance to be initialized.
    Note that only the :attr:`app` and :attr:`hostname` (nodename) attributes have been
    set so far, and the rest of ``__init__`` has not been executed.

* conf
    The configuration of the current app.


.. signal:: celeryd_init

celeryd_init
~~~~~~~~~~~~

This is the first signal sent when :program:`celery worker` starts up.
The ``sender`` is the host name of the worker, so this signal can be used
to setup worker specific configuration:

.. code-block:: python

    from celery.signals import celeryd_init

    @celeryd_init.connect(sender='worker12@example.com')
    def configure_worker12(conf=None, **kwargs):
        conf.CELERY_DEFAULT_RATE_LIMIT = '10/m'

or to set up configuration for multiple workers you can omit specifying a
sender when you connect:

.. code-block:: python

    from celery.signals import celeryd_init

    @celeryd_init.connect
    def configure_workers(sender=None, conf=None, **kwargs):
        if sender in ('worker1@example.com', 'worker2@example.com'):
            conf.CELERY_DEFAULT_RATE_LIMIT = '10/m'
        if sender == 'worker3@example.com':
            conf.CELERYD_PREFETCH_MULTIPLIER = 0

Provides arguments:

* sender
  Nodename of the worker.

* instance
    This is the :class:`celery.apps.worker.Worker` instance to be initialized.
    Note that only the :attr:`app` and :attr:`hostname` (nodename) attributes have been
    set so far, and the rest of ``__init__`` has not been executed.

* conf
    The configuration of the current app.

* options

    Options passed to the worker from command-line arguments (including
    defaults).

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

Dispatched in all pool child processes when they start.

Note that handlers attached to this signal must not be blocking
for more than 4 seconds, or the process will be killed assuming
it failed to start.

.. signal:: worker_process_shutdown

worker_process_shutdown
~~~~~~~~~~~~~~~~~~~~~~~

Dispatched in all pool child processes just before they exit.

Note: There is no guarantee that this signal will be dispatched,
similarly to finally blocks it's impossible to guarantee that handlers
will be called at shutdown, and if called it may be interrupted during.

Provides arguments:

* pid

    The pid of the child process that is about to shutdown.

* exitcode

    The exitcode that will be used when the child process exits.

.. signal:: worker_shutdown

worker_shutdown
~~~~~~~~~~~~~~~

Dispatched when the worker is about to shut down.

Beat Signals
------------

.. signal:: beat_init

beat_init
~~~~~~~~~

Dispatched when :program:`celery beat` starts (either standalone or embedded).
Sender is the :class:`celery.beat.Service` instance.

.. signal:: beat_embedded_init

beat_embedded_init
~~~~~~~~~~~~~~~~~~

Dispatched in addition to the :signal:`beat_init` signal when :program:`celery
beat` is started as an embedded process.  Sender is the
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

Command signals
---------------

.. signal:: user_preload_options

user_preload_options
~~~~~~~~~~~~~~~~~~~~

This signal is sent after any of the Celery command line programs
are finished parsing the user preload options.

It can be used to add additional command-line arguments to the
:program:`celery` umbrella command:

.. code-block:: python

    from celery import Celery
    from celery import signals
    from celery.bin.base import Option

    app = Celery()
    app.user_options['preload'].add(Option(
        '--monitoring', action='store_true',
        help='Enable our external monitoring utility, blahblah',
    ))

    @signals.user_preload_options.connect
    def handle_preload_options(options, **kwargs):
        if options['monitoring']:
            enable_monitoring()


Sender is the :class:`~celery.bin.base.Command` instance, which depends
on what program was called (e.g. for the umbrella command it will be
a :class:`~celery.bin.celery.CeleryCommand`) object).

Provides arguments:

* app

    The app instance.

* options

    Mapping of the parsed user preload options (with default values).

Deprecated Signals
------------------

.. signal:: task_sent

task_sent
~~~~~~~~~

This signal is deprecated, please use :signal:`after_task_publish` instead.
