.. _tut-celery:

========================
 First steps with Celery
========================

.. contents::
    :local:

.. _celerytut-simple-tasks:

Creating a simple task
======================

In this tutorial we are creating a simple task that adds two
numbers.  Tasks are defined in normal Python modules.

By convention we will call our module :file:`tasks.py`, and it looks
like this:

:file: `tasks.py`

.. code-block:: python

    from celery.task import task

    @task
    def add(x, y):
        return x + y


Behind the scenes the `@task` decorator actually creates a class that
inherits from :class:`~celery.task.base.Task`.  The best practice is to
only create custom task classes when you want to change generic behavior,
and use the decorator to define tasks.

.. seealso::

    The full documentation on how to create tasks and task classes is in the
    :doc:`../userguide/tasks` part of the user guide.

.. _celerytut-conf:

Configuration
=============

Celery is configured by using a configuration module.  By default
this module is called :file:`celeryconfig.py`.

The configuration module must either be in the current directory
or on the Python path, so that it can be imported.

You can also set a custom name for the configuration module by using
the :envvar:`CELERY_CONFIG_MODULE` environment variable.

Let's create our :file:`celeryconfig.py`.

1. Configure how we communicate with the broker (RabbitMQ in this example)::

        BROKER_HOST = "localhost"
        BROKER_PORT = 5672
        BROKER_USER = "myuser"
        BROKER_PASSWORD = "mypassword"
        BROKER_VHOST = "myvhost"

2. Define the backend used to store task metadata and return values::

        CELERY_RESULT_BACKEND = "amqp"

   The AMQP backend is non-persistent by default, and you can only
   fetch the result of a task once (as it's sent as a message).

   For list of backends available and related options see
   :ref:`conf-result-backend`.

3. Finally we list the modules the worker should import.  This includes
   the modules containing your tasks.

   We only have a single task module, :file:`tasks.py`, which we added earlier::

        CELERY_IMPORTS = ("tasks", )

That's it.

There are more options available, like how many processes you want to
use to process work in parallel (the :setting:`CELERY_CONCURRENCY` setting),
and we could use a persistent result store backend, but for now, this should
do.  For all of the options available, see :ref:`configuration`.

.. note::

    You can also specify modules to import using the :option:`-I` option to
    :mod:`~celery.bin.celeryd`::

        $ celeryd -l info -I tasks,handlers

    This can be a single, or a comma separated list of task modules to import
    when :program:`celeryd` starts.


.. _celerytut-running-celeryd:

Running the celery worker server
================================

To test we will run the worker server in the foreground, so we can
see what's going on in the terminal::

    $ celeryd --loglevel=INFO

In production you will probably want to run the worker in the
background as a daemon.  To do this you need to use the tools provided
by your platform, or something like `supervisord`_ (see :ref:`daemonizing`
for more information).

For a complete listing of the command line options available, do::

    $  celeryd --help

.. _`supervisord`: http://supervisord.org

.. _celerytut-executing-task:

Executing the task
==================

Whenever we want to execute our task, we use the
:meth:`~celery.task.base.Task.delay` method of the task class.

This is a handy shortcut to the :meth:`~celery.task.base.Task.apply_async`
method which gives greater control of the task execution (see
:ref:`guide-executing`).

    >>> from tasks import add
    >>> add.delay(4, 4)
    <AsyncResult: 889143a6-39a2-4e52-837b-d80d33efb22d>

At this point, the task has been sent to the message broker. The message
broker will hold on to the task until a worker server has consumed and
executed it.

Right now we have to check the worker log files to know what happened
with the task.  This is because we didn't keep the
:class:`~celery.result.AsyncResult` object returned.

The :class:`~celery.result.AsyncResult` lets us check the state of the task,
wait for the task to finish, get its return value or exception/traceback
if the task failed, and more.

Let's execute the task again -- but this time we'll keep track of the task
by holding on to the :class:`~celery.result.AsyncResult`::

    >>> result = add.delay(4, 4)

    >>> result.ready() # returns True if the task has finished processing.
    False

    >>> result.result # task is not ready, so no return value yet.
    None

    >>> result.get()   # Waits until the task is done and returns the retval.
    8

    >>> result.result # direct access to result, doesn't re-raise errors.
    8

    >>> result.successful() # returns True if the task didn't end in failure.
    True

If the task raises an exception, the return value of `result.successful()`
will be :const:`False`, and `result.result` will contain the exception instance
raised by the task.

Where to go from here
=====================

After this you should read the :ref:`guide`. Specifically
:ref:`guide-tasks` and :ref:`guide-executing`.
