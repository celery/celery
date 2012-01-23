.. _tut-celery:

========================
 First steps with Celery
========================

.. contents::
    :local:

.. _celerytut-broker:

Choosing your Broker
====================

Before you can use Celery you need to choose, install and run a broker.
The broker is the service responsible for receiving and delivering task
messages.

There are several choices available, including:

* :ref:`broker-rabbitmq`

`RabbitMQ`_ is feature-complete, safe and durable. If not losing tasks
is important to you, then this is your best option.

* :ref:`broker-redis`

`Redis`_ is also feature-complete, but power failures or abrupt termination
may result in data loss.

* :ref:`broker-sqlalchemy`
* :ref:`broker-django`

Using a database as a message queue is not recommended, but can be sufficient
for very small installations.  Celery can use the SQLAlchemy and Django ORM.

* and more.

In addition to the above, there are several other transport implementations
to choose from, including :ref:`broker-couchdb`, :ref:`broker-beanstalk`,
:ref:`broker-mongodb`, and SQS.  There is a `Transport Comparison`_
in the Kombu documentation.

.. _`RabbitMQ`: http://www.rabbitmq.com/
.. _`Redis`: http://redis.io/
.. _`Transport Comparison`: http://kombu.rtfd.org/transport-comparison

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

        BROKER_URL = "amqp://guest:guest@localhost:5672//"

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
with the task.  Applying a task returns an
:class:`~celery.result.AsyncResult`, if you have configured a result store
the :class:`~celery.result.AsyncResult` enables you to check the state of
the task, wait for the task to finish, get its return value
or exception/traceback if the task failed, and more.

Keeping Results
---------------

If you want to keep track of the tasks state, Celery needs to store or send
the states somewhere.  There are several
built-in backends to choose from: SQLAlchemy/Django ORM, Memcached, Redis,
AMQP, MongoDB, Tokyo Tyrant and Redis -- or you can define your own.

For this example we will use the `amqp` result backend, which sends states
as messages.  The backend is configured via the ``CELERY_RESULT_BACKEND``
option, in addition individual result backends may have additional settings
you can configure::

    CELERY_RESULT_BACKEND = "amqp"

    #: We want the results to expire in 5 minutes, note that this requires
    #: RabbitMQ version 2.1.1 or higher, so please comment out if you have
    #: an earlier version.
    CELERY_TASK_RESULT_EXPIRES = 300

To read more about result backends please see :ref:`task-result-backends`.

Now with the result backend configured, let's execute the task again.
This time we'll hold on to the :class:`~celery.result.AsyncResult`::

    >>> result = add.delay(4, 4)

Here's some examples of what you can do when you have results::

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
