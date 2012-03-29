.. _tut-celery:

========================
 First steps with Celery
========================

.. contents::
    :local:

.. _celerytut-broker:

Choosing a Broker
=================

Celery requires a solution to send and receive messages, this is called
the *message transport*.  Usually this comes in the form of a separate
service called a *message broker*.

There are several choices available, including:

* :ref:`broker-rabbitmq`

`RabbitMQ`_ is feature-complete, stable, durable and easy to install.

* :ref:`broker-redis`

`Redis`_ is also feature-complete, but is more susceptible to data loss in
the event of abrupt termination or power failures.

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

.. _celerytut-conf:

Application
===========

The first thing you need is a Celery instance.  Since the instance is used as
the entry-point for everything you want to do in Celery, like creating task and
managing workers, it must be possible for other modules to import it.

Some people create a dedicated module for it, but in this tutorial we will
keep it in the same module used to start our worker.

Let's create the file :file:`worker.py`:

.. code-block:: python

    from celery import Celery

    celery = Celery(broker="amqp://guest:guest@localhost:5672")

    if __name__ == "__main__":
        celery.worker_main()

The broker argument specifies the message broker we want to use, what
we are using in this example is the default, but we keep it there for
reference so you can see what the URLs look like.

By default the state and return value (results) of the tasks are ignored,
if you want to enable this please see :ref:`celerytut-keeping-results`.

That's all you need to get started!

If you want to dig deeper there are lots of configuration possibilities that
can be applied.  For example you can set the default value for the workers
`--concurrency`` argument, which is used to decide the number of pool worker
processes, the name for this setting is :setting:`CELERYD_CONCURRENCY`:

.. code-block:: python

    celery.conf.CELERY_CONCURRENCY = 10

If you are configuring many settings then one practice is to have a separate module
containing the configuration.  You can tell your Celery instance to use
this module, historically called ``celeryconfig.py``, with the
:meth:`config_from_obj` method:

.. code-block:: python

    celery.config_from_object("celeryconfig")

For a complete reference of configuration options, see :ref:`configuration`.

.. _celerytut-simple-tasks:

Creating a simple task
======================

In this tutorial we are creating a simple task that adds two
numbers.  Tasks are defined in normal Python modules.

By convention we will call our module :file:`tasks.py`, and it looks
like this:

:file: `tasks.py`

.. code-block:: python

    from worker import celery

    @celery.task
    def add(x, y):
        return x + y

.. seealso::

    The full documentation on how to create tasks and task classes is in the
    :doc:`../userguide/tasks` part of the user guide.


.. _celerytut-running-celeryd:

Running the celery worker server
================================

We can now run our ``worker.py`` program::

    $ python worker.py --loglevel=INFO

In production you will probably want to run the worker in the
background as a daemon.  To do this you need to use the tools provided
by your platform, or something like `supervisord`_ (see :ref:`daemonizing`
for more information).

For a complete listing of the command line options available, do::

    $  python worker.py --help

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

.. _celerytut-keeping-results:

Keeping Results
---------------

If you want to keep track of the tasks state, Celery needs to store or send
the states somewhere.  There are several
built-in backends to choose from: SQLAlchemy/Django ORM, Memcached, Redis,
AMQP, MongoDB, Tokyo Tyrant and Redis -- or you can define your own.

For this example we will use the `amqp` result backend, which sends states
as messages.  The backend is configured via the :setting:`CELERY_RESULT_BACKEND`
setting or using the ``backend`` argument to :class:`Celery`, in addition individual
result backends may have additional settings
you can configure::

    from celery.backends.amqp import AMQPBackend

    celery = Celery(backend=AMQPBackend(expires=300))

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
