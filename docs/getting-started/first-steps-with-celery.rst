.. _tut-celery:

========================
 First steps with Celery
========================

.. contents::
    :local:

.. _celerytut-broker:

Choosing a Broker
=================

Celery requires a solution to send and receive messages, usually this
comes in the form of a separate service called a *message broker*.

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

The first thing you need is a Celery instance, this is called the celery
application or just app.  Since this instance is used as
the entry-point for everything you want to do in Celery, like creating tasks and
managing workers, it must be possible for other modules to import it.

Some people create a dedicated module for it, but in this tutorial we will
keep everything in the same module.

Let's create the file :file:`tasks.py`:

.. code-block:: python

    from celery import Celery

    celery = Celery("tasks", broker="amqp://guest:guest@localhost:5672")

    @celery.task
    def add(x, y):
        return x + y

    if __name__ == "__main__":
        celery.start()

The first argument to :class:`~celery.app.Celery` is the name of the current module,
this is needed so that names can be automatically generated, the second
argument is the broker keyword argument which specifies the URL of the
message broker we want to use.

We defined a single task, called ``add``, which returns the sum of two numbers.

.. _celerytut-running-celeryd:

Running the celery worker server
================================

We can now run the worker by executing our program with the ``worker``
argument::

    $ python tasks.py worker --loglevel=INFO

In production you will probably want to run the worker in the
background as a daemon.  To do this you need to use the tools provided
by your platform, or something like `supervisord`_ (see :ref:`daemonizing`
for more information).

For a complete listing of the command line options available, do::

    $  python tasks.py worker --help

There also several other commands available, and similarly you can get a list
of these::

    $ python tasks.py --help

.. _`supervisord`: http://supervisord.org

.. _celerytut-executing-task:

Executing the task
==================

Whenever we want to execute our task, we use the
:meth:`~@Task.delay` method of the task.

This is a handy shortcut to the :meth:`~@Task.apply_async`
method which gives greater control of the task execution (see
:ref:`guide-executing`).

    >>> from tasks import add
    >>> add.delay(4, 4)

The task should now be executed by the worker you started earlier,
and you can verify that by looking at the workers console output.

Applying a task returns an :class:`~@AsyncResult` instance,
which can be used to check the state of the task, wait for the task to finish
or get its return value (or if the task failed, the exception and traceback).
But this isn't enabled by default, and you have to configure Celery to
use a result backend, which is detailed in the next section.

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
This time we'll hold on to the :class:`~@AsyncResult`::

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

If the task raises an exception, the return value of :meth:`~@AsyncResult.successful`
will be :const:`False`, and `result.result` will contain the exception instance
raised by the task.

.. _celerytut-configuration:

Configuration
-------------

Celery is very flexible and comes with many configuration options that
can be set on your app directly, or by using dedicated configuration files.

For example you can set the default value for the workers
``--concurrency`` argument, which is used to decide the number of pool worker
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

Where to go from here
=====================

After this you should read the :ref:`guide`. Specifically
:ref:`guide-tasks` and :ref:`guide-executing`.
