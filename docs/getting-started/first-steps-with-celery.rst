.. _tut-celery:
.. _first-steps:

=========================
 First Steps with Celery
=========================

Celery is a task queue with batteries included.
It is easy to use so that you can get started without learning
the full complexities of the problem it solves. It is designed
around best practices so that your product can scale
and integrate with other languages, and it comes with the
tools and support you need to run such a system in production.

In this tutorial you will learn the absolute basics of using Celery.
You will learn about;

- Choosing and installing a message broker.
- Installing Celery and creating your first task
- Starting the worker and calling tasks.
- Keeping track of tasks as they transition through different states,
  and inspecting return values.

Celery may seem daunting at first - but don't worry - this tutorial
will get you started in no time. It is deliberately kept simple, so
to not confuse you with advanced features.
After you have finished this tutorial
it's a good idea to browse the rest of the documentation,
for example the :ref:`next-steps` tutorial, which will
showcase Celery's capabilities.

.. contents::
    :local:

.. _celerytut-broker:

Choosing a Broker
=================

Celery requires a solution to send and receive messages, usually this
comes in the form of a separate service called a *message broker*.

There are several choices available, including:

RabbitMQ
--------

`RabbitMQ`_ is feature-complete, stable, durable and easy to install.
It's an excellent choice for a production environment.
Detailed information about using RabbitMQ with Celery:

    :ref:`broker-rabbitmq`

.. _`RabbitMQ`: http://www.rabbitmq.com/

If you are using Ubuntu or Debian install RabbitMQ by executing this
command:

.. code-block:: bash

    $ sudo apt-get install rabbitmq-server

When the command completes the broker is already running in the background,
ready to move messages for you: ``Starting rabbitmq-server: SUCCESS``.

And don't worry if you're not running Ubuntu or Debian, you can go to this
website to find similarly simple installation instructions for other
platforms, including Microsoft Windows:

    http://www.rabbitmq.com/download.html


Redis
-----

`Redis`_ is also feature-complete, but is more susceptible to data loss in
the event of abrupt termination or power failures. Detailed information about using Redis:

    :ref:`broker-redis`

.. _`Redis`: http://redis.io/


Using a database
----------------

Using a database as a message queue is not recommended, but can be sufficient
for very small installations.  Your options include:

* :ref:`broker-sqlalchemy`
* :ref:`broker-django`

If you're already using a Django database for example, using it as your
message broker can be convenient while developing even if you use a more
robust system in production.

Other brokers
-------------

In addition to the above, there are other experimental transport implementations
to choose from, including :ref:`Amazon SQS <broker-sqs>`, :ref:`broker-mongodb`
and :ref:`IronMQ <broker-ironmq>`.

See :ref:`broker-overview`.

.. _celerytut-installation:

Installing Celery
=================

Celery is on the Python Package Index (PyPI), so it can be installed
with standard Python tools like ``pip`` or ``easy_install``:

.. code-block:: bash

    $ pip install celery

Application
===========

The first thing you need is a Celery instance, this is called the celery
application or just app in short.  Since this instance is used as
the entry-point for everything you want to do in Celery, like creating tasks and
managing workers, it must be possible for other modules to import it.

In this tutorial you will keep everything contained in a single module,
but for larger projects you want to create
a :ref:`dedicated module <project-layout>`.

Let's create the file :file:`tasks.py`:

.. code-block:: python

    from celery import Celery

    celery = Celery('tasks', broker='amqp://guest@localhost//')

    @celery.task
    def add(x, y):
        return x + y

The first argument to :class:`~celery.app.Celery` is the name of the current module,
this is needed so that names can be automatically generated, the second
argument is the broker keyword argument which specifies the URL of the
message broker you want to use, using RabbitMQ here, which is already the
default option.  See :ref:`celerytut-broker` above for more choices,
e.g. for Redis you can use ``redis://localhost``, or MongoDB:
``mongodb://localhost``.

You defined a single task, called ``add``, which returns the sum of two numbers.

.. _celerytut-running-celeryd:

Running the celery worker server
================================

You now run the worker by executing our program with the ``worker``
argument:

.. code-block:: bash

    $ celery -A tasks worker --loglevel=info

In production you will want to run the worker in the
background as a daemon.  To do this you need to use the tools provided
by your platform, or something like `supervisord`_ (see :ref:`daemonizing`
for more information).

For a complete listing of the command line options available, do:

.. code-block:: bash

    $  celery worker --help

There also several other commands available, and help is also available:

.. code-block:: bash

    $ celery help

.. _`supervisord`: http://supervisord.org

.. _celerytut-calling:

Calling the task
================

To call our task you can use the :meth:`~@Task.delay` method.

This is a handy shortcut to the :meth:`~@Task.apply_async`
method which gives greater control of the task execution (see
:ref:`guide-calling`)::

    >>> from tasks import add
    >>> add.delay(4, 4)

The task has now been processed by the worker you started earlier,
and you can verify that by looking at the workers console output.

Calling a task returns an :class:`~@AsyncResult` instance,
which can be used to check the state of the task, wait for the task to finish
or get its return value (or if the task failed, the exception and traceback).
But this isn't enabled by default, and you have to configure Celery to
use a result backend, which is detailed in the next section.

.. _celerytut-keeping-results:

Keeping Results
===============

If you want to keep track of the tasks' states, Celery needs to store or send
the states somewhere.  There are several
built-in result backends to choose from: `SQLAlchemy`_/`Django`_ ORM,
`Memcached`_, `Redis`_, AMQP (`RabbitMQ`_), and `MongoDB`_ -- or you can define your own.

.. _`Memcached`: http://memcached.org
.. _`MongoDB`: http://www.mongodb.org
.. _`SQLAlchemy`: http://www.sqlalchemy.org/
.. _`Django`: http://djangoproject.com

For this example you will use the `amqp` result backend, which sends states
as messages.  The backend is specified via the ``backend`` argument to
:class:`@Celery`, (or via the :setting:`CELERY_RESULT_BACKEND` setting if
you choose to use a configuration module)::

    celery = Celery('tasks', backend='amqp', broker='amqp://')

or if you want to use Redis as the result backend, but still use RabbitMQ as
the message broker (a popular combination)::

    celery = Celery('tasks', backend='redis://localhost', broker='amqp://')

To read more about result backends please see :ref:`task-result-backends`.

Now with the result backend configured, let's call the task again.
This time you'll hold on to the :class:`~@AsyncResult` instance returned
when you call a task::

    >>> result = add.delay(4, 4)

The :meth:`~@AsyncResult.ready` method returns whether the task
has finished processing or not::

    >>> result.ready()
    False

You can wait for the result to complete, but this is rarely used
since it turns the asynchronous call into a synchronous one::

    >>> result.get(timeout=1)
    8

In case the task raised an exception, :meth:`~@AsyncResult.get` will
re-raise the exception, but you can override this by specifying
the ``propagate`` argument::

    >>> result.get(propagate=True)


If the task raised an exception you can also gain access to the
original traceback::

    >>> result.traceback
    ...

See :mod:`celery.result` for the complete result object reference.

.. _celerytut-configuration:

Configuration
=============

Celery, like a consumer appliance doesn't need much to be operated.
It has an input and an output, where you must connect the input to a broker and maybe
the output to a result backend if so wanted.  But if you look closely at the back
there's a lid revealing loads of sliders, dials and buttons: this is the configuration.

The default configuration should be good enough for most uses, but there's
many things to tweak so Celery works just the way you want it to.
Reading about the options available is a good idea to get familiar with what
can be configured. You can read about the options in the the
:ref:`configuration` reference.

The configuration can be set on the app directly or by using a dedicated
configuration module.
As an example you can configure the default serializer used for serializing
task payloads by changing the :setting:`CELERY_TASK_SERIALIZER` setting:

.. code-block:: python

    celery.conf.CELERY_TASK_SERIALIZER = 'json'

If you are configuring many settings at once you can use ``update``:

.. code-block:: python

    celery.conf.update(
        CELERY_TASK_SERIALIZER='json',
        CELERY_RESULT_SERIALIZER='json',
        CELERY_TIMEZONE='Europe/Oslo',
        CELERY_ENABLE_UTC=True,
    )

For larger projects using a dedicated configuration module is useful,
in fact you are discouraged from hard coding
periodic task intervals and task routing options, as it is much
better to keep this in a centralized location, and especially for libraries
it makes it possible for users to control how they want your tasks to behave,
you can also imagine your SysAdmin making simple changes to the configuration
in the event of system trouble.

You can tell your Celery instance to use a configuration module,
by calling the :meth:`~@Celery.config_from_object` method:

.. code-block:: python

    celery.config_from_object('celeryconfig')

This module is often called "``celeryconfig``", but you can use any
module name.

A module named ``celeryconfig.py`` must then be available to load from the
current directory or on the Python path, it could look like this:

:file:`celeryconfig.py`:

.. code-block:: python

    BROKER_URL = 'amqp://'
    CELERY_RESULT_BACKEND = 'amqp://'

    CELERY_TASK_SERIALIZER = 'json'
    CELERY_RESULT_SERIALIZER = 'json'
    CELERY_TIMEZONE = 'Europe/Oslo'
    CELERY_ENABLE_UTC = True

To verify that your configuration file works properly, and doesn't
contain any syntax errors, you can try to import it:

.. code-block:: bash

    $ python -m celeryconfig

For a complete reference of configuration options, see :ref:`configuration`.

To demonstrate the power of configuration files, this how you would
route a misbehaving task to a dedicated queue:

:file:`celeryconfig.py`:

.. code-block:: python

    CELERY_ROUTES = {
        'tasks.add': 'low-priority',
    }

Or instead of routing it you could rate limit the task
instead, so that only 10 tasks of this type can be processed in a minute
(10/m):

:file:`celeryconfig.py`:

.. code-block:: python

    CELERY_ANNOTATIONS = {
        'tasks.add': {'rate_limit': '10/m'}
    }

If you are using RabbitMQ, Redis or MongoDB as the
broker then you can also direct the workers to set a new rate limit
for the task at runtime:

.. code-block:: bash

    $ celery control rate_limit tasks.add 10/m
    worker.example.com: OK
        new rate limit set successfully

See :ref:`guide-routing` to read more about task routing,
and the :setting:`CELERY_ANNOTATIONS` setting for more about annotations,
or :ref:`guide-monitoring` for more about remote control commands,
and how to monitor what your workers are doing.

Where to go from here
=====================

If you want to learn more you should continue to the
:ref:`Next Steps <next-steps>` tutorial, and after that you
can study the :ref:`User Guide <guide>`.
