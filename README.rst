=================================
 celery - Distributed Task Queue
=================================

:Version: 0.7.99

Introduction
============

Celery is a distributed task queue.

It was first created for Django, but is now usable from Python.
It can also operate with other languages via HTTP+JSON.

This introduction is written for someone who wants to use
Celery from within a Django project. For information about using it from
pure Python see `Can I use Celery without Django?`_, for calling out to other
languages see `Executing tasks on a remote web server`_.

.. _`Can I use Celery without Django?`: http://bit.ly/WPa6n

.. _`Executing tasks on a remote web server`: http://bit.ly/CgXSc

It is used for executing tasks *asynchronously*, routed to one or more
worker servers, running concurrently using multiprocessing.

It is designed to solve certain problems related to running websites
demanding high-availability and performance.

It is perfect for filling caches, posting updates to twitter, mass
downloading data like syndication feeds or web scraping. Use-cases are
plentiful. Implementing these features asynchronously using ``celery`` is
easy and fun, and the performance improvements can make it more than
worthwhile.

Overview
========

This is a high level overview of the architecture.

.. image:: http://cloud.github.com/downloads/ask/celery/Celery-Overview-v4.jpg

The broker is an AMQP server pushing tasks to the worker servers.
A worker server is a networked machine running ``celeryd``. This can be one or
more machines, depending on the workload. See `A look inside the worker`_ to
see how the worker server works.

The result of the task can be stored for later retrieval (called its
"tombstone").

Features
========

    * Uses AMQP messaging (RabbitMQ, ZeroMQ, Qpid) to route tasks to the
      worker servers. Experimental support for STOMP (ActiveMQ) is also 
      available.

    * You can run as many worker servers as you want, and still
      be *guaranteed that the task is only executed once.*

    * Tasks are executed *concurrently* using the Python 2.6
      ``multiprocessing`` module (also available as a back-port
      to older python versions)

    * Supports *periodic tasks*, which makes it a (better) replacement
      for cronjobs.

    * When a task has been executed, the return value can be stored using
      either a MySQL/Oracle/PostgreSQL/SQLite database, Memcached,
      `MongoDB`_ or `Tokyo Tyrant`_ back-end. For high-performance you can
      also use AMQP messages to publish results.

    * If the task raises an exception, the exception instance is stored,
      instead of the return value.

    * All tasks has a Universally Unique Identifier (UUID), which is the
      task id, used for querying task status and return values.

    * Tasks can be retried if they fail, with a configurable maximum number
      of retries.

    * Tasks can be configured to run at a specific time and date in the
      future (ETA) or you can set a countdown in seconds for when the
      task should be executed.

    * Supports *task-sets*, which is a task consisting of several sub-tasks.
      You can find out how many, or if all of the sub-tasks has been executed.
      Excellent for progress-bar like functionality.

    * Has a ``map`` like function that uses tasks, called ``dmap``.

    * However, you rarely want to wait for these results in a web-environment.
      You'd rather want to use Ajax to poll the task status, which is
      available from a URL like ``celery/<task_id>/status/``. This view
      returns a JSON-serialized data structure containing the task status,
      and the return value if completed, or exception on failure.

    * The worker can collect statistics, like, how many tasks has been
      executed by type, and the time it took to process them. Very useful
      for monitoring and profiling.

    * Pool workers are supervised, so if for some reason a worker crashes
        it is automatically replaced by a new worker.

    * Can be configured to send e-mails to the administrators when a task
      fails.

.. _`MongoDB`: http://www.mongodb.org/
.. _`Tokyo Tyrant`: http://tokyocabinet.sourceforge.net/

API Reference Documentation
===========================

The `API Reference`_ is hosted at Github
(http://ask.github.com/celery)

.. _`API Reference`: http://ask.github.com/celery/

Installation
=============

You can install ``celery`` either via the Python Package Index (PyPI)
or from source.

To install using ``pip``,::

    $ pip install celery

To install using ``easy_install``,::

    $ easy_install celery

Downloading and installing from source
--------------------------------------

Download the latest version of ``celery`` from
http://pypi.python.org/pypi/celery/

You can install it by doing the following,::

    $ tar xvfz celery-0.0.0.tar.gz
    $ cd celery-0.0.0
    $ python setup.py build
    # python setup.py install # as root

Using the development version
------------------------------

You can clone the repository by doing the following::

    $ git clone git://github.com/ask/celery.git


Usage
=====

Installing RabbitMQ
-------------------

See `Installing RabbitMQ`_ over at RabbitMQ's website. For Mac OS X
see `Installing RabbitMQ on OS X`_.

.. _`Installing RabbitMQ`: http://www.rabbitmq.com/install.html
.. _`Installing RabbitMQ on OS X`:
    http://playtype.net/past/2008/10/9/installing_rabbitmq_on_osx/


Setting up RabbitMQ
-------------------

To use celery we need to create a RabbitMQ user, a virtual host and
allow that user access to that virtual host::

    $ rabbitmqctl add_user myuser mypassword

    $ rabbitmqctl add_vhost myvhost

From RabbitMQ version 1.6.0 and onward you have to use the new ACL features
to allow access::

    $ rabbitmqctl set_permissions -p myvhost myuser "" ".*" ".*"

See the RabbitMQ `Admin Guide`_ for more information about `access control`_.

.. _`Admin Guide`: http://www.rabbitmq.com/admin-guide.html

.. _`access control`: http://www.rabbitmq.com/admin-guide.html#access-control


If you are still using version 1.5.0 or below, please use ``map_user_vhost``::

    $ rabbitmqctl map_user_vhost myuser myvhost


Configuring your Django project to use Celery
---------------------------------------------

You only need three simple steps to use celery with your Django project.

    1. Add ``celery`` to ``INSTALLED_APPS``.

    2. Create the celery database tables::

            $ python manage.py syncdb

    3. Configure celery to use the AMQP user and virtual host we created
        before, by adding the following to your ``settings.py``::

            AMQP_SERVER = "localhost"
            AMQP_PORT = 5672
            AMQP_USER = "myuser"
            AMQP_PASSWORD = "mypassword"
            AMQP_VHOST = "myvhost"


That's it.

There are more options available, like how many processes you want to process
work in parallel (the ``CELERY_CONCURRENCY`` setting), and the backend used
for storing task statuses. But for now, this should do. For all of the options
available, please consult the `API Reference`_

**Note**: If you're using SQLite as the Django database back-end,
``celeryd`` will only be able to process one task at a time, this is
because SQLite doesn't allow concurrent writes.

Running the celery worker server
--------------------------------

To test this we'll be running the worker server in the foreground, so we can
see what's going on without consulting the logfile::

    $ python manage.py celeryd


However, in production you probably want to run the worker in the
background, as a daemon:: 

    $ python manage.py celeryd --detach


For a complete listing of the command line arguments available, with a short
description, you can use the help command::

    $ python manage.py help celeryd


Defining and executing tasks
----------------------------

**Please note** All of these tasks has to be stored in a real module, they can't
be defined in the python shell or ipython/bpython. This is because the celery
worker server needs access to the task function to be able to run it.
So while it looks like we use the python shell to define the tasks in these
examples, you can't do it this way. Put them in the ``tasks`` module of your
Django application. The worker server will automatically load any ``tasks.py``
file for all of the applications listed in ``settings.INSTALLED_APPS``.
Executing tasks using ``delay`` and ``apply_async`` can be done from the
python shell, but keep in mind that since arguments are pickled, you can't
use custom classes defined in the shell session.

While you can use regular functions, the recommended way is to define
a task class. This way you can cleanly upgrade the task to use the more
advanced features of celery later.

This is a task that basically does nothing but take some arguments,
and return a value:

    >>> from celery.task import Task
    >>> from celery.registry import tasks
    >>> class MyTask(Task):
    ...     def run(self, some_arg, **kwargs):
    ...         logger = self.get_logger(**kwargs)
    ...         logger.info("Did something: %s" % some_arg)
    ...         return 42
    >>> tasks.register(MyTask)

As you can see the worker is sending some keyword arguments to this task,
this is the default keyword arguments. A task can choose not to take these,
or only list the ones it want (the worker will do the right thing).
The current default keyword arguments are:

    * logfile

        The currently used log file, can be passed on to ``self.get_logger``
        to gain access to the workers log file via a ``logger.Logging``
        instance.

    * loglevel

        The current loglevel used.

    * task_id

        The unique id of the executing task.

    * task_name

        Name of the executing task.

    * task_retries

        How many times the current task has been retried.
        (an integer starting a ``0``).

Now if we want to execute this task, we can use the ``delay`` method of the
task class (this is a handy shortcut to the ``apply_async`` method which gives
you greater control of the task execution).

    >>> from myapp.tasks import MyTask
    >>> MyTask.delay(some_arg="foo")

At this point, the task has been sent to the message broker. The message
broker will hold on to the task until a celery worker server has successfully
picked it up.

*Note* If everything is just hanging when you execute ``delay``, please check
that RabbitMQ is running, and that the user/password has access to the virtual
host you configured earlier.

Right now we have to check the celery worker logfiles to know what happened with
the task. This is because we didn't keep the ``AsyncResult`` object returned
by ``delay``.

The ``AsyncResult`` lets us find the state of the task, wait for the task to
finish and get its return value (or exception if the task failed).

So, let's execute the task again, but this time we'll keep track of the task:

    >>> result = MyTask.delay("do_something", some_arg="foo bar baz")
    >>> result.ready() # returns True if the task has finished processing.
    False
    >>> result.result # task is not ready, so no return value yet.
    None
    >>> result.get()   # Waits until the task is done and return the retval.
    42
    >>> result.result
    42
    >>> result.successful() # returns True if the task didn't end in failure.
    True


If the task raises an exception, the ``result.success()`` will be ``False``,
and ``result.result`` will contain the exception instance raised.

Auto-discovery of tasks
-----------------------

``celery`` has an auto-discovery feature like the Django Admin, that
automatically loads any ``tasks.py`` module in the applications listed
in ``settings.INSTALLED_APPS``. This autodiscovery is used by the celery
worker to find registered tasks for your Django project.

Periodic Tasks
---------------

Periodic tasks are tasks that are run every ``n`` seconds. 
Here's an example of a periodic task:

    >>> from celery.task import PeriodicTask
    >>> from celery.registry import tasks
    >>> from datetime import timedelta
    >>> class MyPeriodicTask(PeriodicTask):
    ...     run_every = timedelta(seconds=30)
    ...
    ...     def run(self, **kwargs):
    ...         logger = self.get_logger(**kwargs)
    ...         logger.info("Running periodic task!")
    ...
    >>> tasks.register(MyPeriodicTask)

**Note:** Periodic tasks does not support arguments, as this doesn't
really make sense.


A look inside the worker
========================

.. image:: http://cloud.github.com/downloads/ask/celery/InsideTheWorker-v2.jpg

Getting Help
============

Mailing list
------------

For discussions about the usage, development, and future of celery,
please join the `celery-users`_ mailing list. 

.. _`celery-users`: http://groups.google.com/group/celery-users/

IRC
---

Come chat with us on IRC. The `#celery`_ channel is located at the `Freenode`_
network.

.. _`#celery`: irc://irc.freenode.net/celery
.. _`Freenode`: http://freenode.net


Bug tracker
===========

If you have any suggestions, bug reports or annoyances please report them
to our issue tracker at http://github.com/ask/celery/issues/

Contributing
============

Development of ``celery`` happens at Github: http://github.com/ask/celery

You are highly encouraged to participate in the development
of ``celery``. If you don't like Github (for some reason) you're welcome
to send regular patches.

License
=======

This software is licensed under the ``New BSD License``. See the ``LICENSE``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround
