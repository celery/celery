=================================
 celery - Distributed Task Queue
=================================

:Version: 1.0.0-pre1
:Keywords: task queue, job queue, asynchronous, rabbitmq, amqp, redis.

--

Celery is a task queue/job queue based on distributed message passing.
It is focused on real-time operation, but has support for scheduling as well.

The execution units, called tasks, are executed concurrently on one or more
worker servers, asynchronously (in the background) or synchronously
(wait until ready).

Celery is already used in production to process millions of tasks a day.

It was first created for Django, but is now usable from Python as well.
It can also `operate with other languages via HTTP+JSON`_.

.. _`operate with other languages via HTTP+JSON`: http://bit.ly/CgXSc

Overview
========

This is a high level overview of the architecture.

.. image:: http://cloud.github.com/downloads/ask/celery/Celery-Overview-v4.jpg

The broker pushes tasks to the worker servers.
A worker server is a networked machine running ``celeryd``. This can be one or
more machines, depending on the workload.

The result of the task can be stored for later retrieval (called its
"tombstone").

Example
=======

You probably want to see some code by now, so I'll give you an example task
adding two numbers:
::

    from celery.decorators import task

    @task
    def add(x, y):
        return x + y

You can execute the task in the background, or wait for it to finish::

    >>> result = add.delay(4, 4)
    >>> result.wait() # wait for and return the result
    8

Simple!

Features
========

    * Uses messaging (AMQP: RabbitMQ, ZeroMQ, Qpid) to route tasks to the
      worker servers. Experimental support for STOMP (ActiveMQ) is also 
      available. For simple setups it's also possible to use Redis or an
      SQL database as the message queue.

    * You can run as many worker servers as you want, and still
      be *guaranteed that the task is only executed once.*

    * Tasks are executed *concurrently* using the Python 2.6
      ``multiprocessing`` module (also available as a back-port
      to older python versions)

    * Supports *periodic tasks*, which makes it a (better) replacement
      for cronjobs.

    * When a task has been executed, the return value can be stored using
      either a MySQL/Oracle/PostgreSQL/SQLite database, Memcached,
      `MongoDB`_, `Redis`_ or `Tokyo Tyrant`_ back-end. For high-performance
      you can also use AMQP messages to publish results.

    * Supports calling tasks over HTTP to support multiple programming
      languages and systems.

    * Supports several serialization schemes, like pickle, json, yaml and
      supports registering custom encodings .

    * If the task raises an exception, the exception instance is stored,
      instead of the return value, and it's possible to inspect the traceback
      after the fact.

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

    * However, you rarely want to wait for these results in a web-environment.
      You'd rather want to use Ajax to poll the task status, which is
      available from a URL like ``celery/<task_id>/status/``. This view
      returns a JSON-serialized data structure containing the task status,
      and the return value if completed, or exception on failure.

    * Pool workers are supervised, so if for some reason a worker crashes
        it is automatically replaced by a new worker.

    * Can be configured to send e-mails to the administrators when a task
      fails.

.. _`MongoDB`: http://www.mongodb.org/
.. _`Redis`: http://code.google.com/p/redis/
.. _`Tokyo Tyrant`: http://tokyocabinet.sourceforge.net/

Documentation
=============

The `latest documentation`_ with user guides, tutorials and API reference
is hosted at Github.

.. _`latest documentation`: http://ask.github.com/celery/

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

A look inside the components
============================

.. image:: http://cloud.github.com/downloads/ask/celery/Celery1.0-inside-worker.jpg



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

