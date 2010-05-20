=================================
 celery - Distributed Task Queue
=================================

.. image:: http://cloud.github.com/downloads/ask/celery/celery_favicon_128.png

:Version: 1.1.0
:Web: http://celeryproject.org/
:Download: http://pypi.python.org/pypi/celery/
:Source: http://github.com/ask/celery/
:Keywords: task queue, job queue, asynchronous, rabbitmq, amqp, redis,
  python, webhooks, queue, distributed

--

Celery is a task queue/job queue based on distributed message passing.
It is focused on real-time operation, but supports scheduling as well.

The execution units, called tasks, are executed concurrently on a single or
more worker servers. Tasks can execute asynchronously (in the background) or synchronously
(wait until ready).

Celery is already used in production to process millions of tasks a day.

Celery was originally created for use with Django, but this is no longer
the case. If you want to use Celery in a Django project you need to use
`django-celery`_. It can also `operate with other languages via webhooks`_.

The recommended message broker is `RabbitMQ`_, but support for Redis and
databases is also available.

.. _`django-celery`: http://pypi.python.org/pypi/django-celery
.. _`operate with other languages via webhooks`:
    http://ask.github.com/celery/userguide/remote-tasks.html

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

You probably want to see some code by now, so here's an example task
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

    +-----------------+----------------------------------------------------+
    | Messaging       | Supported brokers include `RabbitMQ`_, `Stomp`_,   |
    |                 | `Redis`_, and most common SQL databases.           |
    +-----------------+----------------------------------------------------+
    | Robust          | Using `RabbitMQ`, celery survives most error       |
    |                 | scenarios, and your tasks will never be lost.      |
    +-----------------+----------------------------------------------------+
    | Distributed     | Runs on one or more machines. Supports             |
    |                 | `clustering`_ when used in combination with        |
    |                 | `RabbitMQ`_. You can set up new workers without    |
    |                 | central configuration (e.g. use your dads laptop   |
    |                 | while the queue is temporarily overloaded).        |
    +-----------------+----------------------------------------------------+
    | Concurrency     | Tasks are executed in parallel using the           |
    |                 | ``multiprocessing`` module.                        |
    +-----------------+----------------------------------------------------+
    | Scheduling      | Supports recurring tasks like cron, or specifying  |
    |                 | an exact date or countdown for when after the task |
    |                 | should be executed.                                |
    +-----------------+----------------------------------------------------+
    | Performance     | Able to execute tasks while the user waits.        |
    +-----------------+----------------------------------------------------+
    | Return Values   | Task return values can be saved to the selected    |
    |                 | result store backend. You can wait for the result, |
    |                 | retrieve it later, or ignore it.                   |
    +-----------------+----------------------------------------------------+
    | Result Stores   | Database, `MongoDB`_, `Redis`_, `Tokyo Tyrant`,    |
    |                 | `AMQP`_ (high performance).                        |
    +-----------------+----------------------------------------------------+
    | Webhooks        | Your tasks can also be HTTP callbacks, enabling    |
    |                 | cross-language communication.                      |
    +-----------------+----------------------------------------------------+
    | Rate limiting   | Supports rate limiting by using the token bucket   |
    |                 | algorithm, which accounts for bursts of traffic.   |
    |                 | Rate limits can be set for each task type, or      |
    |                 | globally for all.                                  |
    +-----------------+----------------------------------------------------+
    | Routing         | Using AMQP you can route tasks arbitrarily to      |
    |                 | different workers.                                 |
    +-----------------+----------------------------------------------------+
    | Remote-control  | You can rate limit and delete (revoke) tasks       |
    |                 | remotely.                                          |
    +-----------------+----------------------------------------------------+
    | Monitoring      | You can capture everything happening with the      |
    |                 | workers in real-time by subscribing to events.     |
    |                 | A real-time web monitor is in development.         |
    +-----------------+----------------------------------------------------+
    | Serialization   | Supports Pickle, JSON, YAML, or easily defined     |
    |                 | custom schemes. One task invocation can have a     |
    |                 | different scheme than another.                     |
    +-----------------+----------------------------------------------------+
    | Tracebacks      | Errors and tracebacks are stored and can be        |
    |                 | investigated after the fact.                       |
    +-----------------+----------------------------------------------------+
    | UUID            | Every task has an UUID (Universally Unique         |
    |                 | Identifier), which is the task id used to query    |
    |                 | task status and return value.                      |
    +-----------------+----------------------------------------------------+
    | Retries         | Tasks can be retried if they fail, with            |
    |                 | configurable maximum number of retries, and delays |
    |                 | between each retry.                                |
    +-----------------+----------------------------------------------------+
    | Task Sets       | A Task set is a task consisting of several         |
    |                 | sub-tasks. You can find out how many, or if all    |
    |                 | of the sub-tasks has been executed, and even       |
    |                 | retrieve the results in order. Progress bars,      |
    |                 | anyone?                                            |
    +-----------------+----------------------------------------------------+
    | Made for Web    | You can query status and results via URLs,         |
    |                 | enabling the ability to poll task status using     |
    |                 | Ajax.                                              |
    +-----------------+----------------------------------------------------+
    | Error e-mails   | Can be configured to send e-mails to the           |
    |                 | administrators when tasks fails.                   |
    +-----------------+----------------------------------------------------+
    | Supervised      | Pool workers are supervised and automatically      |
    |                 | replaced if they crash.                            |
    +-----------------+----------------------------------------------------+


.. _`RabbitMQ`: http://www.rabbitmq.com/
.. _`clustering`: http://www.rabbitmq.com/clustering.html
.. _`AMQP`: http://www.amqp.org/
.. _`Stomp`: http://stomp.codehaus.org/
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

Wiki
====

http://wiki.github.com/ask/celery/

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

