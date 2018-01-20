.. _intro:

========================
 Introduction to Celery
========================

.. contents::
    :local:
    :depth: 1

What's a Task Queue?
====================

Task queues are used as a mechanism to distribute work across threads or
machines.

A task queue's input is a unit of work called a task. Dedicated worker
processes constantly monitor task queues for new work to perform.

Celery communicates via messages, usually using a broker
to mediate between clients and workers. To initiate a task the client adds a
message to the queue, the broker then delivers that message to a worker.

A Celery system can consist of multiple workers and brokers, giving way
to high availability and horizontal scaling.

Celery is written in Python, but the protocol can be implemented in any
language. In addition to Python there's node-celery_ for Node.js,
and a `PHP client`_.

Language interoperability can also be achieved
exposing an HTTP endpoint and having a task that requests it (webhooks).

.. _`PHP client`: https://github.com/gjedeer/celery-php
.. _node-celery: https://github.com/mher/node-celery

What do I need?
===============

.. sidebar:: Version Requirements
    :subtitle: Celery version 4.0 runs on

    - Python ❨2.7, 3.4, 3.5❩
    - PyPy ❨5.4, 5.5❩

    This is the last version to support Python 2.7,
    and from the next version (Celery 5.x) Python 3.5 or newer is required.

    If you're running an older version of Python, you need to be running
    an older version of Celery:

    - Python 2.6: Celery series 3.1 or earlier.
    - Python 2.5: Celery series 3.0 or earlier.
    - Python 2.4 was Celery series 2.2 or earlier.

    Celery is a project with minimal funding,
    so we don't support Microsoft Windows.
    Please don't open any issues related to that platform.

*Celery* requires a message transport to send and receive messages.
The RabbitMQ and Redis broker transports are feature complete,
but there's also support for a myriad of other experimental solutions, including
using SQLite for local development.

*Celery* can run on a single machine, on multiple machines, or even
across data centers.

Get Started
===========

If this is the first time you're trying to use Celery, or if you haven't
kept up with development in the 3.1 version and are coming from previous versions,
then you should read our getting started tutorials:

- :ref:`first-steps`
- :ref:`next-steps`

Celery is…
==========

.. _`mailing-list`: https://groups.google.com/group/celery-users

.. topic:: \

    - **Simple**

        Celery is easy to use and maintain, and it *doesn't need configuration files*.

        It has an active, friendly community you can talk to for support,
        including a `mailing-list`_ and an :ref:`IRC channel <irc-channel>`.

        Here's one of the simplest applications you can make:

        .. code-block:: python

            from celery import Celery

            app = Celery('hello', broker='amqp://guest@localhost//')

            @app.task
            def hello():
                return 'hello world'

    - **Highly Available**

        Workers and clients will automatically retry in the event
        of connection loss or failure, and some brokers support
        HA in way of *Primary/Primary* or *Primary/Replica* replication.

    - **Fast**

        A single Celery process can process millions of tasks a minute,
        with sub-millisecond round-trip latency (using RabbitMQ,
        librabbitmq, and optimized settings).

    - **Flexible**

        Almost every part of *Celery* can be extended or used on its own,
        Custom pool implementations, serializers, compression schemes, logging,
        schedulers, consumers, producers, broker transports, and much more.


.. topic:: It supports

    .. hlist::
        :columns: 2

        - **Brokers**

            - :ref:`RabbitMQ <broker-rabbitmq>`, :ref:`Redis <broker-redis>`,
            - :ref:`Amazon SQS <broker-sqs>`, and more…

        - **Concurrency**

            - prefork (multiprocessing),
            - Eventlet_, gevent_
            - `solo` (single threaded)

        - **Result Stores**

            - AMQP, Redis
            - Memcached,
            - SQLAlchemy, Django ORM
            - Apache Cassandra, Elasticsearch

        - **Serialization**

            - *pickle*, *json*, *yaml*, *msgpack*.
            - *zlib*, *bzip2* compression.
            - Cryptographic message signing.

Features
========

.. topic:: \

    .. hlist::
        :columns: 2

        - **Monitoring**

            A stream of monitoring events is emitted by workers and
            is used by built-in and external tools to tell you what
            your cluster is doing -- in real-time.

            :ref:`Read more… <guide-monitoring>`.

        - **Work-flows**

            Simple and complex work-flows can be composed using
            a set of powerful primitives we call the "canvas",
            including grouping, chaining, chunking, and more.

            :ref:`Read more… <guide-canvas>`.

        - **Time & Rate Limits**

            You can control how many tasks can be executed per second/minute/hour,
            or how long a task can be allowed to run, and this can be set as
            a default, for a specific worker or individually for each task type.

            :ref:`Read more… <worker-time-limits>`.

        - **Scheduling**

            You can specify the time to run a task in seconds or a
            :class:`~datetime.datetime`, or you can use
            periodic tasks for recurring events based on a
            simple interval, or Crontab expressions
            supporting minute, hour, day of week, day of month, and
            month of year.

            :ref:`Read more… <guide-beat>`.

        - **Resource Leak Protection**

            The :option:`--max-tasks-per-child <celery worker --max-tasks-per-child>`
            option is used for user tasks leaking resources, like memory or
            file descriptors, that are simply out of your control.

            :ref:`Read more… <worker-max-tasks-per-child>`.

        - **User Components**

            Each worker component can be customized, and additional components
            can be defined by the user. The worker is built up using "bootsteps" — a
            dependency graph enabling fine grained control of the worker's
            internals.

.. _`Eventlet`: http://eventlet.net/
.. _`gevent`: http://gevent.org/

Framework Integration
=====================

Celery is easy to integrate with web frameworks, some of them even have
integration packages:

    +--------------------+------------------------+
    | `Pyramid`_         | :pypi:`pyramid_celery` |
    +--------------------+------------------------+
    | `Pylons`_          | :pypi:`celery-pylons`  |
    +--------------------+------------------------+
    | `Flask`_           | not needed             |
    +--------------------+------------------------+
    | `web2py`_          | :pypi:`web2py-celery`  |
    +--------------------+------------------------+
    | `Tornado`_         | :pypi:`tornado-celery` |
    +--------------------+------------------------+
    | `Tryton`_          | :pypi:`celery_tryton`  |
    +--------------------+------------------------+

For `Django`_ see :ref:`django-first-steps`.

The integration packages aren't strictly necessary, but they can make
development easier, and sometimes they add important hooks like closing
database connections at :manpage:`fork(2)`.

.. _`Django`: https://djangoproject.com/
.. _`Pylons`: http://pylonshq.com/
.. _`Flask`: http://flask.pocoo.org/
.. _`web2py`: http://web2py.com/
.. _`Bottle`: https://bottlepy.org/
.. _`Pyramid`: http://docs.pylonsproject.org/en/latest/docs/pyramid.html
.. _`Tornado`: http://www.tornadoweb.org/
.. _`Tryton`: http://www.tryton.org/
.. _`tornado-celery`: https://github.com/mher/tornado-celery/

Quick Jump
==========

.. topic:: I want to ⟶

    .. hlist::
        :columns: 2

        - :ref:`get the return value of a task <task-states>`
        - :ref:`use logging from my task <task-logging>`
        - :ref:`learn about best practices <task-best-practices>`
        - :ref:`create a custom task base class <task-custom-classes>`
        - :ref:`add a callback to a group of tasks <canvas-chord>`
        - :ref:`split a task into several chunks <canvas-chunks>`
        - :ref:`optimize the worker <guide-optimizing>`
        - :ref:`see a list of built-in task states <task-builtin-states>`
        - :ref:`create custom task states <custom-states>`
        - :ref:`set a custom task name <task-names>`
        - :ref:`track when a task starts <task-track-started>`
        - :ref:`retry a task when it fails <task-retry>`
        - :ref:`get the id of the current task <task-request-info>`
        - :ref:`know what queue a task was delivered to <task-request-info>`
        - :ref:`see a list of running workers <monitoring-control>`
        - :ref:`purge all messages <monitoring-control>`
        - :ref:`inspect what the workers are doing <monitoring-control>`
        - :ref:`see what tasks a worker has registered <monitoring-control>`
        - :ref:`migrate tasks to a new broker <monitoring-control>`
        - :ref:`see a list of event message types <event-reference>`
        - :ref:`contribute to Celery <contributing>`
        - :ref:`learn about available configuration settings <configuration>`
        - :ref:`get a list of people and companies using Celery <res-using-celery>`
        - :ref:`write my own remote control command <worker-custom-control-commands>`
        - :ref:`change worker queues at runtime <worker-queues>`

.. topic:: Jump to ⟶

    .. hlist::
        :columns: 4

        - :ref:`Brokers <brokers>`
        - :ref:`Applications <guide-app>`
        - :ref:`Tasks <guide-tasks>`
        - :ref:`Calling <guide-calling>`
        - :ref:`Workers <guide-workers>`
        - :ref:`Daemonizing <daemonizing>`
        - :ref:`Monitoring <guide-monitoring>`
        - :ref:`Optimizing <guide-optimizing>`
        - :ref:`Security <guide-security>`
        - :ref:`Routing <guide-routing>`
        - :ref:`Configuration <configuration>`
        - :ref:`Django <django>`
        - :ref:`Contributing <contributing>`
        - :ref:`Signals <signals>`
        - :ref:`FAQ <faq>`
        - :ref:`API Reference <apiref>`

.. include:: ../includes/installation.txt
