.. _intro:

========================
 Introduction to Celery
========================

.. contents::
    :local:
    :depth: 1

What is a Task Queue?
=====================

Task queues are used as a mechanism to distribute work across threads or
machines.

A task queue's input is a unit of work, called a task, dedicated worker
processes then constantly monitor the queue for new work to perform.

Celery communicates via messages using a broker
to mediate between clients and workers.  To initiate a task a client puts a
message on the queue, the broker then delivers the message to a worker.

A Celery system can consist of multiple workers and brokers, giving way
to high availability and horizontal scaling.

Celery is written in Python, but the protocol can be implemented in any
language.  So far there's RCelery_ for the Ruby programming language, and a
`PHP client`, but language interoperability can also be achieved
by :ref:`using webhooks <guide-webhooks>`.

.. _RCelery: http://leapfrogdevelopment.github.com/rcelery/
.. _`PHP client`: https://github.com/gjedeer/celery-php

What do I need?
===============

.. sidebar:: Version Requirements
    :subtitle: Celery version 3.0 runs on

    - Python ❨2.5, 2.6, 2.7, 3.2, 3.3❩
    - PyPy ❨1.8, 1.9❩
    - Jython ❨2.5, 2.7❩.

    This is the last version to support Python 2.5,
    and from the next version Python 2.6 or newer is required.
    The last version to support Python 2.4 was Celery series 2.2.

*Celery* requires a message broker to send and receive messages.
The RabbitMQ, Redis and MongoDB broker transports are feature complete,
but there's also support for a myriad of other solutions, including
using SQLite for local development.

*Celery* can run on a single machine, on multiple machines, or even
across data centers.

Get Started
===========

If this is the first time you're trying to use Celery, or you are
new to Celery 3.0 coming from previous versions then you should read our
getting started tutorials:

- :ref:`first-steps`
- :ref:`next-steps`

Celery is…
==========

.. _`mailing-list`: http://groups.google.com/group/celery-users

.. topic:: \ 

    - **Simple**

        Celery is easy to use and maintain, and it *doesn't need configuration files*.

        It has an active, friendly community you can talk to for support,
        including a `mailing-list`_ and an :ref:`IRC channel <irc-channel>`.

        Here's one of the simplest applications you can make:

        .. code-block:: python

            from celery import Celery

            celery = Celery('hello', broker='amqp://guest@localhost//')

            @celery.task
            def hello():
                return 'hello world'

    - **Highly Available**

        Workers and clients will automatically retry in the event
        of connection loss or failure, and some brokers support
        HA in way of *Master/Master* or *Master/Slave* replication.

    - **Fast**

        A single Celery process can process millions of tasks a minute,
        with sub-millisecond round-trip latency (using RabbitMQ,
        py-librabbitmq, and optimized settings).

    - **Flexible**

        Almost every part of *Celery* can be extended or used on its own,
        Custom pool implementations, serializers, compression schemes, logging,
        schedulers, consumers, producers, autoscalers, broker transports and much more.


.. topic:: It supports

    .. hlist::
        :columns: 2

        - **Brokers**

            - :ref:`RabbitMQ <broker-rabbitmq>`, :ref:`Redis <broker-redis>`,
            - :ref:`MongoDB <broker-mongodb>`, :ref:`Beanstalk <broker-beanstalk>`
            - :ref:`CouchDB <broker-couchdb>`, :ref:`SQLAlchemy <broker-sqlalchemy>`
            - :ref:`Django ORM <broker-django>`, :ref:`Amazon SQS <broker-sqs>`,
            - and more…

        - **Concurrency**

            - multiprocessing,
            - Eventlet_, gevent_
            - threads/single threaded

        - **Result Stores**

            - AMQP, Redis
            - memcached, MongoDB
            - SQLAlchemy, Django ORM
            - Apache Cassandra

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

        - **Workflows**

            Simple and complex workflows can be composed using
            a set of powerful primitives we call the "canvas",
            including grouping, chaining, chunking and more.

            :ref:`Read more… <guide-canvas>`.

        - **Time & Rate Limits**

            You can control how many tasks can be executed per second/minute/hour,
            or how long a task can be allowed to run, and this can be set as
            a default, for a specific worker or individually for each task type.

            :ref:`Read more… <worker-time-limits>`.

        - **Scheduling**

            You can specify the time to run a task in seconds or a
            :class:`~datetime.datetime`, or or you can use
            periodic tasks for recurring events based on a
            simple interval, or crontab expressions
            supporting minute, hour, day of week, day of month, and
            month of year.

            :ref:`Read more… <guide-beat>`.

        - **Autoreloading**

            In development workers can be configured to automatically reload source
            code as it changes, including :manpage:`inotify(7)` support on Linux.

            :ref:`Read more… <worker-autoreloading>`.

        - **Autoscaling**

            Dynamically resizing the worker pool depending on load,
            or custom metrics specified by the user, used to limit
            memory usage in shared hosting/cloud environments or to
            enforce a given quality of service.

            :ref:`Read more… <worker-autoscaling>`.

        - **Resource Leak Protection**

            The :option:`--maxtasksperchild` option is used for user tasks
            leaking resources, like memory or file descriptors, that
            are simply out of your control.

            :ref:`Read more… <worker-maxtasksperchild>`.

        - **User Components**

            Each worker component can be customized, and additional components
            can be defined by the user.  The worker is built up using "boot steps" — a
            dependency graph enabling fine grained control of the worker's
            internals.

.. _`Eventlet`: http://eventlet.net/
.. _`gevent`: http://gevent.org/

Framework Integration
=====================

Celery is easy to integrate with web frameworks, some of which even have
integration packages:

    +--------------------+------------------------+
    | `Django`_          | `django-celery`_       |
    +--------------------+------------------------+
    | `Pyramid`_         | `pyramid_celery`_      |
    +--------------------+------------------------+
    | `Pylons`_          | `celery-pylons`_       |
    +--------------------+------------------------+
    | `Flask`_           | not needed             |
    +--------------------+------------------------+
    | `web2py`_          | `web2py-celery`_       |
    +--------------------+------------------------+
    | `Tornado`_         | `tornado-celery`_      |
    +--------------------+------------------------+

The integration packages are not strictly necessary, but they can make
development easier, and sometimes they add important hooks like closing
database connections at :manpage:`fork(2)`.

.. _`Django`: http://djangoproject.com/
.. _`Pylons`: http://pylonshq.com/
.. _`Flask`: http://flask.pocoo.org/
.. _`web2py`: http://web2py.com/
.. _`Bottle`: http://bottlepy.org/
.. _`Pyramid`: http://docs.pylonsproject.org/en/latest/docs/pyramid.html
.. _`pyramid_celery`: http://pypi.python.org/pypi/pyramid_celery/
.. _`django-celery`: http://pypi.python.org/pypi/django-celery
.. _`celery-pylons`: http://pypi.python.org/pypi/celery-pylons
.. _`web2py-celery`: http://code.google.com/p/web2py-celery/
.. _`Tornado`: http://www.tornadoweb.org/
.. _`tornado-celery`: http://github.com/mher/tornado-celery/

Quickjump
=========

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
        - :ref:`see a list of running workers <monitoring-celeryctl>`
        - :ref:`purge all messages <monitoring-celeryctl>`
        - :ref:`inspect what the workers are doing <monitoring-celeryctl>`
        - :ref:`see what tasks a worker has registerd <monitoring-celeryctl>`
        - :ref:`migrate tasks to a new broker <monitoring-celeryctl>`
        - :ref:`see a list of event message types <event-reference>`
        - :ref:`contribute to Celery <contributing>`
        - :ref:`learn about available configuration settings <configuration>`
        - :ref:`receive email when a task fails <conf-error-mails>`
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
