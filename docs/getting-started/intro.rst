=============================
 Introduction to Task Queues
=============================

.. contents::
    :local:
    :depth: 1

What are Task Queues?
=====================

HELLO

What do I need?
===============

.. sidebar:: Version Requirements
    :subtitle: Celery version 2.6 runs on

    - Python ❨2.5, 2.6, 2.7, 3.2, 3.3❩
    - PyPy ❨1.8, 1.9❩
    - Jython ❨2.5, 2.7❩.

    This is the last version to support Python 2.5,
    and from the next version Python 2.6 or newer is required.
    The last version to support Python 2.4 was Celery series 2.2.

*Celery* requires a message broker to send and receive messages,
but this term has been stretched to include everything from
your fridge to financial-grade messaging systems.

*Celery* can run on a single machine, on multiple machines, or even
across datacenters.

Celery is…
==========

.. topic:: ”

    - **Simple**

        Celery is easy to use and maintain, and does *not need configuration files*.

        It has an active, friendly community you can talk to for support,
        including a :ref:`mailing-list <mailing-list>` and and :ref:`IRC
        channel <irc-channel>`.

        Here's one of the simplest applications you can make:

        .. code-block:: python

            from celery import Celery

            celery = Celery('hello', broker='amqp://guest@localhost//')

            @celery.task()
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
        schedulers, consumers, producers, autoscalers, broker transorts and much more.


.. topic:: It supports

    .. hlist::
        :columns: 2

        - **Brokers**

            - :ref:`RabbitMQ <broker-rabbitmq>`
            - :ref:`Redis <broker-redis>`,
            - :ref:`MongoDB <broker-mongodb>` 
            - :ref:`Beanstalk <broker-beanstalk>`
            - :ref:`CouchDB <broker-couchdb>` 
            - :ref:`SQLAlchemy <broker-sqlalchemy>`
            - :ref:`Django ORM <broker-django>` 
            - :ref:`Amazon SQS <broker-sqs>`
            - and more…

        - **Concurrency**

            - multiprocessing,
            - Eventlet_, gevent_
            - threads
            - single

        - **Result Stores**

            - AMQP 
            - Redis
            - memcached 
            - MongoDB
            - SQLAlchemy
            - Django ORM
            - Apache Cassandra

        - **Serialization**

            - *pickle*, *json*, *yaml*, *msgpack*.
            - Fine-grained serialization settings.

        - **Compression**

            - *zlib*, *bzip2*, or uncompressed.

        - **Crypto**

            - Cryptographic message signing.



.. topic:: Features

    .. hlist::
        :columns: 2

        - **Monitoring**

            The stream of monitoring events emitted by the worker are used
            by built-in and external tools to tell you what your cluster
            is doing in real-time.

            :ref:`Read more… <guide-monitoring>`.

        - **Time Limits & Rate Limits**

            You can control how many tasks can be executed per second/minute/hour,
            or how long a task can be allowed to run, and this can be set as
            a default, for a specific worker or individually for each task type.

            :ref:`Read more… <worker-time-limits>`.

        - **Autoreloading**

            While in development workers can be configured to automatically reload source
            code as it changes.

            :ref:`Read more… <worker-autoreloading>`.

        - **Autoscaling**

            Dynamically resizing the worker pool depending on load,
            or custom metrics specified by the user, used to limit
            memory usage in shared hosting/cloud environment or to
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


.. topic:: I want to ⟶

    .. hlist::
        :columns: 2

        - :ref:`get the return value of a task <task-states>`
        - :ref:`use logging from my task <task-logging>`
        - :ref:`learn about best practices <task-best-practices>`
        - :ref:`create a custom task base class <task-custom-classes>`
        - :ref:`add a callback to a group of tasks <chords>`
        - :ref:`split a task into several chunks <chunking>`
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
        - change worker queues at runtime

.. topic:: Jump to ⟶

    .. hlist::
        :columns: 4

        - :ref:`Brokers <brokers>`
        - :ref:`Tasks <guide-tasks>`
        - :ref:`Calling <guide-calling>`
        - :ref:`Workers <guide-workers>`
        - :ref:`Monitoring <guide-monitoring>`
        - :ref:`Optimizing <guide-optimizing>`
        - :ref:`Security <guide-security>`
        - :ref:`Routing <guide-routing>`
        - :ref:`Configuration Reference <configuration>`
        - :ref:`Django <django>`
        - :ref:`Contributing <contributing>`
        - :ref:`Signals <signals>`
        - :ref:`FAQ <faq>`
