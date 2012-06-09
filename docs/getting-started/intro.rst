=============================
 Introduction to Task Queues
=============================

.. contents::
    :local:
    :depth: 1

What are Task Queues?
=====================

.. compound::

    "The quick brown fox jumps over the lazy dog"

        said the Farmer.

            but little did *he* know...


What do I need?
===============

*Celery* requires a message broker to send and receive messages,
but this term has been stretched to include everything from
financial-grade messaging systems to your fridge.

You can run *Celery* on a single or multiple machines, or even
across datacenters.

Celery runs on Python 2.6/2.7/3.2, PyPy and Jython.


Celery is…
==========

.. topic:: ”

    - **Simple**

        Bla bla bla., yaddi blabla, bla bli bla do re mi, bla bi do,
        re mi bla do blah blah yadda blah blah blah blah.

    - **Fast**

        Bla bla bla. librabbitmq, yaddi blabla lightweight, bla bli bla do re mi, bla bi do,
        re mi bla do blah blah yadda blah blah blah blah.

    - **Highly Available**

        Workers and clients will automatically retry in the event
        of connection loss or failure, and some brokers support
        HA in way of *Master/Master* or -- *Master/Slave* replication.

    - **Flexible**

        Almost every part of *Celery* can be extended or used on its own,
        Custom pool implementations, serializers, compression schemes, logging,
        schedulers, consumers, producers, autoscalers, broker transorts and much more.


.. topic:: It supports

    .. hlist::
        :columns: 2

        - **Brokers**

            :ref:`RabbitMQ <broker-rabbitmq>`, :ref:`Redis <broker-redis>`,
            :ref:`MongoDB <broker-mongodb>`, :ref:`Beanstalk <broker-beanstalk>`,
            :ref:`CouchDB <broker-couchdb>`, or
            :ref:`SQLAlchemy <broker-sqlalchemy>`/:ref:`Django ORM <broker-django>`.

        - **Concurrency**

            multiprocessing, Eventlet_, gevent_ and threads.

        - **Serialization & Compression**

            Messages can be serialized using *pickle*, *json*, *yaml*, *msgpack*,
            and optionally compressed using *zlib* or *bzip2*

        - **Result Stores**

            AMQP, Redis, memcached, MongoDB, SQLAlchemy/Django ORM, Apache Cassandra.


.. topic:: Features

    .. hlist::
        :columns: 2

        - **Monitoring**

            The stream of monitoring events emit by the worker are used
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
            are out simply out of your control.

            :ref:`Read more… <worker-maxtasksperchild>`.

        - **User Components**

            Each worker component can be customized, and additional components
            can be defined by the user.  The worker is built up using "boot steps" — a
            dependency graph enabling fine grained control of the workers
            internals.

.. _`RabbitMQ`: http://www.rabbitmq.com/
.. _`Redis`: http://code.google.com/p/redis/
.. _`SQLAlchemy`: http://www.sqlalchemy.org/
.. _`Django ORM`: http://djangoproject.com/
.. _`Eventlet`: http://eventlet.net/
.. _`gevent`: http://gevent.org/
.. _`Beanstalk`: http://kr.github.com/beanstalkd/
.. _`MongoDB`: http://mongodb.org/
.. _`CouchDB`: http://couchdb.apache.org/
.. _`Amazon SQS`: http://aws.amazon.com/sqs/
.. _`Apache ZooKeeper`: http://zookeeper.apache.org/
