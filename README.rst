=================================
 celery - Distributed Task Queue
=================================

.. image:: http://cloud.github.com/downloads/celery/celery/celery_128.png

:Version: 3.2.0a1 (Cipater)
:Web: http://celeryproject.org/
:Download: http://pypi.python.org/pypi/celery/
:Source: http://github.com/celery/celery/
:Keywords: task queue, job queue, asynchronous, async, rabbitmq, amqp, redis,
  python, webhooks, queue, distributed

--

What is a Task Queue?
=====================

Task queues are used as a mechanism to distribute work across threads or
machines.

A task queue's input is a unit of work, called a task, dedicated worker
processes then constantly monitor the queue for new work to perform.

Celery communicates via messages, usually using a broker
to mediate between clients and workers.  To initiate a task a client puts a
message on the queue, the broker then delivers the message to a worker.

A Celery system can consist of multiple workers and brokers, giving way
to high availability and horizontal scaling.

Celery is a library written in Python, but the protocol can be implemented in
any language.  So far there's RCelery_ for the Ruby programming language, and a
`PHP client`, but language interoperability can also be achieved
by using webhooks.

.. _RCelery: http://leapfrogdevelopment.github.com/rcelery/
.. _`PHP client`: https://github.com/gjedeer/celery-php
.. _`using webhooks`:
    http://docs.celeryproject.org/en/latest/userguide/remote-tasks.html

What do I need?
===============

Celery version 3.0 runs on,

- Python (2.6, 2.7, 3.3, 3.4)
- PyPy (1.8, 1.9)
- Jython (2.5, 2.7).

This is the last version to support Python 2.5,
and from Celery 3.1, Python 2.6 or later is required.
The last version to support Python 2.4 was Celery series 2.2.

*Celery* is usually used with a message broker to send and receive messages.
The RabbitMQ, Redis transports are feature complete,
but there's also experimental support for a myriad of other solutions, including
using SQLite for local development.

*Celery* can run on a single machine, on multiple machines, or even
across datacenters.

Get Started
===========

If this is the first time you're trying to use Celery, or you are
new to Celery 3.0 coming from previous versions then you should read our
getting started tutorials:

- `First steps with Celery`_

    Tutorial teaching you the bare minimum needed to get started with Celery.

- `Next steps`_

    A more complete overview, showing more features.

.. _`First steps with Celery`:
    http://docs.celeryproject.org/en/latest/getting-started/first-steps-with-celery.html

.. _`Next steps`:
    http://docs.celeryproject.org/en/latest/getting-started/next-steps.html

Celery is...
==========

- **Simple**

    Celery is easy to use and maintain, and does *not need configuration files*.

    It has an active, friendly community you can talk to for support,
    including a `mailing-list`_ and and an IRC channel.

    Here's one of the simplest applications you can make::

        from celery import Celery

        app = Celery('hello', broker='amqp://guest@localhost//')

        @app.task
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

It supports...
============

    - **Message Transports**

        - RabbitMQ_, Redis_,
        - MongoDB_ (experimental), Amazon SQS (experimental),
        - CouchDB_ (experimental), SQLAlchemy_ (experimental),
        - Django ORM (experimental), `IronMQ`_
        - and more...

    - **Concurrency**

        - Prefork, Eventlet_, gevent_, threads/single threaded

    - **Result Stores**

        - AMQP, Redis
        - memcached, MongoDB
        - SQLAlchemy, Django ORM
        - Apache Cassandra, IronCache

    - **Serialization**

        - *pickle*, *json*, *yaml*, *msgpack*.
        - *zlib*, *bzip2* compression.
        - Cryptographic message signing.

.. _`Eventlet`: http://eventlet.net/
.. _`gevent`: http://gevent.org/

.. _RabbitMQ: http://rabbitmq.com
.. _Redis: http://redis.io
.. _MongoDB: http://mongodb.org
.. _Beanstalk: http://kr.github.com/beanstalkd
.. _CouchDB: http://couchdb.apache.org
.. _SQLAlchemy: http://sqlalchemy.org
.. _`IronMQ`: http://iron.io

Framework Integration
=====================

Celery is easy to integrate with web frameworks, some of which even have
integration packages:

    +--------------------+------------------------+
    | `Django`_          | not needed             |
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
database connections at ``fork``.

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

.. _celery-documentation:

Documentation
=============

The `latest documentation`_ with user guides, tutorials and API reference
is hosted at Read The Docs.

.. _`latest documentation`: http://docs.celeryproject.org/en/latest/

.. _celery-installation:

Installation
============

You can install Celery either via the Python Package Index (PyPI)
or from source.

To install using `pip`,::

    $ pip install -U Celery

To install using `easy_install`,::

    $ easy_install -U Celery

.. _bundles:

Bundles
-------

Celery also defines a group of bundles that can be used
to install Celery and the dependencies for a given feature.

You can specify these in your requirements or on the ``pip`` comand-line
by using brackets.  Multiple bundles can be specified by separating them by
commas.
::

    $ pip install "celery[librabbitmq]"

    $ pip install "celery[librabbitmq,redis,auth,msgpack]"

The following bundles are available:

Serializers
~~~~~~~~~~~

:celery[auth]:
    for using the auth serializer.

:celery[msgpack]:
    for using the msgpack serializer.

:celery[yaml]:
    for using the yaml serializer.

Concurrency
~~~~~~~~~~~

:celery[eventlet]:
    for using the eventlet pool.

:celery[gevent]:
    for using the gevent pool.

:celery[threads]:
    for using the thread pool.

Transports and Backends
~~~~~~~~~~~~~~~~~~~~~~~

:celery[librabbitmq]:
    for using the librabbitmq C library.

:celery[redis]:
    for using Redis as a message transport or as a result backend.

:celery[mongodb]:
    for using MongoDB as a message transport (*experimental*),
    or as a result backend (*supported*).

:celery[sqs]:
    for using Amazon SQS as a message transport (*experimental*).

:celery[memcache]:
    for using memcached as a result backend.

:celery[cassandra]:
    for using Apache Cassandra as a result backend.

:celery[couchdb]:
    for using CouchDB as a message transport (*experimental*).

:celery[couchbase]:
    for using CouchBase as a result backend.

:celery[beanstalk]:
    for using Beanstalk as a message transport (*experimental*).

:celery[zookeeper]:
    for using Zookeeper as a message transport.

:celery[zeromq]:
    for using ZeroMQ as a message transport (*experimental*).

:celery[sqlalchemy]:
    for using SQLAlchemy as a message transport (*experimental*),
    or as a result backend (*supported*).

:celery[pyro]:
    for using the Pyro4 message transport (*experimental*).

:celery[slmq]:
    for using the SoftLayer Message Queue transport (*experimental*).

.. _celery-installing-from-source:

Downloading and installing from source
--------------------------------------

Download the latest version of Celery from
http://pypi.python.org/pypi/celery/

You can install it by doing the following,::

    $ tar xvfz celery-0.0.0.tar.gz
    $ cd celery-0.0.0
    $ python setup.py build
    # python setup.py install

The last command must be executed as a privileged user if
you are not currently using a virtualenv.

.. _celery-installing-from-git:

Using the development version
-----------------------------

With pip
~~~~~~~~

The Celery development version also requires the development
versions of ``kombu``, ``amqp`` and ``billiard``.

You can install the latest snapshot of these using the following
pip commands::

    $ pip install https://github.com/celery/celery/zipball/master#egg=celery
    $ pip install https://github.com/celery/billiard/zipball/master#egg=billiard
    $ pip install https://github.com/celery/py-amqp/zipball/master#egg=amqp
    $ pip install https://github.com/celery/kombu/zipball/master#egg=kombu

With git
~~~~~~~~

Please the Contributing section.

.. _getting-help:

Getting Help
============

.. _mailing-list:

Mailing list
------------

For discussions about the usage, development, and future of celery,
please join the `celery-users`_ mailing list.

.. _`celery-users`: http://groups.google.com/group/celery-users/

.. _irc-channel:

IRC
---

Come chat with us on IRC. The **#celery** channel is located at the `Freenode`_
network.

.. _`Freenode`: http://freenode.net

.. _bug-tracker:

Bug tracker
===========

If you have any suggestions, bug reports or annoyances please report them
to our issue tracker at http://github.com/celery/celery/issues/

.. _wiki:

Wiki
====

http://wiki.github.com/celery/celery/

.. _contributing-short:

Contributing
============

Development of `celery` happens at Github: http://github.com/celery/celery

You are highly encouraged to participate in the development
of `celery`. If you don't like Github (for some reason) you're welcome
to send regular patches.

Be sure to also read the `Contributing to Celery`_ section in the
documentation.

.. _`Contributing to Celery`:
    http://docs.celeryproject.org/en/master/contributing.html

.. _license:

License
=======

This software is licensed under the `New BSD License`. See the ``LICENSE``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround


.. image:: https://d2weczhvl823v0.cloudfront.net/celery/celery/trend.png
    :alt: Bitdeli badge
    :target: https://bitdeli.com/free

