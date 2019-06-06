.. image:: http://docs.celeryproject.org/en/latest/_images/celery-banner-small.png

|build-status| |coverage| |license| |wheel| |pyversion| |pyimp| |ocbackerbadge| |ocsponsorbadge|

:Version: 4.4.0rc1 (cliffs)
:Web: http://celeryproject.org/
:Download: https://pypi.org/project/celery/
:Source: https://github.com/celery/celery/
:Keywords: task, queue, job, async, rabbitmq, amqp, redis,
  python, distributed, actors

Donations
=========

This project relies on your generous donations.

If you are using Celery to create a commercial product, please consider becoming our `backer`_ or our `sponsor`_ to ensure Celery's future.

.. _`backer`: https://opencollective.com/celery#backer
.. _`sponsor`: https://opencollective.com/celery#sponsor


Sponsors
--------

`Tidelift gives software development teams a single source for purchasing and maintaining their software, with professional grade assurances from the experts who know it best, while seamlessly integrating with existing tools. <https://tidelift.com/subscription/pkg/pypi-celery?utm_source=pypi-celery&utm_medium=referral&utm_campaign=readme>`_


What's a Task Queue?
====================

Task queues are used as a mechanism to distribute work across threads or
machines.

A task queue's input is a unit of work, called a task, dedicated worker
processes then constantly monitor the queue for new work to perform.

Celery communicates via messages, usually using a broker
to mediate between clients and workers. To initiate a task a client puts a
message on the queue, the broker then delivers the message to a worker.

A Celery system can consist of multiple workers and brokers, giving way
to high availability and horizontal scaling.

Celery is written in Python, but the protocol can be implemented in any
language. In addition to Python there's node-celery_ for Node.js,
and a `PHP client`_.

Language interoperability can also be achieved by using webhooks
in such a way that the client enqueues an URL to be requested by a worker.

.. _node-celery: https://github.com/mher/node-celery
.. _`PHP client`: https://github.com/gjedeer/celery-php

What do I need?
===============

Celery version 4.3 runs on,

- Python (2.7, 3.4, 3.5, 3.6, 3.7)
- PyPy2.7 (6.0)
- PyPy3.5 (6.0)


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

*Celery* is usually used with a message broker to send and receive messages.
The RabbitMQ, Redis transports are feature complete,
but there's also experimental support for a myriad of other solutions, including
using SQLite for local development.

*Celery* can run on a single machine, on multiple machines, or even
across datacenters.

Get Started
===========

If this is the first time you're trying to use Celery, or you're
new to Celery 4.2 coming from previous versions then you should read our
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
=============

- **Simple**

    Celery is easy to use and maintain, and does *not need configuration files*.

    It has an active, friendly community you can talk to for support,
    like at our `mailing-list`_, or the IRC channel.

    Here's one of the simplest applications you can make::

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
    py-librabbitmq, and optimized settings).

- **Flexible**

    Almost every part of *Celery* can be extended or used on its own,
    Custom pool implementations, serializers, compression schemes, logging,
    schedulers, consumers, producers, broker transports, and much more.

It supports...
================

    - **Message Transports**

        - RabbitMQ_, Redis_, Amazon SQS

    - **Concurrency**

        - Prefork, Eventlet_, gevent_, single threaded (``solo``)

    - **Result Stores**

        - AMQP, Redis
        - memcached
        - SQLAlchemy, Django ORM
        - Apache Cassandra, IronCache, Elasticsearch

    - **Serialization**

        - *pickle*, *json*, *yaml*, *msgpack*.
        - *zlib*, *bzip2* compression.
        - Cryptographic message signing.

.. _`Eventlet`: http://eventlet.net/
.. _`gevent`: http://gevent.org/

.. _RabbitMQ: https://rabbitmq.com
.. _Redis: https://redis.io
.. _SQLAlchemy: http://sqlalchemy.org

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

The integration packages aren't strictly necessary, but they can make
development easier, and sometimes they add important hooks like closing
database connections at ``fork``.

.. _`Django`: https://djangoproject.com/
.. _`Pylons`: http://pylonsproject.org/
.. _`Flask`: http://flask.pocoo.org/
.. _`web2py`: http://web2py.com/
.. _`Bottle`: https://bottlepy.org/
.. _`Pyramid`: http://docs.pylonsproject.org/en/latest/docs/pyramid.html
.. _`pyramid_celery`: https://pypi.org/project/pyramid_celery/
.. _`celery-pylons`: https://pypi.org/project/celery-pylons/
.. _`web2py-celery`: https://code.google.com/p/web2py-celery/
.. _`Tornado`: http://www.tornadoweb.org/
.. _`tornado-celery`: https://github.com/mher/tornado-celery/

.. _celery-documentation:

Documentation
=============

The `latest documentation`_ is hosted at Read The Docs, containing user guides,
tutorials, and an API reference.

.. _`latest documentation`: http://docs.celeryproject.org/en/latest/

.. _celery-installation:

Installation
============

You can install Celery either via the Python Package Index (PyPI)
or from source.

To install using ``pip``:

::


    $ pip install -U Celery

.. _bundles:

Bundles
-------

Celery also defines a group of bundles that can be used
to install Celery and the dependencies for a given feature.

You can specify these in your requirements or on the ``pip``
command-line by using brackets. Multiple bundles can be specified by
separating them by commas.

::


    $ pip install "celery[librabbitmq]"

    $ pip install "celery[librabbitmq,redis,auth,msgpack]"

The following bundles are available:

Serializers
~~~~~~~~~~~

:``celery[auth]``:
    for using the ``auth`` security serializer.

:``celery[msgpack]``:
    for using the msgpack serializer.

:``celery[yaml]``:
    for using the yaml serializer.

Concurrency
~~~~~~~~~~~

:``celery[eventlet]``:
    for using the ``eventlet`` pool.

:``celery[gevent]``:
    for using the ``gevent`` pool.

Transports and Backends
~~~~~~~~~~~~~~~~~~~~~~~

:``celery[librabbitmq]``:
    for using the librabbitmq C library.

:``celery[redis]``:
    for using Redis as a message transport or as a result backend.

:``celery[sqs]``:
    for using Amazon SQS as a message transport.

:``celery[tblib``]:
    for using the ``task_remote_tracebacks`` feature.

:``celery[memcache]``:
    for using Memcached as a result backend (using ``pylibmc``)

:``celery[pymemcache]``:
    for using Memcached as a result backend (pure-Python implementation).

:``celery[cassandra]``:
    for using Apache Cassandra as a result backend with DataStax driver.

:``celery[azureblockblob]``:
    for using Azure Storage as a result backend (using ``azure-storage``)

:``celery[s3]``:
    for using S3 Storage as a result backend.

:``celery[couchbase]``:
    for using Couchbase as a result backend.

:``celery[arangodb]``:
    for using ArangoDB as a result backend.

:``celery[elasticsearch]``:
    for using Elasticsearch as a result backend.

:``celery[riak]``:
    for using Riak as a result backend.

:``celery[cosmosdbsql]``:
    for using Azure Cosmos DB as a result backend (using ``pydocumentdb``)

:``celery[zookeeper]``:
    for using Zookeeper as a message transport.

:``celery[sqlalchemy]``:
    for using SQLAlchemy as a result backend (*supported*).

:``celery[pyro]``:
    for using the Pyro4 message transport (*experimental*).

:``celery[slmq]``:
    for using the SoftLayer Message Queue transport (*experimental*).

:``celery[consul]``:
    for using the Consul.io Key/Value store as a message transport or result backend (*experimental*).

:``celery[django]``:
    specifies the lowest version possible for Django support.

    You should probably not use this in your requirements, it's here
    for informational purposes only.


.. _celery-installing-from-source:

Downloading and installing from source
--------------------------------------

Download the latest version of Celery from PyPI:

https://pypi.org/project/celery/

You can install it by doing the following,:

::


    $ tar xvfz celery-0.0.0.tar.gz
    $ cd celery-0.0.0
    $ python setup.py build
    # python setup.py install

The last command must be executed as a privileged user if
you aren't currently using a virtualenv.

.. _celery-installing-from-git:

Using the development version
-----------------------------

With pip
~~~~~~~~

The Celery development version also requires the development
versions of ``kombu``, ``amqp``, ``billiard``, and ``vine``.

You can install the latest snapshot of these using the following
pip commands:

::


    $ pip install https://github.com/celery/celery/zipball/master#egg=celery
    $ pip install https://github.com/celery/billiard/zipball/master#egg=billiard
    $ pip install https://github.com/celery/py-amqp/zipball/master#egg=amqp
    $ pip install https://github.com/celery/kombu/zipball/master#egg=kombu
    $ pip install https://github.com/celery/vine/zipball/master#egg=vine

With git
~~~~~~~~

Please see the Contributing section.

.. _getting-help:

Getting Help
============

.. _mailing-list:

Mailing list
------------

For discussions about the usage, development, and future of Celery,
please join the `celery-users`_ mailing list.

.. _`celery-users`: https://groups.google.com/group/celery-users/

.. _irc-channel:

IRC
---

Come chat with us on IRC. The **#celery** channel is located at the `Freenode`_
network.

.. _`Freenode`: https://freenode.net

.. _bug-tracker:

Bug tracker
===========

If you have any suggestions, bug reports, or annoyances please report them
to our issue tracker at https://github.com/celery/celery/issues/

.. _wiki:

Wiki
====

https://github.com/celery/celery/wiki

Credits
=======

.. _contributing-short:

Contributors
------------

This project exists thanks to all the people who contribute. Development of
`celery` happens at GitHub: https://github.com/celery/celery

You're highly encouraged to participate in the development
of `celery`. If you don't like GitHub (for some reason) you're welcome
to send regular patches.

Be sure to also read the `Contributing to Celery`_ section in the
documentation.

.. _`Contributing to Celery`:
    http://docs.celeryproject.org/en/master/contributing.html

|oc-contributors|

.. |oc-contributors| image:: https://opencollective.com/celery/contributors.svg?width=890&button=false
    :target: https://github.com/celery/celery/graphs/contributors

Backers
-------

Thank you to all our backers! 🙏 [`Become a backer`_]

.. _`Become a backer`: https://opencollective.com/celery#backer

|oc-backers|

.. |oc-backers| image:: https://opencollective.com/celery/backers.svg?width=890
    :target: https://opencollective.com/celery#backers

Sponsors
--------

Support this project by becoming a sponsor. Your logo will show up here with a
link to your website. [`Become a sponsor`_]

.. _`Become a sponsor`: https://opencollective.com/celery#sponsor

|oc-sponsors|

.. |oc-sponsors| image:: https://opencollective.com/celery/sponsor/0/avatar.svg
    :target: https://opencollective.com/celery/sponsor/0/website

.. _license:

License
=======

This software is licensed under the `New BSD License`. See the ``LICENSE``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround

.. |build-status| image:: https://secure.travis-ci.org/celery/celery.png?branch=master
    :alt: Build status
    :target: https://travis-ci.org/celery/celery

.. |coverage| image:: https://codecov.io/github/celery/celery/coverage.svg?branch=master
    :target: https://codecov.io/github/celery/celery?branch=master

.. |license| image:: https://img.shields.io/pypi/l/celery.svg
    :alt: BSD License
    :target: https://opensource.org/licenses/BSD-3-Clause

.. |wheel| image:: https://img.shields.io/pypi/wheel/celery.svg
    :alt: Celery can be installed via wheel
    :target: https://pypi.org/project/celery/

.. |pyversion| image:: https://img.shields.io/pypi/pyversions/celery.svg
    :alt: Supported Python versions.
    :target: https://pypi.org/project/celery/

.. |pyimp| image:: https://img.shields.io/pypi/implementation/celery.svg
    :alt: Support Python implementations.
    :target: https://pypi.org/project/celery/

.. |ocbackerbadge| image:: https://opencollective.com/celery/backers/badge.svg
    :alt: Backers on Open Collective
    :target: #backers

.. |ocsponsorbadge| image:: https://opencollective.com/celery/sponsors/badge.svg
    :alt: Sponsors on Open Collective
    :target: #sponsors
