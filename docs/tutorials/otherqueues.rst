==========================================================
 Using Celery with Redis/Database as the messaging queue.
==========================================================

There's a plugin for celery that enables the use of Redis or an SQL database
as the messaging queue. This is not part of celery itself, but exists as
an extension to `carrot`_.

.. _`carrot`: http://ask.github.com/carrot
.. _`ghettoq`: http://ask.github.com/ghettoq

Installation
============

You need to install the latest development versions of `carrot`_ and
`ghettoq`_::

    $ git clone git://github.com/ask/carrot.git
    $ cd carrot
    $ python setup.py install

    $ git clone git://github.com/ask/ghettoq.git
    $ cd ghettoq
    $ python setup.py install

Redis
=====

For the Redis support you have to install the Python redis client::

    $ pip install redis

Configuration
-------------

Configuration is easy, set the carrot backend, and configure the location of
your Redis database::

    CARROT_BACKEND = "ghettoq.toproot.Redis"

    BROKER_HOST = "localhost"  # Maps to redis host.
    BROKER_PORT = 6379         # Maps to redis port.
    BROKER_VHOST = "celery"    # Maps to database name.

Database
========

Configuration
-------------

The database backend uses the Django ``DATABASE_*`` settings for database
configuration values.

* Set your carrot backend::

    CARROT_BACKEND = "ghettoq.toproot.Database"


* Add ``ghettoq`` to ``INSTALLED_APPS``::

    INSTALLED_APPS = ("ghettoq", )


* Sync your database schema.

    $ python manage.py syncdb

* Or if you're not using django, but the default loader instad run
  ``celeryinit``::

    $ celeryinit

Important notes
---------------

These message queues does not have the concept of exchanges and routing keys,
there's only the queue entity. As a result of this you need to set the name of
the exchange to be the same as the queue::

    CELERY_AMQP_CONSUMER_QUEUE = "tasks"
    CELERY_AMQP_EXCHANGE = "tasks"
