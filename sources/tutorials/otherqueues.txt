.. _tut-otherqueues:

==========================================================
 Using Celery with Redis/Database as the messaging queue.
==========================================================

.. contents::
    :local:

.. _otherqueues-installation:

Installation
============

.. _otherqueues-redis:

Redis
=====

For the Redis support you have to install the Python redis client::

    $ pip install -U redis

.. _otherqueues-redis-conf:

Configuration
-------------

Configuration is easy, set the transport, and configure the location of
your Redis database::

    BROKER_BACKEND = "redis"

    BROKER_HOST = "localhost"  # Maps to redis host.
    BROKER_PORT = 6379         # Maps to redis port.
    BROKER_VHOST = "0"         # Maps to database number.

.. _otherqueues-sqlalchemy:

Django Database
===============

.. _otherqueues-sqlalchemy-conf:

For the SQLAlchemy database transport support you have to install the
`kombu-sqlalchemy` library::

    $ pip install -U kombu-sqlalchemy

Configuration
-------------

This transport uses only the :setting:`BROKER_HOST` setting, which have to be
an SQLAlchemy database URI.

#. Set your broker transport::

    BROKER_BACKEND = "sqlakombu.transport.Transport"

#. Configure the database URI::

    BROKER_HOST = "sqlite://celerydb.sqlite"

Please see `SQLAlchemy: Supported Databases`_ for a table of supported databases.
Some other `SQLAlchemy Connection String`_, examples:

.. code-block:: python

    # sqlite (filename)
    BROKER_HOST = "sqlite:///celerydb.sqlite"

    # mysql
    BROKER_HOST = "mysql://scott:tiger@localhost/foo"

    # postgresql
    BROKER_HOST = "postgresql://scott:tiger@localhost/mydatabase"

    # oracle
    BROKER_HOST = "oracle://scott:tiger@127.0.0.1:1521/sidname"

.. _`SQLAlchemy: Supported Databases`:
    http://www.sqlalchemy.org/docs/dbengine.html#supported-databases

.. _`SQLAlchemy Connection String`:
    http://www.sqlalchemy.org/docs/dbengine.html#create-engine-url-arguments

.. _otherqueues-django:

Django Database
===============

.. _otherqueues-django-conf:

For the Django database transport support you have to install the
`django-kombu` library::

    $ pip install -U django-kombu

Configuration
-------------

The database backend uses the Django `DATABASE_*` settings for database
configuration values.

#. Set your broker transport::

    BROKER_BACKEND = "djkombu.transport.DatabaseTransport"

#. Add :mod:`djkombu` to `INSTALLED_APPS`::

    INSTALLED_APPS = ("djkombu", )


#. Verify you database settings::

    DATABASE_ENGINE = "mysql"
    DATABASE_NAME = "mydb"
    DATABASE_USER = "myuser"
    DATABASE_PASSWORD = "secret"

  The above is just an example, if you haven't configured your database before
  you should read the Django database settings reference:
  http://docs.djangoproject.com/en/1.1/ref/settings/#database-engine

#. Sync your database schema.

    $ python manage.py syncdb
