.. _broker-sqlalchemy:

==================
 Using SQLAlchemy
==================

.. admonition:: Experimental Status

    The SQLAlchemy transport is unstable in many areas and there are
    several issues open.  Unfortunately we don't have the resources or funds
    required to improve the situation, so we're looking for contributors
    and partners willing to help.

.. _broker-sqlalchemy-installation:

Installation
============

.. _broker-sqlalchemy-configuration:

Configuration
=============

Celery needs to know the location of your database, which should be the usual
SQLAlchemy connection string, but with 'sqla+' prepended to it::

    broker_url = 'sqla+sqlite:///celerydb.sqlite'

This transport uses only the :setting:`broker_url` setting, which have to be
an SQLAlchemy database URI.


Please see `SQLAlchemy: Supported Databases`_ for a table of supported databases.

Here's a list of examples using a selection of other `SQLAlchemy Connection Strings`_:

.. code-block:: python

    # sqlite (filename)
    broker_url = 'sqla+sqlite:///celerydb.sqlite'

    # mysql
    broker_url = 'sqla+mysql://scott:tiger@localhost/foo'

    # postgresql
    broker_url = 'sqla+postgresql://scott:tiger@localhost/mydatabase'

    # oracle
    broker_url = 'sqla+oracle://scott:tiger@127.0.0.1:1521/sidname'

.. _`SQLAlchemy: Supported Databases`:
    http://www.sqlalchemy.org/docs/core/engines.html#supported-databases

.. _`SQLAlchemy Connection Strings`:
    http://www.sqlalchemy.org/docs/core/engines.html#database-urls

.. _sqlalchemy-results-configuration:

Results
-------

To store results in the database as well, you should configure the result
backend.  See :ref:`conf-database-result-backend`.

.. _broker-sqlalchemy-limitations:

Limitations
===========

The SQLAlchemy database transport does not currently support:

    * Remote control commands (:program:`celery events` command, broadcast)
    * Events, including the Django Admin monitor.
    * Using more than a few workers (can lead to messages being executed
      multiple times).
