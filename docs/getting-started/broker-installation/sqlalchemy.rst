.. _broker-sqlalchemy:

==================
 Using SQLAlchemy
==================

.. _broker-sqlalchemy-installation:

Installation
============

For the SQLAlchemy transport you have to install the
`kombu-sqlalchemy` library::

    $ pip install -U kombu-sqlalchemy

.. _broker-sqlalchemy-configuration:

Configuration
=============

This transport uses only the :setting:`BROKER_HOST` setting, which have to be
an SQLAlchemy database URI.

#. Set your broker transport::

    BROKER_TRANSPORT = "sqlalchemy"

#. Configure the database URI::

    BROKER_HOST = "sqlite:///celerydb.sqlite"

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
    http://www.sqlalchemy.org/docs/core/engines.html#supported-databases

.. _`SQLAlchemy Connection String`:
    http://www.sqlalchemy.org/docs/core/engines.html#database-urls

.. _sqlalchemy-results-configuration:

Results
-------

To store results in the database as well, you should configure the result
backend.  See :ref:`conf-database-result-backend`.

If you don't intend to consume results you should disable them::

    CELERY_IGNORE_RESULT = True
