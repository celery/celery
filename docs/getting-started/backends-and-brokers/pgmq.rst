.. _broker-pgmq:

=======================
 Using PostgreSQL PGMQ
=======================

This guide uses `PGMQ`_ as the broker and the
:ref:`SQLAlchemy result backend <conf-database-result-backend>` to store
task results in PostgreSQL. Task messages and results can share the same
PostgreSQL database.

.. _PGMQ: https://pgmq.github.io/pgmq/latest/

.. _broker-pgmq-installation:

Installation
============

The PGMQ transport was added in Kombu 5.7.0. Install its ``pgmq`` extra
and Celery's ``sqlalchemy`` extra. SQLAlchemy 2.0 or later supports the
psycopg 3 driver used in this guide:

.. code-block:: console

    $ pip install "celery[sqlalchemy]" "kombu[pgmq]>=5.7.0" "sqlalchemy>=2.0"

Install PGMQ in PostgreSQL
--------------------------

With PGMQ Python SDK 1.1.4 or later, you can install PGMQ directly in an
existing PostgreSQL database using the SDK's ``install_pgmq_from_sql()``
helper. This method also works on hosts that do not support custom
PostgreSQL extensions. Installing the Python package alone does not
provision the database.

If you use an SDK version earlier than 1.1.4, make sure PGMQ is already
installed in the broker database before starting Celery. Follow the
`PGMQ installation instructions`_ to provision it separately. The Python
installation example below requires SDK 1.1.4 or later.

Create ``install_pgmq.py``:

.. code-block:: python

    from pgmq import install_pgmq_from_sql

    version = install_pgmq_from_sql(
        host='localhost',
        port='5432',
        username='celery',
        password='password',
        database='celery',
    )
    print(f'Installed PGMQ SQL version {version}')

Run it once against a database where PGMQ has not yet been installed:

.. code-block:: console

    $ python install_pgmq.py

The database and role must already exist, and the role running the installer
needs permission to create PGMQ's schema and SQL objects. The helper returns
the version of the bundled PGMQ SQL. It installs a pinned SQL snapshot and
does not provide an upgrade path or support rerunning on an existing PGMQ
schema. See the `Python SDK SQL installation guide`_ for connection options
and installation details.

After SQL-only installation, disable automatic extension initialization
in Celery:

.. code-block:: python

    broker_transport_options = {'init_extension': False}

PostgreSQL extension installation
---------------------------------

You can also install PGMQ as a PostgreSQL extension by following the
`PGMQ installation instructions`_. Once the extension files are available
on the server, enable it in the broker database:

.. code-block:: sql

    CREATE EXTENSION IF NOT EXISTS pgmq;

The transport attempts to create the extension on connection by default.
Keep ``init_extension=False`` if an administrator has already enabled it.
Extension installations support PostgreSQL's extension versioning and
``ALTER EXTENSION pgmq UPDATE`` workflow.

.. _Python SDK SQL installation guide: https://pgmq.github.io/pgmq-py/main/sql_installation/
.. _PGMQ installation instructions: https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md

.. _broker-pgmq-configuration:

Configuration
=============

In ``celeryconfig.py``, set :setting:`broker_url` for PGMQ and
:setting:`result_backend` for SQLAlchemy:

.. code-block:: python

    broker_url = 'pgmq://celery:password@localhost:5432/celery'
    result_backend = 'db+postgresql+psycopg://celery:password@localhost:5432/celery'
    broker_transport_options = {'init_extension': False}

This example assumes PGMQ has already been provisioned in the ``celery``
database. The database role also needs permission to create and use the
SQLAlchemy result tables.

The ``pgmq://`` URL selects the broker. The ``db+`` prefix selects Celery's
SQLAlchemy result backend, and ``postgresql+psycopg`` selects PostgreSQL
with psycopg 3. Broker messages use PGMQ queues; results use separate
database backend tables. The two components use separate connections.

The URL format is:

.. code-block:: text

    pgmq://USERNAME:PASSWORD@HOST:PORT/DATABASE

Percent-encode special characters in the username and password. For example:

.. code-block:: python

    from kombu.utils.url import safequote

    username = safequote('celery')
    password = safequote('password/with@characters')
    broker_url = f'pgmq://{username}:{password}@localhost:5432/celery'

For PostgreSQL connection parameters such as TLS settings, supply a full
PostgreSQL connection string in :setting:`broker_transport_options`:

.. code-block:: python

    broker_url = 'pgmq://'
    broker_transport_options = {
        'conn_string': (
            'postgresql://celery:password@db.example.com:5432/celery'
            '?sslmode=verify-full&sslrootcert=/etc/ssl/certs/postgres-ca.pem'
        ),
        'init_extension': False,
    }

``conn_string`` overrides the host, port, database, username, and password
from the broker URL.

.. _pgmq-visibility-timeout:

Visibility timeout
==================

Reading a task makes its message invisible to other consumers for the
visibility timeout. Acknowledging the message deletes it from the queue.
If the message remains unacknowledged when the timeout expires, another
consumer can read it again.

The default timeout is 1800 seconds. Set it through
:setting:`broker_transport_options`:

.. code-block:: python

    broker_transport_options = {'visibility_timeout': 3600}

With :setting:`task_acks_late`, allow enough time for a task to wait in the
worker and execute before the timeout expires. ETA and countdown tasks can
also remain reserved without acknowledgement. A timeout shorter than this
period can cause a task to execute more than once.

A longer timeout also increases the time before an unacknowledged task
becomes available after a worker failure. Tasks that may be redelivered
should be idempotent.

Rejecting a message with requeue enabled makes it visible immediately.
Rejecting it without requeue deletes it. Unacknowledged messages are not
automatically made visible when the transport channel closes; they become
available when their visibility timeout expires.

Polling and notifications
=========================

Long polling is enabled by default. ``wait_time_seconds`` controls how long
a database read waits for messages, with a default of 10 seconds.
``poll_interval_ms`` controls the interval between checks within that read,
with a default of 100 milliseconds.

``polling_interval`` controls the client-side wait after an empty poll.
Its default is one second. To disable long polling and check more often:

.. code-block:: python

    broker_transport_options = {
        'wait_time_seconds': 0,
        'polling_interval': 0.1,
    }

PGMQ notifications can wake a consumer between polls through PostgreSQL's
``LISTEN``/``NOTIFY`` mechanism:

.. code-block:: python

    broker_transport_options = {
        'use_notify': True,
        'wait_time_seconds': 0,
        'polling_interval': 1,
        'notify_throttle_interval_ms': 250,
    }

``use_notify`` defaults to ``False``. When enabled, the transport uses a
separate PostgreSQL connection to listen for queue notifications. It still
reads messages from PGMQ; notifications signal that it should poll again.

Queue names and connection pools
================================

Use ``queue_name_prefix`` to prefix this application's queue names:

.. code-block:: python

    broker_transport_options = {'queue_name_prefix': 'myapp_'}

The transport replaces punctuation other than underscores in queue names
with underscores. Use distinct names containing letters, digits, and
underscores to avoid names such as ``my.queue`` and ``my_queue`` referring
to the same PGMQ queue.

``pool_size`` controls the PGMQ client's connection pool size and defaults
to 10. ``pool_timeout`` controls how many seconds it waits for an available
pooled connection and defaults to 30. These settings apply to each
transport instance, rather than to all workers together:

.. code-block:: python

    broker_transport_options = {
        'pool_size': 5,
        'pool_timeout': 10,
    }

Run a task
==========

Create ``tasks.py`` alongside ``celeryconfig.py``:

.. code-block:: python

    from celery import Celery

    app = Celery('tasks')
    app.config_from_object('celeryconfig')


    @app.task
    def add(x, y):
        return x + y

Start a worker:

.. code-block:: console

    $ celery -A tasks worker --loglevel=INFO --concurrency=2

From another terminal in the same directory, submit a task and read its
result:

.. code-block:: pycon

    >>> from tasks import add
    >>> result = add.delay(2, 3)
    >>> result.get(timeout=30)
    5

``delay`` publishes the task through PGMQ. The worker stores the result
through SQLAlchemy, and ``get`` retrieves it from the result backend.
See :ref:`conf-database-result-backend` for result table and connection
configuration.

Limitations
===========

* Broker message priority and message or queue TTL are not supported.
* A visibility timeout does not guarantee that a task executes exactly once.
  An unacknowledged task can be redelivered after the timeout expires.
* PGMQ's ``DelaySeconds`` message property is separate from Celery's ETA and
  countdown scheduling.

The transport supports direct, topic, and fanout exchanges. Advanced
options, including FIFO reads and partitioned queues, are described in the
`Kombu PGMQ transport reference`_. These options can require additional
PGMQ server capabilities or extensions.

.. _Kombu PGMQ transport reference: https://docs.celeryq.dev/projects/kombu/en/latest/reference/kombu.transport.pgmq.html
