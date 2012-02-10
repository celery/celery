.. _configuration:

============================
 Configuration and defaults
============================

This document describes the configuration options available.

If you're using the default loader, you must create the :file:`celeryconfig.py`
module and make sure it is available on the Python path.

.. contents::
    :local:
    :depth: 2

.. _conf-example:

Example configuration file
==========================

This is an example configuration file to get you started.
It should contain all you need to run a basic Celery set-up.

.. code-block:: python

    # List of modules to import when celery starts.
    CELERY_IMPORTS = ("myapp.tasks", )

    ## Result store settings.
    CELERY_RESULT_BACKEND = "database"
    CELERY_RESULT_DBURI = "sqlite:///mydatabase.db"

    ## Broker settings.
    BROKER_URL = "amqp://guest:guest@localhost:5672//"

    ## Worker settings
    ## If you're doing mostly I/O you can have more processes,
    ## but if mostly spending CPU, try to keep it close to the
    ## number of CPUs on your machine. If not set, the number of CPUs/cores
    ## available will be used.
    CELERYD_CONCURRENCY = 10

    CELERY_ANNOTATIONS = {"tasks.add": {"rate_limit": "10/s"}}


Configuration Directives
========================

.. _conf-datetime:

Time and date settings
----------------------

.. setting:: CELERY_ENABLE_UTC

CELERY_ENABLE_UTC
~~~~~~~~~~~~~~~~~

If enabled dates and times in messages will be converted to use
the UTC timezone.

Note that workers running Celery versions below 2.5 will assume a local
timezone for all messages, so only enable if all workers have been
upgraded.

Disabled by default.  UTC will be enabled by default in version 3.0.

.. setting:: CELERY_TIMEZONE

CELERY_TIMEZONE
---------------

Configure Celery to use a custom time zone.
The timezone value can be any time zone supported by the :mod:`pytz`
library.  :mod:`pytz` must be installed for the selected zone
to be used.

If not set then the systems default local time zone is used.

.. _conf-tasks:

Task settings
-------------

.. setting:: CELERY_ANNOTATIONS

CELERY_ANNOTATIONS
~~~~~~~~~~~~~~~~~~

This setting can be used to rewrite any task attribute from the
configuration.  The setting can be a dict, or a list of annotation
objects that filter for tasks and return a map of attributes
to change.


This will change the ``rate_limit`` attribute for the ``tasks.add``
task:

.. code-block:: python

    CELERY_ANNOTATIONS = {"tasks.add": {"rate_limit": "10/s"}}

or change the same for all tasks:

.. code-block:: python

    CELERY_ANNOTATIONS = {"*": {"rate_limit": "10/s"}}


You can change methods too, for example the ``on_failure`` handler:

.. code-block:: python

    def my_on_failure(self, exc, task_id, args, kwargs, einfo):
        print("Oh no! Task failed: %r" % (exc, ))

    CELERY_ANNOTATIONS = {"*": {"on_failure": my_on_failure}}


If you need more flexibility then you can use objects
instead of a dict to choose which tasks to annotate:

.. code-block:: python

    class MyAnnotate(object):

        def annotate(self, task):
            if task.name.startswith("tasks."):
                return {"rate_limit": "10/s"}

    CELERY_ANNOTATIONS = (MyAnnotate(), {...})



.. _conf-concurrency:

Concurrency settings
--------------------

.. setting:: CELERYD_CONCURRENCY

CELERYD_CONCURRENCY
~~~~~~~~~~~~~~~~~~~

The number of concurrent worker processes/threads/green threads, executing
tasks.

Defaults to the number of available CPUs.

.. setting:: CELERYD_PREFETCH_MULTIPLIER

CELERYD_PREFETCH_MULTIPLIER
~~~~~~~~~~~~~~~~~~~~~~~~~~~

How many messages to prefetch at a time multiplied by the number of
concurrent processes.  The default is 4 (four messages for each
process).  The default setting is usually a good choice, however -- if you
have very long running tasks waiting in the queue and you have to start the
workers, note that the first worker to start will receive four times the
number of messages initially.  Thus the tasks may not be fairly distributed
to the workers.

.. _conf-result-backend:

Task result backend settings
----------------------------

.. setting:: CELERY_RESULT_BACKEND

CELERY_RESULT_BACKEND
~~~~~~~~~~~~~~~~~~~~~
:Deprecated aliases: ``CELERY_BACKEND``

The backend used to store task results (tombstones).
Disabled by default.
Can be one of the following:

* database
    Use a relational database supported by `SQLAlchemy`_.
    See :ref:`conf-database-result-backend`.

* cache
    Use `memcached`_ to store the results.
    See :ref:`conf-cache-result-backend`.

* mongodb
    Use `MongoDB`_ to store the results.
    See :ref:`conf-mongodb-result-backend`.

* redis
    Use `Redis`_ to store the results.
    See :ref:`conf-redis-result-backend`.

* tyrant
    Use `Tokyo Tyrant`_ to store the results.
    See :ref:`conf-tyrant-result-backend`.

* amqp
    Send results back as AMQP messages
    See :ref:`conf-amqp-result-backend`.

* cassandra
    Use `Cassandra`_ to store the results.
    See :ref:`conf-cassandra-result-backend`.

.. warning:

    While the AMQP result backend is very efficient, you must make sure
    you only receive the same result once.  See :doc:`userguide/executing`).

.. _`SQLAlchemy`: http://sqlalchemy.org
.. _`memcached`: http://memcached.org
.. _`MongoDB`: http://mongodb.org
.. _`Redis`: http://code.google.com/p/redis/
.. _`Tokyo Tyrant`: http://1978th.net/tokyotyrant/
.. _`Cassandra`: http://cassandra.apache.org/

.. setting:: CELERY_RESULT_SERIALIZER

CELERY_RESULT_SERIALIZER
~~~~~~~~~~~~~~~~~~~~~~~~

Result serialization format.  Default is `"pickle"`. See
:ref:`executing-serializers` for information about supported
serialization formats.

.. _conf-database-result-backend:

Database backend settings
-------------------------

.. setting:: CELERY_RESULT_DBURI

CELERY_RESULT_DBURI
~~~~~~~~~~~~~~~~~~~

Please see `Supported Databases`_ for a table of supported databases.
To use this backend you need to configure it with an
`Connection String`_, some examples include:

.. code-block:: python

    # sqlite (filename)
    CELERY_RESULT_DBURI = "sqlite:///celerydb.sqlite"

    # mysql
    CELERY_RESULT_DBURI = "mysql://scott:tiger@localhost/foo"

    # postgresql
    CELERY_RESULT_DBURI = "postgresql://scott:tiger@localhost/mydatabase"

    # oracle
    CELERY_RESULT_DBURI = "oracle://scott:tiger@127.0.0.1:1521/sidname"

See `Connection String`_ for more information about connection
strings.

.. setting:: CELERY_RESULT_ENGINE_OPTIONS

CELERY_RESULT_ENGINE_OPTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To specify additional SQLAlchemy database engine options you can use
the :setting:`CELERY_RESULT_ENGINE_OPTIONS` setting::

    # echo enables verbose logging from SQLAlchemy.
    CELERY_RESULT_ENGINE_OPTIONS = {"echo": True}


.. setting:: CELERY_RESULT_DB_SHORT_LIVED_SESSIONS
    CELERY_RESULT_DB_SHORT_LIVED_SESSIONS = True

Short lived sessions are disabled by default.  If enabled they can drastically reduce
performance, especially on systems processing lots of tasks.  This option is useful
on low-traffic workers that experience errors as a result of cached database connections
going stale through inactivity.  For example, intermittent errors like
`(OperationalError) (2006, 'MySQL server has gone away')` can be fixed by enabling
short lived sessions.  This option only affects the database backend.

.. _`Supported Databases`:
    http://www.sqlalchemy.org/docs/core/engines.html#supported-databases

.. _`Connection String`:
    http://www.sqlalchemy.org/docs/core/engines.html#database-urls

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CELERY_RESULT_BACKEND = "database"
    CELERY_RESULT_DBURI = "mysql://user:password@host/dbname"

.. _conf-amqp-result-backend:

AMQP backend settings
---------------------

.. note::

    The AMQP backend requires RabbitMQ 1.1.0 or higher to automatically
    expire results.  If you are running an older version of RabbitmQ
    you should disable result expiration like this:

        CELERY_TASK_RESULT_EXPIRES = None

.. setting:: CELERY_RESULT_EXCHANGE

CELERY_RESULT_EXCHANGE
~~~~~~~~~~~~~~~~~~~~~~

Name of the exchange to publish results in.  Default is `"celeryresults"`.

.. setting:: CELERY_RESULT_EXCHANGE_TYPE

CELERY_RESULT_EXCHANGE_TYPE
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The exchange type of the result exchange.  Default is to use a `direct`
exchange.

.. setting:: CELERY_RESULT_PERSISTENT

CELERY_RESULT_PERSISTENT
~~~~~~~~~~~~~~~~~~~~~~~~

If set to :const:`True`, result messages will be persistent.  This means the
messages will not be lost after a broker restart.  The default is for the
results to be transient.

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CELERY_RESULT_BACKEND = "amqp"
    CELERY_TASK_RESULT_EXPIRES = 18000  # 5 hours.

.. _conf-cache-result-backend:

Cache backend settings
----------------------

.. note::

    The cache backend supports the `pylibmc`_ and `python-memcached`
    libraries.  The latter is used only if `pylibmc`_ is not installed.

.. setting:: CELERY_CACHE_BACKEND

CELERY_CACHE_BACKEND
~~~~~~~~~~~~~~~~~~~~

Using a single memcached server:

.. code-block:: python

    CELERY_CACHE_BACKEND = 'memcached://127.0.0.1:11211/'

Using multiple memcached servers:

.. code-block:: python

    CELERY_RESULT_BACKEND = "cache"
    CELERY_CACHE_BACKEND = 'memcached://172.19.26.240:11211;172.19.26.242:11211/'

.. setting:: CELERY_CACHE_BACKEND_OPTIONS


The "dummy" backend stores the cache in memory only:

    CELERY_CACHE_BACKEND = "dummy"

CELERY_CACHE_BACKEND_OPTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can set pylibmc options using the :setting:`CELERY_CACHE_BACKEND_OPTIONS`
setting:

.. code-block:: python

    CELERY_CACHE_BACKEND_OPTIONS = {"binary": True,
                                    "behaviors": {"tcp_nodelay": True}}

.. _`pylibmc`: http://sendapatch.se/projects/pylibmc/

.. _conf-tyrant-result-backend:

Tokyo Tyrant backend settings
-----------------------------

.. note::

    The Tokyo Tyrant backend requires the :mod:`pytyrant` library:
    http://pypi.python.org/pypi/pytyrant/

This backend requires the following configuration directives to be set:

.. setting:: TT_HOST

TT_HOST
~~~~~~~

Host name of the Tokyo Tyrant server.

.. setting:: TT_PORT

TT_PORT
~~~~~~~

The port the Tokyo Tyrant server is listening to.


Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CELERY_RESULT_BACKEND = "tyrant"
    TT_HOST = "localhost"
    TT_PORT = 1978

.. _conf-redis-result-backend:

Redis backend settings
----------------------

.. note::

    The Redis backend requires the :mod:`redis` library:
    http://pypi.python.org/pypi/redis/

    To install the redis package use `pip` or `easy_install`::

        $ pip install redis

This backend requires the following configuration directives to be set.

.. setting:: CELERY_REDIS_HOST

CELERY_REDIS_HOST
~~~~~~~~~~~~~~~~~

Host name of the Redis database server. e.g. `"localhost"`.

.. setting:: CELERY_REDIS_PORT

CELERY_REDIS_PORT
~~~~~~~~~~~~~~~~~

Port to the Redis database server. e.g. `6379`.

.. setting:: CELERY_REDIS_DB

CELERY_REDIS_DB
~~~~~~~~~~~~~~~

Database number to use. Default is 0

.. setting:: CELERY_REDIS_PASSWORD

CELERY_REDIS_PASSWORD
~~~~~~~~~~~~~~~~~~~~~

Password used to connect to the database.

.. setting:: CELERY_REDIS_MAX_CONNECTIONS

CELERY_REDIS_MAX_CONNECTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum number of connections available in the Redis connection
pool used for sending and retrieving results.

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CELERY_RESULT_BACKEND = "redis"
    CELERY_REDIS_HOST = "localhost"
    CELERY_REDIS_PORT = 6379
    CELERY_REDIS_DB = 0

.. _conf-mongodb-result-backend:

MongoDB backend settings
------------------------

.. note::

    The MongoDB backend requires the :mod:`pymongo` library:
    http://github.com/mongodb/mongo-python-driver/tree/master

.. setting:: CELERY_MONGODB_BACKEND_SETTINGS

CELERY_MONGODB_BACKEND_SETTINGS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a dict supporting the following keys:

* host
    Host name of the MongoDB server. Defaults to "localhost".

* port
    The port the MongoDB server is listening to. Defaults to 27017.

* user
    User name to authenticate to the MongoDB server as (optional).

* password
    Password to authenticate to the MongoDB server (optional).

* database
    The database name to connect to. Defaults to "celery".

* taskmeta_collection
    The collection name to store task meta data.
    Defaults to "celery_taskmeta".

.. _example-mongodb-result-config:

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CELERY_RESULT_BACKEND = "mongodb"
    CELERY_MONGODB_BACKEND_SETTINGS = {
        "host": "192.168.1.100",
        "port": 30000,
        "database": "mydb",
        "taskmeta_collection": "my_taskmeta_collection",
    }

.. _conf-cassandra-result-backend:

Cassandra backend settings
--------------------------

.. note::

    The Cassandra backend requires the :mod:`pycassa` library:
    http://pypi.python.org/pypi/pycassa/

    To install the pycassa package use `pip` or `easy_install`::

        $ pip install pycassa

This backend requires the following configuration directives to be set.

.. setting:: CASSANDRA_SERVERS

CASSANDRA_SERVERS
~~~~~~~~~~~~~~~~~

List of ``host:port`` Cassandra servers. e.g. ``["localhost:9160]"``.

.. setting:: CASSANDRA_KEYSPACE

CASSANDRA_KEYSPACE
~~~~~~~~~~~~~~~~~~

The keyspace in which to store the results. e.g. ``"tasks_keyspace"``.

.. setting:: CASSANDRA_COLUMN_FAMILY

CASSANDRA_COLUMN_FAMILY
~~~~~~~~~~~~~~~~~~~~~~~

The column family in which to store the results. eg ``"tasks"``

.. setting:: CASSANDRA_READ_CONSISTENCY

CASSANDRA_READ_CONSISTENCY
~~~~~~~~~~~~~~~~~~~~~~~~~~

The read consistency used. Values can be ``"ONE"``, ``"QUORUM"`` or ``"ALL"``.

.. setting:: CASSANDRA_WRITE_CONSISTENCY

CASSANDRA_WRITE_CONSISTENCY
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The write consistency used. Values can be ``"ONE"``, ``"QUORUM"`` or ``"ALL"``.

.. setting:: CASSANDRA_DETAILED_MODE

CASSANDRA_DETAILED_MODE
~~~~~~~~~~~~~~~~~~~~~~~

Enable or disable detailed mode. Default is :const:`False`.
This mode allows to use the power of Cassandra wide columns to
store all states for a task as a wide column, instead of only the last one.

To use this mode, you need to configure your ColumnFamily to
use the ``TimeUUID`` type as a comparator::

    create column family task_results with comparator = TimeUUIDType;

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CASSANDRA_SERVERS = ["localhost:9160"]
    CASSANDRA_KEYSPACE = "celery"
    CASSANDRA_COLUMN_FAMILY = "task_results"
    CASSANDRA_READ_CONSISTENCY = "ONE"
    CASSANDRA_WRITE_CONSISTENCY = "ONE"
    CASSANDRA_DETAILED_MODE = True

.. _conf-messaging:

Message Routing
---------------

.. _conf-messaging-routing:

.. setting:: CELERY_QUEUES

CELERY_QUEUES
~~~~~~~~~~~~~

The mapping of queues the worker consumes from.  This is a dictionary
of queue name/options.  See :ref:`guide-routing` for more information.

The default is a queue/exchange/binding key of `"celery"`, with
exchange type `direct`.

You don't have to care about this unless you want custom routing facilities.

.. setting:: CELERY_ROUTES

CELERY_ROUTES
~~~~~~~~~~~~~

A list of routers, or a single router used to route tasks to queues.
When deciding the final destination of a task the routers are consulted
in order.  See :ref:`routers` for more information.

.. setting:: CELERY_CREATE_MISSING_QUEUES

CELERY_CREATE_MISSING_QUEUES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled (default), any queues specified that is not defined in
:setting:`CELERY_QUEUES` will be automatically created. See
:ref:`routing-automatic`.

.. setting:: CELERY_DEFAULT_QUEUE

CELERY_DEFAULT_QUEUE
~~~~~~~~~~~~~~~~~~~~

The queue used by default, if no custom queue is specified.  This queue must
be listed in :setting:`CELERY_QUEUES`.  The default is: `celery`.

.. seealso::

    :ref:`routing-changing-default-queue`

.. setting:: CELERY_DEFAULT_EXCHANGE

CELERY_DEFAULT_EXCHANGE
~~~~~~~~~~~~~~~~~~~~~~~

Name of the default exchange to use when no custom exchange is
specified.  The default is: `celery`.

.. setting:: CELERY_DEFAULT_EXCHANGE_TYPE

CELERY_DEFAULT_EXCHANGE_TYPE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default exchange type used when no custom exchange is specified.
The default is: `direct`.

.. setting:: CELERY_DEFAULT_ROUTING_KEY

CELERY_DEFAULT_ROUTING_KEY
~~~~~~~~~~~~~~~~~~~~~~~~~~

The default routing key used when sending tasks.
The default is: `celery`.

.. setting:: CELERY_DEFAULT_DELIVERY_MODE

CELERY_DEFAULT_DELIVERY_MODE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Can be `transient` or `persistent`.  The default is to send
persistent messages.

.. _conf-broker-settings:

Broker Settings
---------------

.. setting:: BROKER_TRANSPORT

BROKER_TRANSPORT
~~~~~~~~~~~~~~~~
:Aliases: ``BROKER_BACKEND``
:Deprecated aliases: ``CARROT_BACKEND``

The Kombu transport to use.  Default is ``amqplib``.

You can use a custom transport class name, or select one of the
built-in transports: ``amqplib``, ``pika``, ``redis``, ``beanstalk``,
``sqlalchemy``, ``django``, ``mongodb``, ``couchdb``.

.. setting:: BROKER_URL

BROKER_URL
~~~~~~~~~~

Default broker URL.  This must be an URL in the form of::

    transport://userid:password@hostname:port/virtual_host

Only the scheme part (``transport://``) is required, the rest
is optional, and defaults to the specific transports default values.

If this setting is defined it will override a subset of the
other ``BROKER`` options. These options are :setting:`BROKER_HOST`,
:setting:`BROKER_USER`, :setting:`BROKER_PASSWORD`, :setting:`BROKER_PORT`,
and :setting:`BROKER_VHOST`.

See the Kombu documentation for more information about broker URLs.

.. setting:: BROKER_HOST

BROKER_HOST
~~~~~~~~~~~

Hostname of the broker.

.. setting:: BROKER_PORT

BROKER_PORT
~~~~~~~~~~~

Custom port of the broker.  Default is to use the default port for the
selected backend.

.. setting:: BROKER_USER

BROKER_USER
~~~~~~~~~~~

Username to connect as.

.. setting:: BROKER_PASSWORD

BROKER_PASSWORD
~~~~~~~~~~~~~~~

Password to connect with.

.. setting:: BROKER_VHOST

BROKER_VHOST
~~~~~~~~~~~~

Virtual host.  Default is `"/"`.

.. setting:: BROKER_USE_SSL

BROKER_USE_SSL
~~~~~~~~~~~~~~

Use SSL to connect to the broker.  Off by default.  This may not be supported
by all transports.

.. setting:: BROKER_POOL_LIMIT

BROKER_POOL_LIMIT
~~~~~~~~~~~~~~~~~

.. versionadded:: 2.3

The maximum number of connections that can be open in the connection pool.

The pool is enabled by default since version 2.5, with a default limit of ten
connections.  This number can be tweaked depending on the number of
threads/greenthreads (eventlet/gevent) using a connection.  For example
running eventlet with 1000 greenlets that use a connection to the broker,
contention can arise and you should consider increasing the limit.

If set to :const:`None` or 0 the connection pool will be disabled and
connections will be established and closed for every use.

Default (since 2.5) is to use a pool of 10 connections.

.. setting:: BROKER_CONNECTION_TIMEOUT

BROKER_CONNECTION_TIMEOUT
~~~~~~~~~~~~~~~~~~~~~~~~~

The default timeout in seconds before we give up establishing a connection
to the AMQP server.  Default is 4 seconds.

.. setting:: BROKER_CONNECTION_RETRY

BROKER_CONNECTION_RETRY
~~~~~~~~~~~~~~~~~~~~~~~

Automatically try to re-establish the connection to the AMQP broker if lost.

The time between retries is increased for each retry, and is
not exhausted before :setting:`BROKER_CONNECTION_MAX_RETRIES` is
exceeded.

This behavior is on by default.

.. setting:: BROKER_CONNECTION_MAX_RETRIES

BROKER_CONNECTION_MAX_RETRIES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum number of retries before we give up re-establishing a connection
to the AMQP broker.

If this is set to :const:`0` or :const:`None`, we will retry forever.

Default is 100 retries.

.. setting:: BROKER_TRANSPORT_OPTIONS

BROKER_TRANSPORT_OPTIONS
~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

A dict of additional options passed to the underlying transport.

See your transport user manual for supported options (if any).

.. _conf-task-execution:

Task execution settings
-----------------------

.. setting:: CELERY_ALWAYS_EAGER

CELERY_ALWAYS_EAGER
~~~~~~~~~~~~~~~~~~~

If this is :const:`True`, all tasks will be executed locally by blocking until
the task returns.  ``apply_async()`` and ``Task.delay()`` will return
an :class:`~celery.result.EagerResult` instance, which emulates the API
and behavior of :class:`~celery.result.AsyncResult`, except the result
is already evaluated.

That is, tasks will be executed locally instead of being sent to
the queue.

.. setting:: CELERY_EAGER_PROPAGATES_EXCEPTIONS

CELERY_EAGER_PROPAGATES_EXCEPTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If this is :const:`True`, eagerly executed tasks (applied by `task.apply()`,
or when the :setting:`CELERY_ALWAYS_EAGER` setting is enabled), will
propagate exceptions.

It's the same as always running ``apply()`` with ``throw=True``.

.. setting:: CELERY_IGNORE_RESULT

CELERY_IGNORE_RESULT
~~~~~~~~~~~~~~~~~~~~

Whether to store the task return values or not (tombstones).
If you still want to store errors, just not successful return values,
you can set :setting:`CELERY_STORE_ERRORS_EVEN_IF_IGNORED`.

.. setting:: CELERY_MESSAGE_COMPRESSION

CELERY_MESSAGE_COMPRESSION
~~~~~~~~~~~~~~~~~~~~~~~~~~

Default compression used for task messages.
Can be ``"gzip"``, ``"bzip2"`` (if available), or any custom
compression schemes registered in the Kombu compression registry.

The default is to send uncompressed messages.

.. setting:: CELERY_TASK_RESULT_EXPIRES

CELERY_TASK_RESULT_EXPIRES
~~~~~~~~~~~~~~~~~~~~~~~~~~

Time (in seconds, or a :class:`~datetime.timedelta` object) for when after
stored task tombstones will be deleted.

A built-in periodic task will delete the results after this time
(:class:`celery.task.backend_cleanup`).

.. note::

    For the moment this only works with the amqp, database, cache, redis and MongoDB
    backends.

    When using the database or MongoDB backends, `celerybeat` must be
    running for the results to be expired.

.. setting:: CELERY_MAX_CACHED_RESULTS

CELERY_MAX_CACHED_RESULTS
~~~~~~~~~~~~~~~~~~~~~~~~~

Result backends caches ready results used by the client.

This is the total number of results to cache before older results are evicted.
The default is 5000.

.. setting:: CELERY_TRACK_STARTED

CELERY_TRACK_STARTED
~~~~~~~~~~~~~~~~~~~~

If :const:`True` the task will report its status as "started" when the
task is executed by a worker.  The default value is :const:`False` as
the normal behaviour is to not report that level of granularity.  Tasks
are either pending, finished, or waiting to be retried.  Having a "started"
state can be useful for when there are long running tasks and there is a
need to report which task is currently running.

.. setting:: CELERY_TASK_SERIALIZER

CELERY_TASK_SERIALIZER
~~~~~~~~~~~~~~~~~~~~~~

A string identifying the default serialization method to use.  Can be
`pickle` (default), `json`, `yaml`, `msgpack` or any custom serialization
methods that have been registered with :mod:`kombu.serialization.registry`.

.. seealso::

    :ref:`executing-serializers`.

.. setting:: CELERY_TASK_PUBLISH_RETRY

CELERY_TASK_PUBLISH_RETRY
~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Decides if publishing task messages will be retried in the case
of connection loss or other connection errors.
See also :setting:`CELERY_TASK_PUBLISH_RETRY_POLICY`.

Disabled by default.

.. setting:: CELERY_TASK_PUBLISH_RETRY_POLICY

CELERY_TASK_PUBLISH_RETRY_POLICY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Defines the default policy when retrying publishing a task message in
the case of connection loss or other connection errors.

This is a mapping that must contain the following keys:

    * `max_retries`

        Maximum number of retries before giving up, in this case the
        exception that caused the retry to fail will be raised.

        A value of 0 or :const:`None` means it will retry forever.

        The default is to retry 3 times.

    * `interval_start`

        Defines the number of seconds (float or integer) to wait between
        retries.  Default is 0, which means the first retry will be
        instantaneous.

    * `interval_step`

        On each consecutive retry this number will be added to the retry
        delay (float or integer).  Default is 0.2.

    * `interval_max`

        Maximum number of seconds (float or integer) to wait between
        retries.  Default is 0.2.

With the default policy of::

    {"max_retries": 3,
     "interval_start": 0,
     "interval_step": 0.2,
     "interval_max": 0.2}

the maximum time spent retrying will be 0.4 seconds.  It is set relatively
short by default because a connection failure could lead to a retry pile effect
if the broker connection is down: e.g. many web server processes waiting
to retry blocking other incoming requests.


.. setting:: CELERY_DEFAULT_RATE_LIMIT

CELERY_DEFAULT_RATE_LIMIT
~~~~~~~~~~~~~~~~~~~~~~~~~

The global default rate limit for tasks.

This value is used for tasks that does not have a custom rate limit
The default is no rate limit.

.. setting:: CELERY_DISABLE_RATE_LIMITS

CELERY_DISABLE_RATE_LIMITS
~~~~~~~~~~~~~~~~~~~~~~~~~~

Disable all rate limits, even if tasks has explicit rate limits set.

.. setting:: CELERY_ACKS_LATE

CELERY_ACKS_LATE
~~~~~~~~~~~~~~~~

Late ack means the task messages will be acknowledged **after** the task
has been executed, not *just before*, which is the default behavior.

.. seealso::

    FAQ: :ref:`faq-acks_late-vs-retry`.

.. _conf-celeryd:

Worker: celeryd
---------------

.. setting:: CELERY_IMPORTS

CELERY_IMPORTS
~~~~~~~~~~~~~~

A sequence of modules to import when the celery daemon starts.

This is used to specify the task modules to import, but also
to import signal handlers and additional remote control commands, etc.

.. setting:: CELERYD_FORCE_EXECV

CELERYD_FORCE_EXECV
~~~~~~~~~~~~~~~~~~~

On Unix the processes pool will fork, so that child processes
start with the same memory as the parent process.

This can cause problems as there is a known deadlock condition
with pthread locking primitives when `fork()` is combined with threads.

You should enable this setting if you are experiencing hangs (deadlocks),
especially in combination with time limits or having a max tasks per child limit.

This option will be enabled by default in a later version.

This is not a problem on Windows, as it does not have `fork()`.

.. setting:: CELERYD_MAX_TASKS_PER_CHILD

CELERYD_MAX_TASKS_PER_CHILD
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum number of tasks a pool worker process can execute before
it's replaced with a new one.  Default is no limit.

.. setting:: CELERYD_TASK_TIME_LIMIT

CELERYD_TASK_TIME_LIMIT
~~~~~~~~~~~~~~~~~~~~~~~

Task hard time limit in seconds.  The worker processing the task will
be killed and replaced with a new one when this is exceeded.

.. setting:: CELERYD_TASK_SOFT_TIME_LIMIT

CELERYD_TASK_SOFT_TIME_LIMIT
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Task soft time limit in seconds.

The :exc:`~celery.exceptions.SoftTimeLimitExceeded` exception will be
raised when this is exceeded.  The task can catch this to
e.g. clean up before the hard time limit comes.

Example:

.. code-block:: python

    from celery.task import task
    from celery.exceptions import SoftTimeLimitExceeded

    @task()
    def mytask():
        try:
            return do_work()
        except SoftTimeLimitExceeded:
            cleanup_in_a_hurry()

.. setting:: CELERY_STORE_ERRORS_EVEN_IF_IGNORED

CELERY_STORE_ERRORS_EVEN_IF_IGNORED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If set, the worker stores all task errors in the result store even if
:attr:`Task.ignore_result <celery.task.base.Task.ignore_result>` is on.

.. setting:: CELERYD_STATE_DB

CELERYD_STATE_DB
~~~~~~~~~~~~~~~~

Name of the file used to stores persistent worker state (like revoked tasks).
Can be a relative or absolute path, but be aware that the suffix `.db`
may be appended to the file name (depending on Python version).

Can also be set via the :option:`--statedb` argument to
:mod:`~celery.bin.celeryd`.

Not enabled by default.

.. setting:: CELERYD_ETA_SCHEDULER_PRECISION

CELERYD_ETA_SCHEDULER_PRECISION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set the maximum time in seconds that the ETA scheduler can sleep between
rechecking the schedule.  Default is 1 second.

Setting this value to 1 second means the schedulers precision will
be 1 second. If you need near millisecond precision you can set this to 0.1.

.. _conf-error-mails:

Error E-Mails
-------------

.. setting:: CELERY_SEND_TASK_ERROR_EMAILS

CELERY_SEND_TASK_ERROR_EMAILS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default value for the `Task.send_error_emails` attribute, which if
set to :const:`True` means errors occurring during task execution will be
sent to :setting:`ADMINS` by email.

Disabled by default.

.. setting:: ADMINS

ADMINS
~~~~~~

List of `(name, email_address)` tuples for the administrators that should
receive error emails.

.. setting:: SERVER_EMAIL

SERVER_EMAIL
~~~~~~~~~~~~

The email address this worker sends emails from.
Default is celery@localhost.

.. setting:: EMAIL_HOST

EMAIL_HOST
~~~~~~~~~~

The mail server to use.  Default is `"localhost"`.

.. setting:: EMAIL_HOST_USER

EMAIL_HOST_USER
~~~~~~~~~~~~~~~

User name (if required) to log on to the mail server with.

.. setting:: EMAIL_HOST_PASSWORD

EMAIL_HOST_PASSWORD
~~~~~~~~~~~~~~~~~~~

Password (if required) to log on to the mail server with.

.. setting:: EMAIL_PORT

EMAIL_PORT
~~~~~~~~~~

The port the mail server is listening on.  Default is `25`.


.. setting:: EMAIL_USE_SSL

EMAIL_USE_SSL
~~~~~~~~~~~~~

Use SSL when connecting to the SMTP server.  Disabled by default.

.. setting:: EMAIL_USE_TLS

EMAIL_USE_TLS
~~~~~~~~~~~~~

Use TLS when connecting to the SMTP server.  Disabled by default.

.. setting:: EMAIL_TIMEOUT

EMAIL_TIMEOUT
~~~~~~~~~~~~~

Timeout in seconds for when we give up trying to connect
to the SMTP server when sending emails.

The default is 2 seconds.

.. _conf-example-error-mail-config:

Example E-Mail configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This configuration enables the sending of error emails to
george@vandelay.com and kramer@vandelay.com:

.. code-block:: python

    # Enables error emails.
    CELERY_SEND_TASK_ERROR_EMAILS = True

    # Name and email addresses of recipients
    ADMINS = (
        ("George Costanza", "george@vandelay.com"),
        ("Cosmo Kramer", "kosmo@vandelay.com"),
    )

    # Email address used as sender (From field).
    SERVER_EMAIL = "no-reply@vandelay.com"

    # Mailserver configuration
    EMAIL_HOST = "mail.vandelay.com"
    EMAIL_PORT = 25
    # EMAIL_HOST_USER = "servers"
    # EMAIL_HOST_PASSWORD = "s3cr3t"

.. _conf-events:

Events
------

.. setting:: CELERY_SEND_EVENTS

CELERY_SEND_EVENTS
~~~~~~~~~~~~~~~~~~

Send events so the worker can be monitored by tools like `celerymon`.

.. setting:: CELERY_SEND_TASK_SENT_EVENT

CELERY_SEND_TASK_SENT_EVENT
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

If enabled, a `task-sent` event will be sent for every task so tasks can be
tracked before they are consumed by a worker.

Disabled by default.

.. setting:: CELERY_EVENT_SERIALIZER

CELERY_EVENT_SERIALIZER
~~~~~~~~~~~~~~~~~~~~~~~

Message serialization format used when sending event messages.
Default is `"json"`. See :ref:`executing-serializers`.

.. _conf-broadcast:

Broadcast Commands
------------------

.. setting:: CELERY_BROADCAST_QUEUE

CELERY_BROADCAST_QUEUE
~~~~~~~~~~~~~~~~~~~~~~

Name prefix for the queue used when listening for broadcast messages.
The workers host name will be appended to the prefix to create the final
queue name.

Default is `"celeryctl"`.

.. setting:: CELERY_BROADCAST_EXCHANGE

CELERY_BROADCAST_EXCHANGE
~~~~~~~~~~~~~~~~~~~~~~~~~

Name of the exchange used for broadcast messages.

Default is `"celeryctl"`.

.. setting:: CELERY_BROADCAST_EXCHANGE_TYPE

CELERY_BROADCAST_EXCHANGE_TYPE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Exchange type used for broadcast messages.  Default is `"fanout"`.

.. _conf-logging:

Logging
-------

.. setting:: CELERYD_HIJACK_ROOT_LOGGER

CELERYD_HIJACK_ROOT_LOGGER
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

By default any previously configured logging options will be reset,
because the Celery programs "hijacks" the root logger.

If you want to customize your own logging then you can disable
this behavior.

.. note::

    Logging can also be customized by connecting to the
    :signal:`celery.signals.setup_logging` signal.

.. setting:: CELERYD_LOG_COLOR

CELERYD_LOG_COLOR
~~~~~~~~~~~~~~~~~

Enables/disables colors in logging output by the Celery apps.

By default colors are enabled if

    1) the app is logging to a real terminal, and not a file.
    2) the app is not running on Windows.

.. setting:: CELERYD_LOG_FORMAT

CELERYD_LOG_FORMAT
~~~~~~~~~~~~~~~~~~

The format to use for log messages.

Default is `[%(asctime)s: %(levelname)s/%(processName)s] %(message)s`

See the Python :mod:`logging` module for more information about log
formats.

.. setting:: CELERYD_TASK_LOG_FORMAT

CELERYD_TASK_LOG_FORMAT
~~~~~~~~~~~~~~~~~~~~~~~

The format to use for log messages logged in tasks.  Can be overridden using
the :option:`--loglevel` option to :mod:`~celery.bin.celeryd`.

Default is::

    [%(asctime)s: %(levelname)s/%(processName)s]
        [%(task_name)s(%(task_id)s)] %(message)s

See the Python :mod:`logging` module for more information about log
formats.

.. setting:: CELERY_REDIRECT_STDOUTS

CELERY_REDIRECT_STDOUTS
~~~~~~~~~~~~~~~~~~~~~~~

If enabled `stdout` and `stderr` will be redirected
to the current logger.

Enabled by default.
Used by :program:`celeryd` and :program:`celerybeat`.

.. setting:: CELERY_REDIRECT_STDOUTS_LEVEL

CELERY_REDIRECT_STDOUTS_LEVEL
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The log level output to `stdout` and `stderr` is logged as.
Can be one of :const:`DEBUG`, :const:`INFO`, :const:`WARNING`,
:const:`ERROR` or :const:`CRITICAL`.

Default is :const:`WARNING`.

.. _conf-security:

Security
--------

.. setting:: CELERY_SECURITY_KEY

CELERY_SECURITY_KEY
~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.5

The relative or absolute path to a file containing the private key
used to sign messages when :ref:`message-signing` is used.

.. setting:: CELERY_SECURITY_CERTIFICATE

CELERY_SECURITY_CERTIFICATE
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.5

The relative or absolute path to an X.509 certificate file
used to sign messages when :ref:`message-signing` is used.

.. setting:: CELERY_SECURITY_CERT_STORE

CELERY_SECURITY_CERT_STORE
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.5

The directory containing X.509 certificates used for
:ref:`message-signing`.  Can be a glob with wildcards,
(for example :file:`/etc/certs/*.pem`).

.. _conf-custom-components:

Custom Component Classes (advanced)
-----------------------------------

.. setting:: CELERYD_BOOT_STEPS

CELERYD_BOOT_STEPS
~~~~~~~~~~~~~~~~~~

This setting enables you to add additional components to the worker process.
It should be a list of module names with :class:`celery.abstract.Component`
classes, that augments functionality in the worker.

.. setting:: CELERYD_POOL

CELERYD_POOL
~~~~~~~~~~~~

Name of the pool class used by the worker.

You can use a custom pool class name, or select one of
the built-in aliases: ``processes``, ``eventlet``, ``gevent``.

Default is ``processes``.

.. setting:: CELERYD_AUTOSCALER

CELERYD_AUTOSCALER
~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Name of the autoscaler class to use.

Default is ``"celery.worker.autoscale.Autoscaler"``.

.. setting:: CELERYD_AUTORELOADER

CELERYD_AUTORELOADER
~~~~~~~~~~~~~~~~~~~~

Name of the autoreloader class used by the worker to reload
Python modules and files that have changed.

Default is: ``"celery.worker.autoreload.Autoreloader"``.

.. setting:: CELERYD_CONSUMER

CELERYD_CONSUMER
~~~~~~~~~~~~~~~~

Name of the consumer class used by the worker.
Default is :class:`celery.worker.consumer.Consumer`

.. setting:: CELERYD_MEDIATOR

CELERYD_MEDIATOR
~~~~~~~~~~~~~~~~

Name of the mediator class used by the worker.
Default is :class:`celery.worker.controllers.Mediator`.

.. setting:: CELERYD_ETA_SCHEDULER

CELERYD_ETA_SCHEDULER
~~~~~~~~~~~~~~~~~~~~~

Name of the ETA scheduler class used by the worker.
Default is :class:`celery.utils.timer2.Timer`, or one overrided
by the pool implementation.

.. _conf-celerybeat:

Periodic Task Server: celerybeat
--------------------------------

.. setting:: CELERYBEAT_SCHEDULE

CELERYBEAT_SCHEDULE
~~~~~~~~~~~~~~~~~~~

The periodic task schedule used by :mod:`~celery.bin.celerybeat`.
See :ref:`beat-entries`.

.. setting:: CELERYBEAT_SCHEDULER

CELERYBEAT_SCHEDULER
~~~~~~~~~~~~~~~~~~~~

The default scheduler class.  Default is
`"celery.beat.PersistentScheduler"`.

Can also be set via the :option:`-S` argument to
:mod:`~celery.bin.celerybeat`.

.. setting:: CELERYBEAT_SCHEDULE_FILENAME

CELERYBEAT_SCHEDULE_FILENAME
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Name of the file used by `PersistentScheduler` to store the last run times
of periodic tasks.  Can be a relative or absolute path, but be aware that the
suffix `.db` may be appended to the file name (depending on Python version).

Can also be set via the :option:`--schedule` argument to
:mod:`~celery.bin.celerybeat`.

.. setting:: CELERYBEAT_MAX_LOOP_INTERVAL

CELERYBEAT_MAX_LOOP_INTERVAL
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The maximum number of seconds :mod:`~celery.bin.celerybeat` can sleep
between checking the schedule.  Default is 300 seconds (5 minutes).


.. _conf-celerymon:

Monitor Server: celerymon
-------------------------


.. setting:: CELERYMON_LOG_FORMAT

CELERYMON_LOG_FORMAT
~~~~~~~~~~~~~~~~~~~~

The format to use for log messages.

Default is `[%(asctime)s: %(levelname)s/%(processName)s] %(message)s`

See the Python :mod:`logging` module for more information about log
formats.

.. _conf-deprecated:

Deprecated Settings
-------------------

These settings have been deprecated and should no longer used,
as they will be removed in future versions.

.. setting:: CELERY_AMQP_TASK_RESULT_EXPIRES

CELERY_AMQP_TASK_RESULT_EXPIRES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. deprecated:: 2.5

The time in seconds of which the task result queues should expire.

This setting is deprecated, and will be removed in version 3.0.
Please use :setting:`CELERY_TASK_RESULT_EXPIRES` instead.

.. note::

    AMQP result expiration requires RabbitMQ versions 2.1.0 or higher.

.. setting:: CELERY_TASK_ERROR_WHITELIST

CELERY_TASK_ERROR_WHITELIST
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. deprecated:: 2.5

A white list of exceptions to send error emails for.

This option is pending deprecation and is scheduled for removal
in version 3.0.

.. setting:: CELERYD_LOG_FILE

CELERYD_LOG_FILE
~~~~~~~~~~~~~~~~

.. deprecated:: 2.4

This option is deprecated and is scheduled for removal in version 3.0.
Please use the :option:`--logfile` argument instead.

The default file name the worker daemon logs messages to.  Can be overridden
using the :option:`--logfile` option to :mod:`~celery.bin.celeryd`.

The default is :const:`None` (`stderr`)

.. setting:: CELERYD_LOG_LEVEL

CELERYD_LOG_LEVEL
~~~~~~~~~~~~~~~~~

.. deprecated:: 2.4

This option is deprecated and is scheduled for removal in version 3.0.
Please use the :option:`--loglevel` argument instead.

Worker log level, can be one of :const:`DEBUG`, :const:`INFO`, :const:`WARNING`,
:const:`ERROR` or :const:`CRITICAL`.

Can also be set via the :option:`--loglevel` argument to
:mod:`~celery.bin.celeryd`.

See the :mod:`logging` module for more information.

.. setting:: CELERYBEAT_LOG_FILE

CELERYBEAT_LOG_FILE
~~~~~~~~~~~~~~~~~~~

.. deprecated:: 2.4

This option is deprecated and is scheduled for removal in version 3.0.
Please use the :option:`--logfile` argument instead.

The default file name to log messages to.  Can be overridden using
the `--logfile` option to :mod:`~celery.bin.celerybeat`.

The default is :const:`None` (`stderr`).

.. setting:: CELERYBEAT_LOG_LEVEL

CELERYBEAT_LOG_LEVEL
~~~~~~~~~~~~~~~~~~~~

.. deprecated:: 2.4

This option is deprecated and is scheduled for removal in version 3.0.
Please use the :option:`--loglevel` argument instead.

Logging level. Can be any of :const:`DEBUG`, :const:`INFO`, :const:`WARNING`,
:const:`ERROR`, or :const:`CRITICAL`.

Can also be set via the :option:`--loglevel` argument to
:mod:`~celery.bin.celerybeat`.

See the :mod:`logging` module for more information.

.. setting:: CELERYMON_LOG_FILE

CELERYMON_LOG_FILE
~~~~~~~~~~~~~~~~~~

.. deprecated:: 2.4

This option is deprecated and is scheduled for removal in version 3.0.
Please use the :option:`--logfile` argument instead.

The default file name to log messages to.  Can be overridden using
the :option:`--logfile` argument to `celerymon`.

The default is :const:`None` (`stderr`)

.. setting:: CELERYMON_LOG_LEVEL

CELERYMON_LOG_LEVEL
~~~~~~~~~~~~~~~~~~~

.. deprecated:: 2.4

This option is deprecated and is scheduled for removal in version 3.0.
Please use the :option:`--loglevel` argument instead.

Logging level. Can be any of :const:`DEBUG`, :const:`INFO`, :const:`WARNING`,
:const:`ERROR`, or :const:`CRITICAL`.

See the :mod:`logging` module for more information.
