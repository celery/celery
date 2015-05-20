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

    ## Broker settings.
    BROKER_URL = 'amqp://guest:guest@localhost:5672//'

    # List of modules to import when celery starts.
    CELERY_IMPORTS = ('myapp.tasks', )

    ## Using the database to store task state and results.
    CELERY_RESULT_BACKEND = 'db+sqlite:///results.db'

    CELERY_ANNOTATIONS = {'tasks.add': {'rate_limit': '10/s'}}


Configuration Directives
========================

.. _conf-datetime:

Time and date settings
----------------------

.. setting:: CELERY_ENABLE_UTC

CELERY_ENABLE_UTC
~~~~~~~~~~~~~~~~~

.. versionadded:: 2.5

If enabled dates and times in messages will be converted to use
the UTC timezone.

Note that workers running Celery versions below 2.5 will assume a local
timezone for all messages, so only enable if all workers have been
upgraded.

Enabled by default since version 3.0.

.. setting:: CELERY_TIMEZONE

CELERY_TIMEZONE
~~~~~~~~~~~~~~~

Configure Celery to use a custom time zone.
The timezone value can be any time zone supported by the `pytz`_
library.

If not set the UTC timezone is used.  For backwards compatibility
there is also a :setting:`CELERY_ENABLE_UTC` setting, and this is set
to false the system local timezone is used instead.

.. _`pytz`: http://pypi.python.org/pypi/pytz/



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

    CELERY_ANNOTATIONS = {'tasks.add': {'rate_limit': '10/s'}}

or change the same for all tasks:

.. code-block:: python

    CELERY_ANNOTATIONS = {'*': {'rate_limit': '10/s'}}


You can change methods too, for example the ``on_failure`` handler:

.. code-block:: python

    def my_on_failure(self, exc, task_id, args, kwargs, einfo):
        print('Oh no! Task failed: {0!r}'.format(exc))

    CELERY_ANNOTATIONS = {'*': {'on_failure': my_on_failure}}


If you need more flexibility then you can use objects
instead of a dict to choose which tasks to annotate:

.. code-block:: python

    class MyAnnotate(object):

        def annotate(self, task):
            if task.name.startswith('tasks.'):
                return {'rate_limit': '10/s'}

    CELERY_ANNOTATIONS = (MyAnnotate(), {…})



.. _conf-concurrency:

Concurrency settings
--------------------

.. setting:: CELERYD_CONCURRENCY

CELERYD_CONCURRENCY
~~~~~~~~~~~~~~~~~~~

The number of concurrent worker processes/threads/green threads executing
tasks.

If you're doing mostly I/O you can have more processes,
but if mostly CPU-bound, try to keep it close to the
number of CPUs on your machine. If not set, the number of CPUs/cores
on the host will be used.

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

To disable prefetching, set CELERYD_PREFETCH_MULTIPLIER to 1.  Setting 
CELERYD_PREFETCH_MULTIPLIER to 0 will allow the worker to keep consuming
as many messages as it wants.

For more on prefetching, read :ref:`optimizing-prefetch-limit`

.. note::

    Tasks with ETA/countdown are not affected by prefetch limits.

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

* amqp
    Send results back as AMQP messages
    See :ref:`conf-amqp-result-backend`.

* cassandra
    Use `Cassandra`_ to store the results.
    See :ref:`conf-cassandra-result-backend`.

* ironcache
    Use `IronCache`_ to store the results.
    See :ref:`conf-ironcache-result-backend`.

* couchbase
    Use `Couchbase`_ to store the results.
    See :ref:`conf-couchbase-result-backend`.

* couchdb
    Use `CouchDB`_ to store the results.
    See :ref:`conf-couchdb-result-backend`.

.. warning:

    While the AMQP result backend is very efficient, you must make sure
    you only receive the same result once.  See :doc:`userguide/calling`).

.. _`SQLAlchemy`: http://sqlalchemy.org
.. _`memcached`: http://memcached.org
.. _`MongoDB`: http://mongodb.org
.. _`Redis`: http://redis.io
.. _`Cassandra`: http://cassandra.apache.org/
.. _`IronCache`: http://www.iron.io/cache
.. _`CouchDB`: http://www.couchdb.com/
.. _`Couchbase`: http://www.couchbase.com/


.. setting:: CELERY_RESULT_SERIALIZER

CELERY_RESULT_SERIALIZER
~~~~~~~~~~~~~~~~~~~~~~~~

Result serialization format.  Default is ``pickle``. See
:ref:`calling-serializers` for information about supported
serialization formats.

.. _conf-database-result-backend:

Database backend settings
-------------------------

Database URL Examples
~~~~~~~~~~~~~~~~~~~~~

To use the database backend you have to configure the
:setting:`CELERY_RESULT_BACKEND` setting with a connection URL and the ``db+``
prefix:

.. code-block:: python

    CELERY_RESULT_BACKEND = 'db+scheme://user:password@host:port/dbname'

Examples:

    # sqlite (filename)
    CELERY_RESULT_BACKEND = 'db+sqlite:///results.sqlite'

    # mysql
    CELERY_RESULT_BACKEND = 'db+mysql://scott:tiger@localhost/foo'

    # postgresql
    CELERY_RESULT_BACKEND = 'db+postgresql://scott:tiger@localhost/mydatabase'

    # oracle
    CELERY_RESULT_BACKEND = 'db+oracle://scott:tiger@127.0.0.1:1521/sidname'

.. code-block:: python

Please see `Supported Databases`_ for a table of supported databases,
and `Connection String`_ for more information about connection
strings (which is the part of the URI that comes after the ``db+`` prefix).

.. _`Supported Databases`:
    http://www.sqlalchemy.org/docs/core/engines.html#supported-databases

.. _`Connection String`:
    http://www.sqlalchemy.org/docs/core/engines.html#database-urls

.. setting:: CELERY_RESULT_DBURI

CELERY_RESULT_DBURI
~~~~~~~~~~~~~~~~~~~

This setting is no longer used as it's now possible to specify
the database URL directly in the :setting:`CELERY_RESULT_BACKEND` setting.

.. setting:: CELERY_RESULT_ENGINE_OPTIONS

CELERY_RESULT_ENGINE_OPTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To specify additional SQLAlchemy database engine options you can use
the :setting:`CELERY_RESULT_ENGINE_OPTIONS` setting::

    # echo enables verbose logging from SQLAlchemy.
    CELERY_RESULT_ENGINE_OPTIONS = {'echo': True}

.. setting:: CELERY_RESULT_DB_SHORT_LIVED_SESSIONS

Short lived sessions
~~~~~~~~~~~~~~~~~~~~

    CELERY_RESULT_DB_SHORT_LIVED_SESSIONS = True

Short lived sessions are disabled by default.  If enabled they can drastically reduce
performance, especially on systems processing lots of tasks.  This option is useful
on low-traffic workers that experience errors as a result of cached database connections
going stale through inactivity.  For example, intermittent errors like
`(OperationalError) (2006, 'MySQL server has gone away')` can be fixed by enabling
short lived sessions.  This option only affects the database backend.

Specifying Table Names
~~~~~~~~~~~~~~~~~~~~~~

.. setting:: CELERY_RESULT_DB_TABLENAMES

When SQLAlchemy is configured as the result backend, Celery automatically
creates two tables to store result metadata for tasks.  This setting allows
you to customize the table names:

.. code-block:: python

    # use custom table names for the database result backend.
    CELERY_RESULT_DB_TABLENAMES = {
        'task': 'myapp_taskmeta',
        'group': 'myapp_groupmeta',
    }

.. _conf-amqp-result-backend:

AMQP backend settings
---------------------

.. note::

    The AMQP backend requires RabbitMQ 1.1.0 or higher to automatically
    expire results.  If you are running an older version of RabbitMQ
    you should disable result expiration like this:

        CELERY_TASK_RESULT_EXPIRES = None

.. setting:: CELERY_RESULT_EXCHANGE

CELERY_RESULT_EXCHANGE
~~~~~~~~~~~~~~~~~~~~~~

Name of the exchange to publish results in.  Default is `celeryresults`.

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

    CELERY_RESULT_BACKEND = 'amqp'
    CELERY_TASK_RESULT_EXPIRES = 18000  # 5 hours.

.. _conf-cache-result-backend:

Cache backend settings
----------------------

.. note::

    The cache backend supports the `pylibmc`_ and `python-memcached`
    libraries.  The latter is used only if `pylibmc`_ is not installed.

Using a single memcached server:

.. code-block:: python

    CELERY_RESULT_BACKEND = 'cache+memcached://127.0.0.1:11211/'

Using multiple memcached servers:

.. code-block:: python

    CELERY_RESULT_BACKEND = """
        cache+memcached://172.19.26.240:11211;172.19.26.242:11211/
    """.strip()

.. setting:: CELERY_CACHE_BACKEND_OPTIONS

The "memory" backend stores the cache in memory only:

    CELERY_CACHE_BACKEND = 'memory'

CELERY_CACHE_BACKEND_OPTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can set pylibmc options using the :setting:`CELERY_CACHE_BACKEND_OPTIONS`
setting:

.. code-block:: python

    CELERY_CACHE_BACKEND_OPTIONS = {'binary': True,
                                    'behaviors': {'tcp_nodelay': True}}

.. _`pylibmc`: http://sendapatch.se/projects/pylibmc/

.. setting:: CELERY_CACHE_BACKEND

CELERY_CACHE_BACKEND
~~~~~~~~~~~~~~~~~~~~

This setting is no longer used as it's now possible to specify
the cache backend directly in the :setting:`CELERY_RESULT_BACKEND` setting.

.. _conf-redis-result-backend:

Redis backend settings
----------------------

Configuring the backend URL
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

    The Redis backend requires the :mod:`redis` library:
    http://pypi.python.org/pypi/redis/

    To install the redis package use `pip` or `easy_install`:

    .. code-block:: bash

        $ pip install redis

This backend requires the :setting:`CELERY_RESULT_BACKEND`
setting to be set to a Redis URL::

    CELERY_RESULT_BACKEND = 'redis://:password@host:port/db'

For example::

    CELERY_RESULT_BACKEND = 'redis://localhost/0'

which is the same as::

    CELERY_RESULT_BACKEND = 'redis://'

The fields of the URL are defined as follows:

- *host*

Host name or IP address of the Redis server. e.g. `localhost`.

- *port*

Port to the Redis server. Default is 6379.

- *db*

Database number to use. Default is 0.
The db can include an optional leading slash.

- *password*

Password used to connect to the database.

.. setting:: CELERY_REDIS_MAX_CONNECTIONS

CELERY_REDIS_MAX_CONNECTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum number of connections available in the Redis connection
pool used for sending and retrieving results.

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

* database
    The database name to connect to. Defaults to ``celery``.

* taskmeta_collection
    The collection name to store task meta data.
    Defaults to ``celery_taskmeta``.

* max_pool_size
    Passed as max_pool_size to PyMongo's Connection or MongoClient
    constructor. It is the maximum number of TCP connections to keep
    open to MongoDB at a given time. If there are more open connections
    than max_pool_size, sockets will be closed when they are released.
    Defaults to 10.

* options

    Additional keyword arguments to pass to the mongodb connection
    constructor.  See the :mod:`pymongo` docs to see a list of arguments
    supported.

.. _example-mongodb-result-config:

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CELERY_RESULT_BACKEND = 'mongodb://192.168.1.100:30000/'
    CELERY_MONGODB_BACKEND_SETTINGS = {
        'database': 'mydb',
        'taskmeta_collection': 'my_taskmeta_collection',
    }

.. _conf-cassandra-result-backend:

Cassandra backend settings
--------------------------

.. note::

    The Cassandra backend requires the :mod:`pycassa` library:
    http://pypi.python.org/pypi/pycassa/

    To install the pycassa package use `pip` or `easy_install`:

    .. code-block:: bash

        $ pip install pycassa

This backend requires the following configuration directives to be set.

.. setting:: CASSANDRA_SERVERS

CASSANDRA_SERVERS
~~~~~~~~~~~~~~~~~

List of ``host:port`` Cassandra servers. e.g.::

    CASSANDRA_SERVERS = ['localhost:9160']

.. setting:: CASSANDRA_KEYSPACE

CASSANDRA_KEYSPACE
~~~~~~~~~~~~~~~~~~

The keyspace in which to store the results. e.g.::

    CASSANDRA_KEYSPACE = 'tasks_keyspace'

.. setting:: CASSANDRA_COLUMN_FAMILY

CASSANDRA_COLUMN_FAMILY
~~~~~~~~~~~~~~~~~~~~~~~

The column family in which to store the results. e.g.::

    CASSANDRA_COLUMN_FAMILY = 'tasks'

.. setting:: CASSANDRA_READ_CONSISTENCY

CASSANDRA_READ_CONSISTENCY
~~~~~~~~~~~~~~~~~~~~~~~~~~

The read consistency used. Values can be ``ONE``, ``QUORUM`` or ``ALL``.

.. setting:: CASSANDRA_WRITE_CONSISTENCY

CASSANDRA_WRITE_CONSISTENCY
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The write consistency used. Values can be ``ONE``, ``QUORUM`` or ``ALL``.

.. setting:: CASSANDRA_DETAILED_MODE

CASSANDRA_DETAILED_MODE
~~~~~~~~~~~~~~~~~~~~~~~

Enable or disable detailed mode. Default is :const:`False`.
This mode allows to use the power of Cassandra wide columns to
store all states for a task as a wide column, instead of only the last one.

To use this mode, you need to configure your ColumnFamily to
use the ``TimeUUID`` type as a comparator::

    create column family task_results with comparator = TimeUUIDType;

CASSANDRA_OPTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Options to be passed to the `pycassa connection pool`_ (optional).

.. _`pycassa connection pool`: http://pycassa.github.com/pycassa/api/pycassa/pool.html

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CASSANDRA_SERVERS = ['localhost:9160']
    CASSANDRA_KEYSPACE = 'celery'
    CASSANDRA_COLUMN_FAMILY = 'task_results'
    CASSANDRA_READ_CONSISTENCY = 'ONE'
    CASSANDRA_WRITE_CONSISTENCY = 'ONE'
    CASSANDRA_DETAILED_MODE = True
    CASSANDRA_OPTIONS = {
        'timeout': 300,
        'max_retries': 10
    }

.. _conf-riak-result-backend:

Riak backend settings
---------------------

.. note::

    The Riak backend requires the :mod:`riak` library:
    http://pypi.python.org/pypi/riak/

    To install the riak package use `pip` or `easy_install`:

    .. code-block:: bash

        $ pip install riak

This backend requires the :setting:`CELERY_RESULT_BACKEND`
setting to be set to a Riak URL::

    CELERY_RESULT_BACKEND = "riak://host:port/bucket"

For example::

    CELERY_RESULT_BACKEND = "riak://localhost/celery

which is the same as::

    CELERY_RESULT_BACKEND = "riak://"

The fields of the URL are defined as follows:

- *host*

Host name or IP address of the Riak server. e.g. `"localhost"`.

- *port*

Port to the Riak server using the protobuf protocol. Default is 8087.

- *bucket*

Bucket name to use. Default is `celery`.
The bucket needs to be a string with ascii characters only.

Altenatively, this backend can be configured with the following configuration directives.

.. setting:: CELERY_RIAK_BACKEND_SETTINGS

CELERY_RIAK_BACKEND_SETTINGS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a dict supporting the following keys:

* host
    The host name of the Riak server. Defaults to "localhost".

* port
    The port the Riak server is listening to. Defaults to 8087.

* bucket
    The bucket name to connect to. Defaults to "celery".

* protocol
    The protocol to use to connect to the Riak server. This is not configurable
    via :setting:`CELERY_RESULT_BACKEND`

.. _conf-ironcache-result-backend:

IronCache backend settings
--------------------------

.. note::

    The IronCache backend requires the :mod:`iron_celery` library:
    http://pypi.python.org/pypi/iron_celery

    To install the iron_celery package use `pip` or `easy_install`:

    .. code-block:: bash

        $ pip install iron_celery

IronCache is configured via the URL provided in :setting:`CELERY_RESULT_BACKEND`, for example::

    CELERY_RESULT_BACKEND = 'ironcache://project_id:token@'

Or to change the cache name::

    ironcache:://project_id:token@/awesomecache

For more information, see: https://github.com/iron-io/iron_celery


.. _conf-couchbase-result-backend:

Couchbase backend settings
--------------------------

.. note::

    The Couchbase backend requires the :mod:`couchbase` library:
    https://pypi.python.org/pypi/couchbase

    To install the couchbase package use `pip` or `easy_install`:

    .. code-block:: bash

        $ pip install couchbase

This backend can be configured via the :setting:`CELERY_RESULT_BACKEND`
set to a couchbase URL::

    CELERY_RESULT_BACKEND = 'couchbase://username:password@host:port/bucket'


.. setting:: CELERY_COUCHBASE_BACKEND_SETTINGS

CELERY_COUCHBASE_BACKEND_SETTINGS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a dict supporting the following keys:

* host
    Host name of the Couchbase server. Defaults to ``localhost``.

* port
    The port the Couchbase server is listening to. Defaults to ``8091``.

* bucket
    The default bucket the Couchbase server is writing to.
    Defaults to ``default``.

* username
    User name to authenticate to the Couchbase server as (optional).

* password
    Password to authenticate to the Couchbase server (optional).


.. _conf-couchdb-result-backend:

CouchDB backend settings
------------------------

.. note::

    The CouchDB backend requires the :mod:`pycouchdb` library:
    https://pypi.python.org/pypi/pycouchdb

    To install the couchbase package use `pip` or `easy_install`:

    .. code-block:: bash

        $ pip install pycouchdb

This backend can be configured via the :setting:`CELERY_RESULT_BACKEND`
set to a couchdb URL::

    CELERY_RESULT_BACKEND = 'couchdb://username:password@host:port/container'


The URL is formed out of the following parts:

* username
    User name to authenticate to the CouchDB server as (optional).

* password
    Password to authenticate to the CouchDB server (optional).

* host
    Host name of the CouchDB server. Defaults to ``localhost``.

* port
    The port the CouchDB server is listening to. Defaults to ``8091``.

* container
    The default container the CouchDB server is writing to.
    Defaults to ``default``.


.. _conf-messaging:

Message Routing
---------------

.. _conf-messaging-routing:

.. setting:: CELERY_QUEUES

CELERY_QUEUES
~~~~~~~~~~~~~

Most users will not want to specify this setting and should rather use
the :ref:`automatic routing facilities <routing-automatic>`.

If you really want to configure advanced routing, this setting should
be a list of :class:`kombu.Queue` objects the worker will consume from.

Note that workers can be overriden this setting via the `-Q` option,
or individual queues from this list (by name) can be excluded using
the `-X` option.

Also see :ref:`routing-basics` for more information.

The default is a queue/exchange/binding key of ``celery``, with
exchange type ``direct``.

.. setting:: CELERY_ROUTES

CELERY_ROUTES
~~~~~~~~~~~~~

A list of routers, or a single router used to route tasks to queues.
When deciding the final destination of a task the routers are consulted
in order.  See :ref:`routers` for more information.

.. setting:: CELERY_QUEUE_HA_POLICY

CELERY_QUEUE_HA_POLICY
~~~~~~~~~~~~~~~~~~~~~~
:brokers: RabbitMQ

This will set the default HA policy for a queue, and the value
can either be a string (usually ``all``):

.. code-block:: python

    CELERY_QUEUE_HA_POLICY = 'all'

Using 'all' will replicate the queue to all current nodes,
Or you can give it a list of nodes to replicate to:

.. code-block:: python

    CELERY_QUEUE_HA_POLICY = ['rabbit@host1', 'rabbit@host2']


Using a list will implicitly set ``x-ha-policy`` to 'nodes' and
``x-ha-policy-params`` to the given list of nodes.

See http://www.rabbitmq.com/ha.html for more information.

.. setting:: CELERY_WORKER_DIRECT

CELERY_WORKER_DIRECT
~~~~~~~~~~~~~~~~~~~~

This option enables so that every worker has a dedicated queue,
so that tasks can be routed to specific workers.

The queue name for each worker is automatically generated based on
the worker hostname and a ``.dq`` suffix, using the ``C.dq`` exchange.

For example the queue name for the worker with node name ``w1@example.com``
becomes::

    w1@example.com.dq

Then you can route the task to the task by specifying the hostname
as the routing key and the ``C.dq`` exchange::

    CELERY_ROUTES = {
        'tasks.add': {'exchange': 'C.dq', 'routing_key': 'w1@example.com'}
    }

.. setting:: CELERY_CREATE_MISSING_QUEUES

CELERY_CREATE_MISSING_QUEUES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled (default), any queues specified that are not defined in
:setting:`CELERY_QUEUES` will be automatically created. See
:ref:`routing-automatic`.

.. setting:: CELERY_DEFAULT_QUEUE

CELERY_DEFAULT_QUEUE
~~~~~~~~~~~~~~~~~~~~

The name of the default queue used by `.apply_async` if the message has
no route or no custom queue has been specified.


This queue must be listed in :setting:`CELERY_QUEUES`.
If :setting:`CELERY_QUEUES` is not specified then it is automatically
created containing one queue entry, where this name is used as the name of
that queue.

The default is: `celery`.

.. seealso::

    :ref:`routing-changing-default-queue`

.. setting:: CELERY_DEFAULT_EXCHANGE

CELERY_DEFAULT_EXCHANGE
~~~~~~~~~~~~~~~~~~~~~~~

Name of the default exchange to use when no custom exchange is
specified for a key in the :setting:`CELERY_QUEUES` setting.

The default is: `celery`.

.. setting:: CELERY_DEFAULT_EXCHANGE_TYPE

CELERY_DEFAULT_EXCHANGE_TYPE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default exchange type used when no custom exchange type is specified
for a key in the :setting:`CELERY_QUEUES` setting.
The default is: `direct`.

.. setting:: CELERY_DEFAULT_ROUTING_KEY

CELERY_DEFAULT_ROUTING_KEY
~~~~~~~~~~~~~~~~~~~~~~~~~~

The default routing key used when no custom routing key
is specified for a key in the :setting:`CELERY_QUEUES` setting.

The default is: `celery`.

.. setting:: CELERY_DEFAULT_DELIVERY_MODE

CELERY_DEFAULT_DELIVERY_MODE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Can be `transient` or `persistent`.  The default is to send
persistent messages.

.. _conf-broker-settings:

Broker Settings
---------------

.. setting:: CELERY_ACCEPT_CONTENT

CELERY_ACCEPT_CONTENT
~~~~~~~~~~~~~~~~~~~~~

A whitelist of content-types/serializers to allow.

If a message is received that is not in this list then
the message will be discarded with an error.

By default any content type is enabled (including pickle and yaml)
so make sure untrusted parties do not have access to your broker.
See :ref:`guide-security` for more.

Example::

    # using serializer name
    CELERY_ACCEPT_CONTENT = ['json']

    # or the actual content-type (MIME)
    CELERY_ACCEPT_CONTENT = ['application/json']

.. setting:: BROKER_FAILOVER_STRATEGY

BROKER_FAILOVER_STRATEGY
~~~~~~~~~~~~~~~~~~~~~~~~

Default failover strategy for the broker Connection object. If supplied,
may map to a key in 'kombu.connection.failover_strategies', or be a reference
to any method that yields a single item from a supplied list.

Example::

    # Random failover strategy
    def random_failover_strategy(servers):
        it = list(it)  # don't modify callers list
        shuffle = random.shuffle
        for _ in repeat(None):
            shuffle(it)
            yield it[0]

    BROKER_FAILOVER_STRATEGY=random_failover_strategy

.. setting:: BROKER_TRANSPORT

BROKER_TRANSPORT
~~~~~~~~~~~~~~~~
:Aliases: ``BROKER_BACKEND``
:Deprecated aliases: ``CARROT_BACKEND``

.. setting:: BROKER_URL

BROKER_URL
~~~~~~~~~~

Default broker URL.  This must be an URL in the form of::

    transport://userid:password@hostname:port/virtual_host

Only the scheme part (``transport://``) is required, the rest
is optional, and defaults to the specific transports default values.

The transport part is the broker implementation to use, and the
default is ``amqp``, which uses ``librabbitmq`` by default or falls back to
``pyamqp`` if that is not installed.  Also there are many other choices including
``redis``, ``beanstalk``, ``sqlalchemy``, ``django``, ``mongodb``,
``couchdb``.
It can also be a fully qualified path to your own transport implementation.

See :ref:`kombu:connection-urls` in the Kombu documentation for more
information.

.. setting:: BROKER_HEARTBEAT

BROKER_HEARTBEAT
~~~~~~~~~~~~~~~~
:transports supported: ``pyamqp``

It's not always possible to detect connection loss in a timely
manner using TCP/IP alone, so AMQP defines something called heartbeats
that's is used both by the client and the broker to detect if
a connection was closed.

Heartbeats are disabled by default.

If the heartbeat value is 10 seconds, then
the heartbeat will be monitored at the interval specified
by the :setting:`BROKER_HEARTBEAT_CHECKRATE` setting, which by default is
double the rate of the heartbeat value
(so for the default 10 seconds, the heartbeat is checked every 5 seconds).

.. setting:: BROKER_HEARTBEAT_CHECKRATE

BROKER_HEARTBEAT_CHECKRATE
~~~~~~~~~~~~~~~~~~~~~~~~~~
:transports supported: ``pyamqp``

At intervals the worker will monitor that the broker has not missed
too many heartbeats.  The rate at which this is checked is calculated
by dividing the :setting:`BROKER_HEARTBEAT` value with this value,
so if the heartbeat is 10.0 and the rate is the default 2.0, the check
will be performed every 5 seconds (twice the heartbeat sending rate).

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

.. setting:: BROKER_LOGIN_METHOD

BROKER_LOGIN_METHOD
~~~~~~~~~~~~~~~~~~~

Set custom amqp login method, default is ``AMQPLAIN``.

.. setting:: BROKER_TRANSPORT_OPTIONS

BROKER_TRANSPORT_OPTIONS
~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

A dict of additional options passed to the underlying transport.

See your transport user manual for supported options (if any).

Example setting the visibility timeout (supported by Redis and SQS
transports):

.. code-block:: python

    BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 18000}  # 5 hours

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
Can be ``gzip``, ``bzip2`` (if available), or any custom
compression schemes registered in the Kombu compression registry.

The default is to send uncompressed messages.

.. setting:: CELERY_TASK_PROTOCOL

CELERY_TASK_PROTOCOL
~~~~~~~~~~~~~~~~~~~~

Default task message protocol version.
Supports protocols: 1 and 2 (default is 1 for backwards compatibility).

.. setting:: CELERY_TASK_RESULT_EXPIRES

CELERY_TASK_RESULT_EXPIRES
~~~~~~~~~~~~~~~~~~~~~~~~~~

Time (in seconds, or a :class:`~datetime.timedelta` object) for when after
stored task tombstones will be deleted.

A built-in periodic task will delete the results after this time
(:class:`celery.task.backend_cleanup`).

A value of :const:`None` or 0 means results will never expire (depending
on backend specifications).

Default is to expire after 1 day.

.. note::

    For the moment this only works with the amqp, database, cache, redis and MongoDB
    backends.

    When using the database or MongoDB backends, `celery beat` must be
    running for the results to be expired.

.. setting:: CELERY_MAX_CACHED_RESULTS

CELERY_MAX_CACHED_RESULTS
~~~~~~~~~~~~~~~~~~~~~~~~~

Result backends caches ready results used by the client.

This is the total number of results to cache before older results are evicted.
The default is 5000.  0 or None means no limit, and a value of :const:`-1`
will disable the cache.

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

    :ref:`calling-serializers`.

.. setting:: CELERY_TASK_PUBLISH_RETRY

CELERY_TASK_PUBLISH_RETRY
~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Decides if publishing task messages will be retried in the case
of connection loss or other connection errors.
See also :setting:`CELERY_TASK_PUBLISH_RETRY_POLICY`.

Enabled by default.

.. setting:: CELERY_TASK_PUBLISH_RETRY_POLICY

CELERY_TASK_PUBLISH_RETRY_POLICY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Defines the default policy when retrying publishing a task message in
the case of connection loss or other connection errors.

See :ref:`calling-retry` for more information.

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

.. _conf-worker:

Worker
------

.. setting:: CELERY_IMPORTS

CELERY_IMPORTS
~~~~~~~~~~~~~~

A sequence of modules to import when the worker starts.

This is used to specify the task modules to import, but also
to import signal handlers and additional remote control commands, etc.

The modules will be imported in the original order.

.. setting:: CELERY_INCLUDE

CELERY_INCLUDE
~~~~~~~~~~~~~~

Exact same semantics as :setting:`CELERY_IMPORTS`, but can be used as a means
to have different import categories.

The modules in this setting are imported after the modules in
:setting:`CELERY_IMPORTS`.

.. setting:: CELERYD_WORKER_LOST_WAIT

CELERYD_WORKER_LOST_WAIT
~~~~~~~~~~~~~~~~~~~~~~~~

In some cases a worker may be killed without proper cleanup,
and the worker may have published a result before terminating.
This value specifies how long we wait for any missing results before
raising a :exc:`@WorkerLostError` exception.

Default is 10.0

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

The :exc:`~@SoftTimeLimitExceeded` exception will be
raised when this is exceeded.  The task can catch this to
e.g. clean up before the hard time limit comes.

Example:

.. code-block:: python

    from celery.exceptions import SoftTimeLimitExceeded

    @app.task
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
:mod:`~celery.bin.worker`.

Not enabled by default.

.. setting:: CELERYD_TIMER_PRECISION

CELERYD_TIMER_PRECISION
~~~~~~~~~~~~~~~~~~~~~~~

Set the maximum time in seconds that the ETA scheduler can sleep between
rechecking the schedule.  Default is 1 second.

Setting this value to 1 second means the schedulers precision will
be 1 second. If you need near millisecond precision you can set this to 0.1.

.. setting:: CELERY_ENABLE_REMOTE_CONTROL

CELERY_ENABLE_REMOTE_CONTROL
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specify if remote control of the workers is enabled.

Default is :const:`True`.


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

The mail server to use.  Default is ``localhost``.

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
        ('George Costanza', 'george@vandelay.com'),
        ('Cosmo Kramer', 'kosmo@vandelay.com'),
    )

    # Email address used as sender (From field).
    SERVER_EMAIL = 'no-reply@vandelay.com'

    # Mailserver configuration
    EMAIL_HOST = 'mail.vandelay.com'
    EMAIL_PORT = 25
    # EMAIL_HOST_USER = 'servers'
    # EMAIL_HOST_PASSWORD = 's3cr3t'

.. _conf-events:

Events
------

.. setting:: CELERY_SEND_EVENTS

CELERY_SEND_EVENTS
~~~~~~~~~~~~~~~~~~

Send task-related events so that tasks can be monitored using tools like
`flower`.  Sets the default value for the workers :option:`-E` argument.

.. setting:: CELERY_SEND_TASK_SENT_EVENT

CELERY_SEND_TASK_SENT_EVENT
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

If enabled, a :event:`task-sent` event will be sent for every task so tasks can be
tracked before they are consumed by a worker.

Disabled by default.

.. setting:: CELERY_EVENT_QUEUE_TTL

CELERY_EVENT_QUEUE_TTL
~~~~~~~~~~~~~~~~~~~~~~
:transports supported: ``amqp``

Message expiry time in seconds (int/float) for when messages sent to a monitor clients
event queue is deleted (``x-message-ttl``)

For example, if this value is set to 10 then a message delivered to this queue
will be deleted after 10 seconds.

Disabled by default.

.. setting:: CELERY_EVENT_QUEUE_EXPIRES

CELERY_EVENT_QUEUE_EXPIRES
~~~~~~~~~~~~~~~~~~~~~~~~~~
:transports supported: ``amqp``


Expiry time in seconds (int/float) for when after a monitor clients
event queue will be deleted (``x-expires``).

Default is never, relying on the queue autodelete setting.

.. setting:: CELERY_EVENT_SERIALIZER

CELERY_EVENT_SERIALIZER
~~~~~~~~~~~~~~~~~~~~~~~

Message serialization format used when sending event messages.
Default is ``json``. See :ref:`calling-serializers`.

.. _conf-broadcast:

Broadcast Commands
------------------

.. setting:: CELERY_BROADCAST_QUEUE

CELERY_BROADCAST_QUEUE
~~~~~~~~~~~~~~~~~~~~~~

Name prefix for the queue used when listening for broadcast messages.
The workers host name will be appended to the prefix to create the final
queue name.

Default is ``celeryctl``.

.. setting:: CELERY_BROADCAST_EXCHANGE

CELERY_BROADCAST_EXCHANGE
~~~~~~~~~~~~~~~~~~~~~~~~~

Name of the exchange used for broadcast messages.

Default is ``celeryctl``.

.. setting:: CELERY_BROADCAST_EXCHANGE_TYPE

CELERY_BROADCAST_EXCHANGE_TYPE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Exchange type used for broadcast messages.  Default is ``fanout``.

.. _conf-logging:

Logging
-------

.. setting:: CELERYD_HIJACK_ROOT_LOGGER

CELERYD_HIJACK_ROOT_LOGGER
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

By default any previously configured handlers on the root logger will be
removed. If you want to customize your own logging handlers, then you
can disable this behavior by setting
`CELERYD_HIJACK_ROOT_LOGGER = False`.

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
the :option:`--loglevel` option to :mod:`~celery.bin.worker`.

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
Used by :program:`celery worker` and :program:`celery beat`.

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

.. setting:: CELERYD_POOL

CELERYD_POOL
~~~~~~~~~~~~

Name of the pool class used by the worker.

.. admonition:: Eventlet/Gevent

    Never use this option to select the eventlet or gevent pool.
    You must use the `-P` option instead, otherwise the monkey patching
    will happen too late and things will break in strange and silent ways.

Default is ``celery.concurrency.prefork:TaskPool``.

.. setting:: CELERYD_POOL_RESTARTS

CELERYD_POOL_RESTARTS
~~~~~~~~~~~~~~~~~~~~~

If enabled the worker pool can be restarted using the
:control:`pool_restart` remote control command.

Disabled by default.

.. setting:: CELERYD_AUTOSCALER

CELERYD_AUTOSCALER
~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Name of the autoscaler class to use.

Default is ``celery.worker.autoscale:Autoscaler``.

.. setting:: CELERYD_AUTORELOADER

CELERYD_AUTORELOADER
~~~~~~~~~~~~~~~~~~~~

Name of the autoreloader class used by the worker to reload
Python modules and files that have changed.

Default is: ``celery.worker.autoreload:Autoreloader``.

.. setting:: CELERYD_CONSUMER

CELERYD_CONSUMER
~~~~~~~~~~~~~~~~

Name of the consumer class used by the worker.
Default is :class:`celery.worker.consumer.Consumer`

.. setting:: CELERYD_TIMER

CELERYD_TIMER
~~~~~~~~~~~~~~~~~~~~~

Name of the ETA scheduler class used by the worker.
Default is :class:`celery.utils.timer2.Timer`, or one overrided
by the pool implementation.

.. _conf-celerybeat:

Periodic Task Server: celery beat
---------------------------------

.. setting:: CELERYBEAT_SCHEDULE

CELERYBEAT_SCHEDULE
~~~~~~~~~~~~~~~~~~~

The periodic task schedule used by :mod:`~celery.bin.beat`.
See :ref:`beat-entries`.

.. setting:: CELERYBEAT_SCHEDULER

CELERYBEAT_SCHEDULER
~~~~~~~~~~~~~~~~~~~~

The default scheduler class.  Default is ``celery.beat:PersistentScheduler``.

Can also be set via the :option:`-S` argument to
:mod:`~celery.bin.beat`.

.. setting:: CELERYBEAT_SCHEDULE_FILENAME

CELERYBEAT_SCHEDULE_FILENAME
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Name of the file used by `PersistentScheduler` to store the last run times
of periodic tasks.  Can be a relative or absolute path, but be aware that the
suffix `.db` may be appended to the file name (depending on Python version).

Can also be set via the :option:`--schedule` argument to
:mod:`~celery.bin.beat`.

.. setting:: CELERYBEAT_SYNC_EVERY

CELERYBEAT_SYNC_EVERY
~~~~~~~~~~~~~~~~~~~~~

The number of periodic tasks that can be called before another database sync
is issued.
Defaults to 0 (sync based on timing - default of 3 minutes as determined by
scheduler.sync_every). If set to 1, beat will call sync after every task
message sent.

.. setting:: CELERYBEAT_MAX_LOOP_INTERVAL

CELERYBEAT_MAX_LOOP_INTERVAL
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The maximum number of seconds :mod:`~celery.bin.beat` can sleep
between checking the schedule.

The default for this value is scheduler specific.
For the default celery beat scheduler the value is 300 (5 minutes),
but for e.g. the django-celery database scheduler it is 5 seconds
because the schedule may be changed externally, and so it must take
changes to the schedule into account.

Also when running celery beat embedded (:option:`-B`) on Jython as a thread
the max interval is overridden and set to 1 so that it's possible
to shut down in a timely manner.


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
