.. _configuration:

============================
 Configuration and defaults
============================

This document describes the configuration options available.

If you're using the default loader, you must create the :file:`celeryconfig.py`
module and make sure it's available on the Python path.

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
    broker_url = 'amqp://guest:guest@localhost:5672//'

    # List of modules to import when the Celery worker starts.
    imports = ('myapp.tasks',)

    ## Using the database to store task state and results.
    result_backend = 'db+sqlite:///results.db'

    task_annotations = {'tasks.add': {'rate_limit': '10/s'}}


.. _conf-old-settings-map:

New lowercase settings
======================

Version 4.0 introduced new lower case settings and setting organization.

The major difference between previous versions, apart from the lower case
names, are the renaming of some prefixes, like ``celerybeat_`` to ``beat_``,
``celeryd_`` to ``worker_``, and most of the top level ``celery_`` settings
have been moved into a new  ``task_`` prefix.

.. note::

    Celery will still be able to read old configuration files, so
    there's no rush in moving to the new settings format. Furthermore,
    we provide the ``celery upgrade`` command that should handle plenty
    of cases (including :ref:`Django <latentcall-django-admonition>`).


=====================================  ==============================================
**Setting name**                       **Replace with**
=====================================  ==============================================
``CELERY_ACCEPT_CONTENT``              :setting:`accept_content`
``CELERY_ENABLE_UTC``                  :setting:`enable_utc`
``CELERY_IMPORTS``                     :setting:`imports`
``CELERY_INCLUDE``                     :setting:`include`
``CELERY_TIMEZONE``                    :setting:`timezone`
``CELERYBEAT_MAX_LOOP_INTERVAL``       :setting:`beat_max_loop_interval`
``CELERYBEAT_SCHEDULE``                :setting:`beat_schedule`
``CELERYBEAT_SCHEDULER``               :setting:`beat_scheduler`
``CELERYBEAT_SCHEDULE_FILENAME``       :setting:`beat_schedule_filename`
``CELERYBEAT_SYNC_EVERY``              :setting:`beat_sync_every`
``BROKER_URL``                         :setting:`broker_url`
``BROKER_TRANSPORT``                   :setting:`broker_transport`
``BROKER_TRANSPORT_OPTIONS``           :setting:`broker_transport_options`
``BROKER_CONNECTION_TIMEOUT``          :setting:`broker_connection_timeout`
``BROKER_CONNECTION_RETRY``            :setting:`broker_connection_retry`
``BROKER_CONNECTION_MAX_RETRIES``      :setting:`broker_connection_max_retries`
``BROKER_FAILOVER_STRATEGY``           :setting:`broker_failover_strategy`
``BROKER_HEARTBEAT``                   :setting:`broker_heartbeat`
``BROKER_LOGIN_METHOD``                :setting:`broker_login_method`
``BROKER_POOL_LIMIT``                  :setting:`broker_pool_limit`
``BROKER_USE_SSL``                     :setting:`broker_use_ssl`
``CELERY_CACHE_BACKEND``               :setting:`cache_backend`
``CELERY_CACHE_BACKEND_OPTIONS``       :setting:`cache_backend_options`
``CASSANDRA_COLUMN_FAMILY``            :setting:`cassandra_table`
``CASSANDRA_ENTRY_TTL``                :setting:`cassandra_entry_ttl`
``CASSANDRA_KEYSPACE``                 :setting:`cassandra_keyspace`
``CASSANDRA_PORT``                     :setting:`cassandra_port`
``CASSANDRA_READ_CONSISTENCY``         :setting:`cassandra_read_consistency`
``CASSANDRA_SERVERS``                  :setting:`cassandra_servers`
``CASSANDRA_WRITE_CONSISTENCY``        :setting:`cassandra_write_consistency`
``CASSANDRA_OPTIONS``                  :setting:`cassandra_options`
``CELERY_COUCHBASE_BACKEND_SETTINGS``  :setting:`couchbase_backend_settings`
``CELERY_MONGODB_BACKEND_SETTINGS``    :setting:`mongodb_backend_settings`
``CELERY_EVENT_QUEUE_EXPIRES``         :setting:`event_queue_expires`
``CELERY_EVENT_QUEUE_TTL``             :setting:`event_queue_ttl`
``CELERY_EVENT_QUEUE_PREFIX``          :setting:`event_queue_prefix`
``CELERY_EVENT_SERIALIZER``            :setting:`event_serializer`
``CELERY_REDIS_DB``                    :setting:`redis_db`
``CELERY_REDIS_HOST``                  :setting:`redis_host`
``CELERY_REDIS_MAX_CONNECTIONS``       :setting:`redis_max_connections`
``CELERY_REDIS_PASSWORD``              :setting:`redis_password`
``CELERY_REDIS_PORT``                  :setting:`redis_port`
``CELERY_RESULT_BACKEND``              :setting:`result_backend`
``CELERY_MAX_CACHED_RESULTS``          :setting:`result_cache_max`
``CELERY_MESSAGE_COMPRESSION``         :setting:`result_compression`
``CELERY_RESULT_EXCHANGE``             :setting:`result_exchange`
``CELERY_RESULT_EXCHANGE_TYPE``        :setting:`result_exchange_type`
``CELERY_TASK_RESULT_EXPIRES``         :setting:`result_expires`
``CELERY_RESULT_PERSISTENT``           :setting:`result_persistent`
``CELERY_RESULT_SERIALIZER``           :setting:`result_serializer`
``CELERY_RESULT_DBURI``                Use :setting:`result_backend` instead.
``CELERY_RESULT_ENGINE_OPTIONS``       :setting:`database_engine_options`
``[...]_DB_SHORT_LIVED_SESSIONS``      :setting:`database_short_lived_sessions`
``CELERY_RESULT_DB_TABLE_NAMES``       :setting:`database_db_names`
``CELERY_SECURITY_CERTIFICATE``        :setting:`security_certificate`
``CELERY_SECURITY_CERT_STORE``         :setting:`security_cert_store`
``CELERY_SECURITY_KEY``                :setting:`security_key`
``CELERY_ACKS_LATE``                   :setting:`task_acks_late`
``CELERY_TASK_ALWAYS_EAGER``           :setting:`task_always_eager`
``CELERY_TASK_ANNOTATIONS``            :setting:`task_annotations`
``CELERY_TASK_COMPRESSION``            :setting:`task_compression`
``CELERY_TASK_CREATE_MISSING_QUEUES``  :setting:`task_create_missing_queues`
``CELERY_TASK_DEFAULT_DELIVERY_MODE``  :setting:`task_default_delivery_mode`
``CELERY_TASK_DEFAULT_EXCHANGE``       :setting:`task_default_exchange`
``CELERY_TASK_DEFAULT_EXCHANGE_TYPE``  :setting:`task_default_exchange_type`
``CELERY_TASK_DEFAULT_QUEUE``          :setting:`task_default_queue`
``CELERY_TASK_DEFAULT_RATE_LIMIT``     :setting:`task_default_rate_limit`
``CELERY_TASK_DEFAULT_ROUTING_KEY``    :setting:`task_default_routing_key`
``CELERY_TASK_EAGER_PROPAGATES``       :setting:`task_eager_propagates`
``CELERY_TASK_IGNORE_RESULT``          :setting:`task_ignore_result`
``CELERY_TASK_PUBLISH_RETRY``          :setting:`task_publish_retry`
``CELERY_TASK_PUBLISH_RETRY_POLICY``   :setting:`task_publish_retry_policy`
``CELERY_QUEUES``                      :setting:`task_queues`
``CELERY_ROUTES``                      :setting:`task_routes`
``CELERY_TASK_SEND_SENT_EVENT``        :setting:`task_send_sent_event`
``CELERY_TASK_SERIALIZER``             :setting:`task_serializer`
``CELERYD_TASK_SOFT_TIME_LIMIT``       :setting:`task_soft_time_limit`
``CELERYD_TASK_TIME_LIMIT``            :setting:`task_time_limit`
``CELERY_TRACK_STARTED``               :setting:`task_track_started`
``CELERYD_AGENT``                      :setting:`worker_agent`
``CELERYD_AUTOSCALER``                 :setting:`worker_autoscaler`
``CELERYD_CONCURRENCY``                :setting:`worker_concurrency`
``CELERYD_CONSUMER``                   :setting:`worker_consumer`
``CELERY_WORKER_DIRECT``               :setting:`worker_direct`
``CELERY_DISABLE_RATE_LIMITS``         :setting:`worker_disable_rate_limits`
``CELERY_ENABLE_REMOTE_CONTROL``       :setting:`worker_enable_remote_control`
``CELERYD_HIJACK_ROOT_LOGGER``         :setting:`worker_hijack_root_logger`
``CELERYD_LOG_COLOR``                  :setting:`worker_log_color`
``CELERYD_LOG_FORMAT``                 :setting:`worker_log_format`
``CELERYD_WORKER_LOST_WAIT``           :setting:`worker_lost_wait`
``CELERYD_MAX_TASKS_PER_CHILD``        :setting:`worker_max_tasks_per_child`
``CELERYD_POOL``                       :setting:`worker_pool`
``CELERYD_POOL_PUTLOCKS``              :setting:`worker_pool_putlocks`
``CELERYD_POOL_RESTARTS``              :setting:`worker_pool_restarts`
``CELERYD_PREFETCH_MULTIPLIER``        :setting:`worker_prefetch_multiplier`
``CELERYD_REDIRECT_STDOUTS``           :setting:`worker_redirect_stdouts`
``CELERYD_REDIRECT_STDOUTS_LEVEL``     :setting:`worker_redirect_stdouts_level`
``CELERYD_SEND_EVENTS``                :setting:`worker_send_task_events`
``CELERYD_STATE_DB``                   :setting:`worker_state_db`
``CELERYD_TASK_LOG_FORMAT``            :setting:`worker_task_log_format`
``CELERYD_TIMER``                      :setting:`worker_timer`
``CELERYD_TIMER_PRECISION``            :setting:`worker_timer_precision`
=====================================  ==============================================

Configuration Directives
========================

.. _conf-datetime:

General settings
----------------

.. setting:: accept_content

``accept_content``
~~~~~~~~~~~~~~~~~~

Default: ``{'json'}``  (set, list, or tuple).

A white-list of content-types/serializers to allow.

If a message is received that's not in this list then
the message will be discarded with an error.

By default only json is enabled but any content type can be added,
including pickle and yaml; when this is the case make sure
untrusted parties don't have access to your broker.
See :ref:`guide-security` for more.

Example::

    # using serializer name
    accept_content = ['json']

    # or the actual content-type (MIME)
    accept_content = ['application/json']

Time and date settings
----------------------

.. setting:: enable_utc

``enable_utc``
~~~~~~~~~~~~~~

.. versionadded:: 2.5

Default: Enabled by default since version 3.0.

If enabled dates and times in messages will be converted to use
the UTC timezone.

Note that workers running Celery versions below 2.5 will assume a local
timezone for all messages, so only enable if all workers have been
upgraded.

.. setting:: timezone

``timezone``
~~~~~~~~~~~~

.. versionadded:: 2.5

Default: ``"UTC"``.

Configure Celery to use a custom time zone.
The timezone value can be any time zone supported by the :pypi:`pytz`
library.

If not set the UTC timezone is used. For backwards compatibility
there's also a :setting:`enable_utc` setting, and when this is set
to false the system local timezone is used instead.

.. _conf-tasks:

Task settings
-------------

.. setting:: task_annotations

``task_annotations``
~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.5

Default: :const:`None`.

This setting can be used to rewrite any task attribute from the
configuration. The setting can be a dict, or a list of annotation
objects that filter for tasks and return a map of attributes
to change.

This will change the ``rate_limit`` attribute for the ``tasks.add``
task:

.. code-block:: python

    task_annotations = {'tasks.add': {'rate_limit': '10/s'}}

or change the same for all tasks:

.. code-block:: python

    task_annotations = {'*': {'rate_limit': '10/s'}}

You can change methods too, for example the ``on_failure`` handler:

.. code-block:: python

    def my_on_failure(self, exc, task_id, args, kwargs, einfo):
        print('Oh no! Task failed: {0!r}'.format(exc))

    task_annotations = {'*': {'on_failure': my_on_failure}}

If you need more flexibility then you can use objects
instead of a dict to choose the tasks to annotate:

.. code-block:: python

    class MyAnnotate(object):

        def annotate(self, task):
            if task.name.startswith('tasks.'):
                return {'rate_limit': '10/s'}

    task_annotations = (MyAnnotate(), {other,})

.. setting:: task_compression

``task_compression``
~~~~~~~~~~~~~~~~~~~~

Default: :const:`None`

Default compression used for task messages.
Can be ``gzip``, ``bzip2`` (if available), or any custom
compression schemes registered in the Kombu compression registry.

The default is to send uncompressed messages.

.. setting:: task_protocol

``task_protocol``
~~~~~~~~~~~~~~~~~

.. versionadded: 4.0

Default: 2 (since 4.0).

Set the default task message protocol version used to send tasks.
Supports protocols: 1 and 2.

Protocol 2 is supported by 3.1.24 and 4.x+.

.. setting:: task_serializer

``task_serializer``
~~~~~~~~~~~~~~~~~~~

Default: ``"json"`` (since 4.0, earlier: pickle).

A string identifying the default serialization method to use. Can be
`json` (default), `pickle`, `yaml`, `msgpack`, or any custom serialization
methods that have been registered with :mod:`kombu.serialization.registry`.

.. seealso::

    :ref:`calling-serializers`.

.. setting:: task_publish_retry

``task_publish_retry``
~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Default: Enabled.

Decides if publishing task messages will be retried in the case
of connection loss or other connection errors.
See also :setting:`task_publish_retry_policy`.

.. setting:: task_publish_retry_policy

``task_publish_retry_policy``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Default: See :ref:`calling-retry`.

Defines the default policy when retrying publishing a task message in
the case of connection loss or other connection errors.

.. _conf-task-execution:

Task execution settings
-----------------------

.. setting:: task_always_eager

``task_always_eager``
~~~~~~~~~~~~~~~~~~~~~

Default: Disabled.

If this is :const:`True`, all tasks will be executed locally by blocking until
the task returns. ``apply_async()`` and ``Task.delay()`` will return
an :class:`~celery.result.EagerResult` instance, that emulates the API
and behavior of :class:`~celery.result.AsyncResult`, except the result
is already evaluated.

That is, tasks will be executed locally instead of being sent to
the queue.

.. setting:: task_eager_propagates

``task_eager_propagates``
~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled.

If this is :const:`True`, eagerly executed tasks (applied by `task.apply()`,
or when the :setting:`task_always_eager` setting is enabled), will
propagate exceptions.

It's the same as always running ``apply()`` with ``throw=True``.

.. setting:: task_remote_tracebacks

``task_remote_tracebacks``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled.

If enabled task results will include the workers stack when re-raising
task errors.

This requires the :pypi:`tblib` library, that can be installed using
:command:`pip`:

.. code-block:: console

    $ pip install celery[tblib]

See :ref:`bundles` for information on combining multiple extension
requirements.

.. setting:: task_ignore_result

``task_ignore_result``
~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled.

Whether to store the task return values or not (tombstones).
If you still want to store errors, just not successful return values,
you can set :setting:`task_store_errors_even_if_ignored`.

.. setting:: task_store_errors_even_if_ignored

``task_store_errors_even_if_ignored``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled.

If set, the worker stores all task errors in the result store even if
:attr:`Task.ignore_result <celery.task.base.Task.ignore_result>` is on.

.. setting:: task_track_started

``task_track_started``
~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled.

If :const:`True` the task will report its status as 'started' when the
task is executed by a worker. The default value is :const:`False` as
the normal behavior is to not report that level of granularity. Tasks
are either pending, finished, or waiting to be retried. Having a 'started'
state can be useful for when there are long running tasks and there's a
need to report what task is currently running.

.. setting:: task_time_limit

``task_time_limit``
~~~~~~~~~~~~~~~~~~~

Default: No time limit.

Task hard time limit in seconds. The worker processing the task will
be killed and replaced with a new one when this is exceeded.

.. setting:: task_soft_time_limit

``task_soft_time_limit``
~~~~~~~~~~~~~~~~~~~~~~~~

Default: No soft time limit.

Task soft time limit in seconds.

The :exc:`~@SoftTimeLimitExceeded` exception will be
raised when this is exceeded. For example, the task can catch this to
clean up before the hard time limit comes:

.. code-block:: python

    from celery.exceptions import SoftTimeLimitExceeded

    @app.task
    def mytask():
        try:
            return do_work()
        except SoftTimeLimitExceeded:
            cleanup_in_a_hurry()

.. setting:: task_acks_late

``task_acks_late``
~~~~~~~~~~~~~~~~~~

Default: Disabled.

Late ack means the task messages will be acknowledged **after** the task
has been executed, not *just before* (the default behavior).

.. seealso::

    FAQ: :ref:`faq-acks_late-vs-retry`.

.. setting:: task_reject_on_worker_lost

``task_reject_on_worker_lost``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled.

Even if :setting:`task_acks_late` is enabled, the worker will
acknowledge tasks when the worker process executing them abruptly
exits or is signaled (e.g., :sig:`KILL`/:sig:`INT`, etc).

Setting this to true allows the message to be re-queued instead,
so that the task will execute again by the same worker, or another
worker.

.. warning::

    Enabling this can cause message loops; make sure you know
    what you're doing.

.. setting:: task_default_rate_limit

``task_default_rate_limit``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: No rate limit.

The global default rate limit for tasks.

This value is used for tasks that doesn't have a custom rate limit

.. seealso::

    The setting:`worker_disable_rate_limits` setting can
    disable all rate limits.

.. _conf-result-backend:

Task result backend settings
----------------------------

.. setting:: result_backend

``result_backend``
~~~~~~~~~~~~~~~~~~

Default: No result backend enabled by default.

The backend used to store task results (tombstones).
Can be one of the following:

* ``rpc``
    Send results back as AMQP messages
    See :ref:`conf-rpc-result-backend`.

* ``database``
    Use a relational database supported by `SQLAlchemy`_.
    See :ref:`conf-database-result-backend`.

* ``redis``
    Use `Redis`_ to store the results.
    See :ref:`conf-redis-result-backend`.

* ``cache``
    Use `Memcached`_ to store the results.
    See :ref:`conf-cache-result-backend`.

* ``cassandra``
    Use `Cassandra`_ to store the results.
    See :ref:`conf-cassandra-result-backend`.

* ``elasticsearch``
    Use `Elasticsearch`_ to store the results.
    See :ref:`conf-elasticsearch-result-backend`.

* ``ironcache``
    Use `IronCache`_ to store the results.
    See :ref:`conf-ironcache-result-backend`.

* ``couchbase``
    Use `Couchbase`_ to store the results.
    See :ref:`conf-couchbase-result-backend`.

* ``couchdb``
    Use `CouchDB`_ to store the results.
    See :ref:`conf-couchdb-result-backend`.

* ``filesystem``
    Use a shared directory to store the results.
    See :ref:`conf-filesystem-result-backend`.

* ``consul``
    Use the `Consul`_ K/V store to store the results
    See :ref:`conf-consul-result-backend`.

.. warning:

    While the AMQP result backend is very efficient, you must make sure
    you only receive the same result once. See :doc:`userguide/calling`).

.. _`SQLAlchemy`: http://sqlalchemy.org
.. _`Memcached`: http://memcached.org
.. _`Redis`: https://redis.io
.. _`Cassandra`: http://cassandra.apache.org/
.. _`Elasticsearch`: https://aws.amazon.com/elasticsearch-service/
.. _`IronCache`: http://www.iron.io/cache
.. _`CouchDB`: http://www.couchdb.com/
.. _`Couchbase`: https://www.couchbase.com/
.. _`Consul`: https://consul.io/


.. setting:: result_backend_transport_options

``result_backend_transport_options``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``{}`` (empty mapping).

A dict of additional options passed to the underlying transport.

See your transport user manual for supported options (if any).

Example setting the visibility timeout (supported by Redis and SQS
transports):

.. code-block:: python

    result_backend_transport_options = {'visibility_timeout': 18000}  # 5 hours



.. setting:: result_serializer

``result_serializer``
~~~~~~~~~~~~~~~~~~~~~

Default: ``json`` since 4.0 (earlier: pickle).

Result serialization format.

See :ref:`calling-serializers` for information about supported
serialization formats.

.. setting:: result_compression

``result_compression``
~~~~~~~~~~~~~~~~~~~~~~

Default: No compression.

Optional compression method used for task results.
Supports the same options as the :setting:`task_serializer` setting.

.. setting:: result_expires

``result_expires``
~~~~~~~~~~~~~~~~~~

Default: Expire after 1 day.

Time (in seconds, or a :class:`~datetime.timedelta` object) for when after
stored task tombstones will be deleted.

A built-in periodic task will delete the results after this time
(``celery.backend_cleanup``), assuming that ``celery beat`` is
enabled. The task runs daily at 4am.

A value of :const:`None` or 0 means results will never expire (depending
on backend specifications).

.. note::

    For the moment this only works with the AMQP, database, cache,
    and Redis backends.

    When using the database backend, ``celery beat`` must be
    running for the results to be expired.

.. setting:: result_cache_max

``result_cache_max``
~~~~~~~~~~~~~~~~~~~~

Default: Disabled by default.

Enables client caching of results.

This can be useful for the old deprecated
'amqp' backend where the result is unavailable as soon as one result instance
consumes it.

This is the total number of results to cache before older results are evicted.
A value of 0 or None means no limit, and a value of :const:`-1`
will disable the cache.

Disabled by default.

.. _conf-database-result-backend:

Database backend settings
-------------------------

Database URL Examples
~~~~~~~~~~~~~~~~~~~~~

To use the database backend you have to configure the
:setting:`result_backend` setting with a connection URL and the ``db+``
prefix:

.. code-block:: python

    result_backend = 'db+scheme://user:password@host:port/dbname'

Examples::

    # sqlite (filename)
    result_backend = 'db+sqlite:///results.sqlite'

    # mysql
    result_backend = 'db+mysql://scott:tiger@localhost/foo'

    # postgresql
    result_backend = 'db+postgresql://scott:tiger@localhost/mydatabase'

    # oracle
    result_backend = 'db+oracle://scott:tiger@127.0.0.1:1521/sidname'

.. code-block:: python

Please see `Supported Databases`_ for a table of supported databases,
and `Connection String`_ for more information about connection
strings (this is the part of the URI that comes after the ``db+`` prefix).

.. _`Supported Databases`:
    http://www.sqlalchemy.org/docs/core/engines.html#supported-databases

.. _`Connection String`:
    http://www.sqlalchemy.org/docs/core/engines.html#database-urls

.. setting:: database_engine_options

``database_engine_options``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``{}`` (empty mapping).

To specify additional SQLAlchemy database engine options you can use
the :setting:`sqlalchmey_engine_options` setting::

    # echo enables verbose logging from SQLAlchemy.
    app.conf.database_engine_options = {'echo': True}

.. setting:: database_short_lived_sessions

``database_short_lived_sessions``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled by default.

Short lived sessions are disabled by default. If enabled they can drastically reduce
performance, especially on systems processing lots of tasks. This option is useful
on low-traffic workers that experience errors as a result of cached database connections
going stale through inactivity. For example, intermittent errors like
`(OperationalError) (2006, 'MySQL server has gone away')` can be fixed by enabling
short lived sessions. This option only affects the database backend.

.. setting:: database_table_names

``database_table_names``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``{}`` (empty mapping).

When SQLAlchemy is configured as the result backend, Celery automatically
creates two tables to store result meta-data for tasks. This setting allows
you to customize the table names:

.. code-block:: python

    # use custom table names for the database result backend.
    database_table_names = {
        'task': 'myapp_taskmeta',
        'group': 'myapp_groupmeta',
    }

.. _conf-rpc-result-backend:

RPC backend settings
--------------------

.. setting:: result_persistent

``result_persistent``
~~~~~~~~~~~~~~~~~~~~~

Default: Disabled by default (transient messages).

If set to :const:`True`, result messages will be persistent. This means the
messages won't be lost after a broker restart.

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    result_backend = 'rpc://'
    result_persistent = False

.. _conf-cache-result-backend:

Cache backend settings
----------------------

.. note::

    The cache backend supports the :pypi:`pylibmc` and :pypi:`python-memcached`
    libraries. The latter is used only if :pypi:`pylibmc` isn't installed.

Using a single Memcached server:

.. code-block:: python

    result_backend = 'cache+memcached://127.0.0.1:11211/'

Using multiple Memcached servers:

.. code-block:: python

    result_backend = """
        cache+memcached://172.19.26.240:11211;172.19.26.242:11211/
    """.strip()

The "memory" backend stores the cache in memory only:

.. code-block:: python

    result_backend = 'cache'
    cache_backend = 'memory'

.. setting:: cache_backend_options

``cache_backend_options``
~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``{}`` (empty mapping).

You can set :pypi:`pylibmc` options using the :setting:`cache_backend_options`
setting:

.. code-block:: python

    cache_backend_options = {
        'binary': True,
        'behaviors': {'tcp_nodelay': True},
    }

.. setting:: cache_backend

``cache_backend``
~~~~~~~~~~~~~~~~~

This setting is no longer used as it's now possible to specify
the cache backend directly in the :setting:`result_backend` setting.

.. _conf-redis-result-backend:

Redis backend settings
----------------------

Configuring the backend URL
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

    The Redis backend requires the :pypi:`redis` library.

    To install this package use :command:`pip`:

    .. code-block:: console

        $ pip install celery[redis]

    See :ref:`bundles` for information on combining multiple extension
    requirements.

This backend requires the :setting:`result_backend`
setting to be set to a Redis or `Redis over TLS`_ URL::

    result_backend = 'redis://:password@host:port/db'

.. _`Redis over TLS`:
    https://www.iana.org/assignments/uri-schemes/prov/rediss

For example::

    result_backend = 'redis://localhost/0'

is the same as::

    result_backend = 'redis://'

Use the ``rediss://`` protocol to connect to redis over TLS::

    result_backend = 'rediss://:password@host:port/db?ssl_cert_reqs=CERT_REQUIRED'

The fields of the URL are defined as follows:

#. ``password``

    Password used to connect to the database.

#. ``host``

    Host name or IP address of the Redis server (e.g., `localhost`).

#. ``port``

    Port to the Redis server. Default is 6379.

#. ``db``

    Database number to use. Default is 0.
    The db can include an optional leading slash.

When using a TLS connection (protocol is ``rediss://``), you may pass in all values in :setting:`broker_use_ssl` as query parameters. Paths to certificates must be URL encoded, and ``ssl_cert_reqs`` is required. Example:

.. code-block:: python

    result_backend = 'rediss://:password@host:port/db?\
        ssl_cert_reqs=CERT_REQUIRED\
        &ssl_ca_certs=%2Fvar%2Fssl%2Fmyca.pem\                  # /var/ssl/myca.pem
        &ssl_certfile=%2Fvar%2Fssl%2Fredis-server-cert.pem\     # /var/ssl/redis-server-cert.pem
        &ssl_keyfile=%2Fvar%2Fssl%2Fprivate%2Fworker-key.pem'   # /var/ssl/private/worker-key.pem


.. setting:: redis_backend_use_ssl

``redis_backend_use_ssl``
~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled.

The Redis backend supports SSL. The valid values of this options are the same
as :setting:`broker_use_ssl`.

.. setting:: redis_max_connections

``redis_max_connections``
~~~~~~~~~~~~~~~~~~~~~~~~~

Default: No limit.

Maximum number of connections available in the Redis connection
pool used for sending and retrieving results.

.. setting:: redis_socket_connect_timeout

``redis_socket_connect_timeout``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 5.0.1

Default: :const:`None`

Socket timeout for connections to Redis from the result backend
in seconds (int/float)

.. setting:: redis_socket_timeout

``redis_socket_timeout``
~~~~~~~~~~~~~~~~~~~~~~~~

Default: 120.0 seconds.

Socket timeout for reading/writing operations to the Redis server
in seconds (int/float), used by the redis result backend.

.. _conf-cassandra-result-backend:

Cassandra backend settings
--------------------------

.. note::

    This Cassandra backend driver requires :pypi:`cassandra-driver`.

    To install, use :command:`pip`:

    .. code-block:: console

        $ pip install celery[cassandra]

    See :ref:`bundles` for information on combining multiple extension
    requirements.

This backend requires the following configuration directives to be set.

.. setting:: cassandra_servers

``cassandra_servers``
~~~~~~~~~~~~~~~~~~~~~

Default: ``[]`` (empty list).

List of ``host`` Cassandra servers. For example::

    cassandra_servers = ['localhost']

.. setting:: cassandra_port

``cassandra_port``
~~~~~~~~~~~~~~~~~~

Default: 9042.

Port to contact the Cassandra servers on.

.. setting:: cassandra_keyspace

``cassandra_keyspace``
~~~~~~~~~~~~~~~~~~~~~~

Default: None.

The key-space in which to store the results. For example::

    cassandra_keyspace = 'tasks_keyspace'

.. setting:: cassandra_table

``cassandra_table``
~~~~~~~~~~~~~~~~~~~

Default: None.

The table (column family) in which to store the results. For example::

    cassandra_table = 'tasks'

.. setting:: cassandra_read_consistency

``cassandra_read_consistency``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: None.

The read consistency used. Values can be ``ONE``, ``TWO``, ``THREE``, ``QUORUM``, ``ALL``,
``LOCAL_QUORUM``, ``EACH_QUORUM``, ``LOCAL_ONE``.

.. setting:: cassandra_write_consistency

``cassandra_write_consistency``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: None.

The write consistency used. Values can be ``ONE``, ``TWO``, ``THREE``, ``QUORUM``, ``ALL``,
``LOCAL_QUORUM``, ``EACH_QUORUM``, ``LOCAL_ONE``.

.. setting:: cassandra_entry_ttl

``cassandra_entry_ttl``
~~~~~~~~~~~~~~~~~~~~~~~

Default: None.

Time-to-live for status entries. They will expire and be removed after that many seconds
after adding. A value of :const:`None` (default) means they will never expire.

.. setting:: cassandra_auth_provider

``cassandra_auth_provider``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: :const:`None`.

AuthProvider class within ``cassandra.auth`` module to use. Values can be
``PlainTextAuthProvider`` or ``SaslAuthProvider``.

.. setting:: cassandra_auth_kwargs

``cassandra_auth_kwargs``
~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``{}`` (empty mapping).

Named arguments to pass into the authentication provider. For example:

.. code-block:: python

    cassandra_auth_kwargs = {
        username: 'cassandra',
        password: 'cassandra'
    }

.. setting:: cassandra_options

``cassandra_options``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``{}`` (empty mapping).

Named arguments to pass into the ``cassandra.cluster`` class.

.. code-block:: python

    cassandra_options = {
        'cql_version': '3.2.1'
        'protocol_version': 3
    }

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    cassandra_servers = ['localhost']
    cassandra_keyspace = 'celery'
    cassandra_table = 'tasks'
    cassandra_read_consistency = 'ONE'
    cassandra_write_consistency = 'ONE'
    cassandra_entry_ttl = 86400

.. _conf-elasticsearch-result-backend:

Elasticsearch backend settings
------------------------------

To use `Elasticsearch`_ as the result backend you simply need to
configure the :setting:`result_backend` setting with the correct URL.

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    result_backend = 'elasticsearch://example.com:9200/index_name/doc_type'

.. setting:: elasticsearch_retry_on_timeout

``elasticsearch_retry_on_timeout``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: :const:`False`

Should timeout trigger a retry on different node?

.. setting:: elasticsearch_max_retries

``elasticsearch_max_retries``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: 3.

Maximum number of retries before an exception is propagated.

.. setting:: elasticsearch_timeout

``elasticsearch_timeout``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: 10.0 seconds.

Global timeout,used by the elasticsearch result backend.

.. _conf-riak-result-backend:

Riak backend settings
---------------------

.. note::

    The Riak backend requires the :pypi:`riak` library.

    To install the this package use :command:`pip`:

    .. code-block:: console

        $ pip install celery[riak]

    See :ref:`bundles` for information on combining multiple extension
    requirements.

This backend requires the :setting:`result_backend`
setting to be set to a Riak URL::

    result_backend = 'riak://host:port/bucket'

For example::

    result_backend = 'riak://localhost/celery

is the same as::

    result_backend = 'riak://'

The fields of the URL are defined as follows:

#. ``host``

    Host name or IP address of the Riak server (e.g., `'localhost'`).

#. ``port``

    Port to the Riak server using the protobuf protocol. Default is 8087.

#. ``bucket``

    Bucket name to use. Default is `celery`.
    The bucket needs to be a string with ASCII characters only.

Alternatively, this backend can be configured with the following configuration directives.

.. setting:: riak_backend_settings

``riak_backend_settings``
~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``{}`` (empty mapping).

This is a dict supporting the following keys:

* ``host``

    The host name of the Riak server. Defaults to ``"localhost"``.

* ``port``

    The port the Riak server is listening to. Defaults to 8087.

* ``bucket``

    The bucket name to connect to. Defaults to "celery".

* ``protocol``

    The protocol to use to connect to the Riak server. This isn't configurable
    via :setting:`result_backend`

.. _conf-dynamodb-result-backend:

AWS DynamoDB backend settings
-----------------------------

.. note::

    The Dynamodb backend requires the :pypi:`boto3` library.

    To install this package use :command:`pip`:

    .. code-block:: console

        $ pip install celery[dynamodb]

    See :ref:`bundles` for information on combining multiple extension
    requirements.

This backend requires the :setting:`result_backend`
setting to be set to a DynamoDB URL::

    result_backend = 'dynamodb://aws_access_key_id:aws_secret_access_key@region:port/table?read=n&write=m'

For example, specifying the AWS region and the table name::

    result_backend = 'dynamodb://@us-east-1/celery_results

or retrieving AWS configuration parameters from the environment, using the default table name (``celery``)
and specifying read and write provisioned throughput::

    result_backend = 'dynamodb://@/?read=5&write=5'

or using the `downloadable version <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html>`_
of DynamoDB
`locally <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.Endpoint.html>`_::

    result_backend = 'dynamodb://@localhost:8000'

or using downloadable version or other service with conforming API deployed on any host::

    result_backend = 'dynamodb://@us-east-1'
    dynamodb_endpoint_url = 'http://192.168.0.40:8000'

The fields of the DynamoDB URL in ``result_backend`` are defined as follows:

#. ``aws_access_key_id & aws_secret_access_key``

    The credentials for accessing AWS API resources. These can also be resolved
    by the :pypi:`boto3` library from various sources, as
    described `here <http://boto3.readthedocs.io/en/latest/guide/configuration.html#configuring-credentials>`_.

#. ``region``

    The AWS region, e.g. ``us-east-1`` or ``localhost`` for the `Downloadable Version <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html>`_.
    See the :pypi:`boto3` library `documentation <http://boto3.readthedocs.io/en/latest/guide/configuration.html#environment-variable-configuration>`_
    for definition options.

#. ``port``

   The listening port of the local DynamoDB instance, if you are using the downloadable version.
   If you have not specified the ``region`` parameter as ``localhost``,
   setting this parameter has **no effect**.

#. ``table``

    Table name to use. Default is ``celery``.
    See the `DynamoDB Naming Rules <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-naming-rules>`_
    for information on the allowed characters and length.

#. ``read & write``

    The Read & Write Capacity Units for the created DynamoDB table. Default is ``1`` for both read and write.
    More details can be found in the `Provisioned Throughput documentation <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ProvisionedThroughput.html>`_.

.. _conf-ironcache-result-backend:

IronCache backend settings
--------------------------

.. note::

    The IronCache backend requires the :pypi:`iron_celery` library:

    To install this package use :command:`pip`:

    .. code-block:: console

        $ pip install iron_celery

IronCache is configured via the URL provided in :setting:`result_backend`, for example::

    result_backend = 'ironcache://project_id:token@'

Or to change the cache name::

    ironcache:://project_id:token@/awesomecache

For more information, see: https://github.com/iron-io/iron_celery

.. _conf-couchbase-result-backend:

Couchbase backend settings
--------------------------

.. note::

    The Couchbase backend requires the :pypi:`couchbase` library.

    To install this package use :command:`pip`:

    .. code-block:: console

        $ pip install celery[couchbase]

    See :ref:`bundles` for instructions how to combine multiple extension
    requirements.

This backend can be configured via the :setting:`result_backend`
set to a Couchbase URL:

.. code-block:: python

    result_backend = 'couchbase://username:password@host:port/bucket'

.. setting:: couchbase_backend_settings

``couchbase_backend_settings``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``{}`` (empty mapping).

This is a dict supporting the following keys:

* ``host``

    Host name of the Couchbase server. Defaults to ``localhost``.

* ``port``

    The port the Couchbase server is listening to. Defaults to ``8091``.

* ``bucket``

    The default bucket the Couchbase server is writing to.
    Defaults to ``default``.

* ``username``

    User name to authenticate to the Couchbase server as (optional).

* ``password``

    Password to authenticate to the Couchbase server (optional).

.. _conf-couchdb-result-backend:

CouchDB backend settings
------------------------

.. note::

    The CouchDB backend requires the :pypi:`pycouchdb` library:

    To install this Couchbase package use :command:`pip`:

    .. code-block:: console

        $ pip install celery[couchdb]

    See :ref:`bundles` for information on combining multiple extension
    requirements.

This backend can be configured via the :setting:`result_backend`
set to a CouchDB URL::

    result_backend = 'couchdb://username:password@host:port/container'

The URL is formed out of the following parts:

* ``username``

    User name to authenticate to the CouchDB server as (optional).

* ``password``

    Password to authenticate to the CouchDB server (optional).

* ``host``

    Host name of the CouchDB server. Defaults to ``localhost``.

* ``port``

    The port the CouchDB server is listening to. Defaults to ``8091``.

* ``container``

    The default container the CouchDB server is writing to.
    Defaults to ``default``.

.. _conf-filesystem-result-backend:

File-system backend settings
----------------------------

This backend can be configured using a file URL, for example::

    CELERY_RESULT_BACKEND = 'file:///var/celery/results'

The configured directory needs to be shared and writable by all servers using
the backend.

If you're trying Celery on a single system you can simply use the backend
without any further configuration. For larger clusters you could use NFS,
`GlusterFS`_, CIFS, `HDFS`_ (using FUSE), or any other file-system.

.. _`GlusterFS`: http://www.gluster.org/
.. _`HDFS`: http://hadoop.apache.org/

.. _conf-consul-result-backend:

Consul K/V store backend settings
---------------------------------

The Consul backend can be configured using a URL, for example:

    CELERY_RESULT_BACKEND = 'consul://localhost:8500/'

The backend will storage results in the K/V store of Consul
as individual keys.

The backend supports auto expire of results using TTLs in Consul.

.. _conf-messaging:

Message Routing
---------------

.. _conf-messaging-routing:

.. setting:: task_queues

``task_queues``
~~~~~~~~~~~~~~~

Default: :const:`None` (queue taken from default queue settings).

Most users will not want to specify this setting and should rather use
the :ref:`automatic routing facilities <routing-automatic>`.

If you really want to configure advanced routing, this setting should
be a list of :class:`kombu.Queue` objects the worker will consume from.

Note that workers can be overridden this setting via the
:option:`-Q <celery worker -Q>` option, or individual queues from this
list (by name) can be excluded using the :option:`-X <celery worker -X>`
option.

Also see :ref:`routing-basics` for more information.

The default is a queue/exchange/binding key of ``celery``, with
exchange type ``direct``.

See also :setting:`task_routes`

.. setting:: task_routes

``task_routes``
~~~~~~~~~~~~~~~

Default: :const:`None`.

A list of routers, or a single router used to route tasks to queues.
When deciding the final destination of a task the routers are consulted
in order.

A router can be specified as either:

*  A function with the signature ``(name, args, kwargs,
   options, task=None, **kwargs)``
*  A string providing the path to a router function.
*  A dict containing router specification:
     Will be converted to a :class:`celery.routes.MapRoute` instance.
* A list of ``(pattern, route)`` tuples:
     Will be converted to a :class:`celery.routes.MapRoute` instance.

Examples:

.. code-block:: python

    task_routes = {
        'celery.ping': 'default',
        'mytasks.add': 'cpu-bound',
        'feed.tasks.*': 'feeds',                           # <-- glob pattern
        re.compile(r'(image|video)\.tasks\..*'): 'media',  # <-- regex
        'video.encode': {
            'queue': 'video',
            'exchange': 'media'
            'routing_key': 'media.video.encode',
        },
    }

    task_routes = ('myapp.tasks.route_task', {'celery.ping': 'default})

Where ``myapp.tasks.route_task`` could be:

.. code-block:: python

    def route_task(self, name, args, kwargs, options, task=None, **kw):
            if task == 'celery.ping':
                return {'queue': 'default'}

``route_task`` may return a string or a dict. A string then means
it's a queue name in :setting:`task_queues`, a dict means it's a custom route.

When sending tasks, the routers are consulted in order. The first
router that doesn't return ``None`` is the route to use. The message options
is then merged with the found route settings, where the routers settings
have priority.

Example if :func:`~celery.execute.apply_async` has these arguments:

.. code-block:: python

   Task.apply_async(immediate=False, exchange='video',
                    routing_key='video.compress')

and a router returns:

.. code-block:: python

    {'immediate': True, 'exchange': 'urgent'}

the final message options will be:

.. code-block:: python

    immediate=True, exchange='urgent', routing_key='video.compress'

(and any default message options defined in the
:class:`~celery.task.base.Task` class)

Values defined in :setting:`task_routes` have precedence over values defined in
:setting:`task_queues` when merging the two.

With the follow settings:

.. code-block:: python

    task_queues = {
        'cpubound': {
            'exchange': 'cpubound',
            'routing_key': 'cpubound',
        },
    }

    task_routes = {
        'tasks.add': {
            'queue': 'cpubound',
            'routing_key': 'tasks.add',
            'serializer': 'json',
        },
    }

The final routing options for ``tasks.add`` will become:

.. code-block:: javascript

    {'exchange': 'cpubound',
     'routing_key': 'tasks.add',
     'serializer': 'json'}

See :ref:`routers` for more examples.

.. setting:: task_queue_ha_policy

``task_queue_ha_policy``
~~~~~~~~~~~~~~~~~~~~~~~~
:brokers: RabbitMQ

Default: :const:`None`.

This will set the default HA policy for a queue, and the value
can either be a string (usually ``all``):

.. code-block:: python

    task_queue_ha_policy = 'all'

Using 'all' will replicate the queue to all current nodes,
Or you can give it a list of nodes to replicate to:

.. code-block:: python

    task_queue_ha_policy = ['rabbit@host1', 'rabbit@host2']

Using a list will implicitly set ``x-ha-policy`` to 'nodes' and
``x-ha-policy-params`` to the given list of nodes.

See http://www.rabbitmq.com/ha.html for more information.

.. setting:: task_queue_max_priority

``task_queue_max_priority``
~~~~~~~~~~~~~~~~~~~~~~~~~~~
:brokers: RabbitMQ

Default: :const:`None`.

See :ref:`routing-options-rabbitmq-priorities`.

.. setting:: worker_direct

``worker_direct``
~~~~~~~~~~~~~~~~~

Default: Disabled.

This option enables so that every worker has a dedicated queue,
so that tasks can be routed to specific workers.

The queue name for each worker is automatically generated based on
the worker hostname and a ``.dq`` suffix, using the ``C.dq`` exchange.

For example the queue name for the worker with node name ``w1@example.com``
becomes::

    w1@example.com.dq

Then you can route the task to the task by specifying the hostname
as the routing key and the ``C.dq`` exchange::

    task_routes = {
        'tasks.add': {'exchange': 'C.dq', 'routing_key': 'w1@example.com'}
    }

.. setting:: task_create_missing_queues

``task_create_missing_queues``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Enabled.

If enabled (default), any queues specified that aren't defined in
:setting:`task_queues` will be automatically created. See
:ref:`routing-automatic`.

.. setting:: task_default_queue

``task_default_queue``
~~~~~~~~~~~~~~~~~~~~~~

Default: ``"celery"``.

The name of the default queue used by `.apply_async` if the message has
no route or no custom queue has been specified.

This queue must be listed in :setting:`task_queues`.
If :setting:`task_queues` isn't specified then it's automatically
created containing one queue entry, where this name is used as the name of
that queue.

.. seealso::

    :ref:`routing-changing-default-queue`

.. setting:: task_default_exchange

``task_default_exchange``
~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``"celery"``.

Name of the default exchange to use when no custom exchange is
specified for a key in the :setting:`task_queues` setting.

.. setting:: task_default_exchange_type

``task_default_exchange_type``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``"direct"``.

Default exchange type used when no custom exchange type is specified
for a key in the :setting:`task_queues` setting.

.. setting:: task_default_routing_key

``task_default_routing_key``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``"celery"``.

The default routing key used when no custom routing key
is specified for a key in the :setting:`task_queues` setting.

.. setting:: task_default_delivery_mode

``task_default_delivery_mode``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``"persistent"``.

Can be `transient` (messages not written to disk) or `persistent` (written to
disk).

.. _conf-broker-settings:

Broker Settings
---------------

.. setting:: broker_url

``broker_url``
~~~~~~~~~~~~~~

Default: ``"amqp://"``

Default broker URL. This must be a URL in the form of::

    transport://userid:password@hostname:port/virtual_host

Only the scheme part (``transport://``) is required, the rest
is optional, and defaults to the specific transports default values.

The transport part is the broker implementation to use, and the
default is ``amqp``, (uses ``librabbitmq`` if installed or falls back to
``pyamqp``). There are also other choices available, including;
``redis://``, ``sqs://``, and ``qpid://``.

The scheme can also be a fully qualified path to your own transport
implementation::

    broker_url = 'proj.transports.MyTransport://localhost'

More than one broker URL, of the same transport, can also be specified.
The broker URLs can be passed in as a single string that's semicolon delimited::

    broker_url = 'transport://userid:password@hostname:port//;transport://userid:password@hostname:port//'

Or as a list::

    broker_url = [
        'transport://userid:password@localhost:port//',
        'transport://userid:password@hostname:port//'
    ]

The brokers will then be used in the :setting:`broker_failover_strategy`.

See :ref:`kombu:connection-urls` in the Kombu documentation for more
information.

.. setting:: broker_read_url

.. setting:: broker_write_url

``broker_read_url`` / ``broker_write_url``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Taken from :setting:`broker_url`.

These settings can be configured, instead of :setting:`broker_url` to specify
different connection parameters for broker connections used for consuming and
producing.

Example::

    broker_read_url = 'amqp://user:pass@broker.example.com:56721'
    broker_write_url = 'amqp://user:pass@broker.example.com:56722'

Both options can also be specified as a list for failover alternates, see
:setting:`broker_url` for more information.

.. setting:: broker_failover_strategy

``broker_failover_strategy``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``"round-robin"``.

Default failover strategy for the broker Connection object. If supplied,
may map to a key in 'kombu.connection.failover_strategies', or be a reference
to any method that yields a single item from a supplied list.

Example::

    # Random failover strategy
    def random_failover_strategy(servers):
        it = list(servers)  # don't modify callers list
        shuffle = random.shuffle
        for _ in repeat(None):
            shuffle(it)
            yield it[0]

    broker_failover_strategy = random_failover_strategy

.. setting:: broker_heartbeat

``broker_heartbeat``
~~~~~~~~~~~~~~~~~~~~
:transports supported: ``pyamqp``

Default: ``120.0`` (negotiated by server).

Note: This value is only used by the worker, clients do not use
a heartbeat at the moment.

It's not always possible to detect connection loss in a timely
manner using TCP/IP alone, so AMQP defines something called heartbeats
that's is used both by the client and the broker to detect if
a connection was closed.

If the heartbeat value is 10 seconds, then
the heartbeat will be monitored at the interval specified
by the :setting:`broker_heartbeat_checkrate` setting (by default
this is set to double the rate of the heartbeat value,
so for the 10 seconds, the heartbeat is checked every 5 seconds).

.. setting:: broker_heartbeat_checkrate

``broker_heartbeat_checkrate``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:transports supported: ``pyamqp``

Default: 2.0.

At intervals the worker will monitor that the broker hasn't missed
too many heartbeats. The rate at which this is checked is calculated
by dividing the :setting:`broker_heartbeat` value with this value,
so if the heartbeat is 10.0 and the rate is the default 2.0, the check
will be performed every 5 seconds (twice the heartbeat sending rate).

.. setting:: broker_use_ssl

``broker_use_ssl``
~~~~~~~~~~~~~~~~~~
:transports supported: ``pyamqp``, ``redis``

Default: Disabled.

Toggles SSL usage on broker connection and SSL settings.

The valid values for this option vary by transport.

``pyamqp``
__________

If ``True`` the connection will use SSL with default SSL settings.
If set to a dict, will configure SSL connection according to the specified
policy. The format used is Python's :func:`ssl.wrap_socket` options.

Note that SSL socket is generally served on a separate port by the broker.

Example providing a client cert and validating the server cert against a custom
certificate authority:

.. code-block:: python

    import ssl

    broker_use_ssl = {
      'keyfile': '/var/ssl/private/worker-key.pem',
      'certfile': '/var/ssl/amqp-server-cert.pem',
      'ca_certs': '/var/ssl/myca.pem',
      'cert_reqs': ssl.CERT_REQUIRED
    }

.. warning::

    Be careful using ``broker_use_ssl=True``. It's possible that your default
    configuration won't validate the server cert at all. Please read Python
    `ssl module security
    considerations <https://docs.python.org/3/library/ssl.html#ssl-security>`_.

``redis``
_________


The setting must be a dict the keys:

*  ``ssl_cert_reqs`` (required): one of the ``SSLContext.verify_mode`` values:
    * ``ssl.CERT_NONE``
    * ``ssl.CERT_OPTIONAL``
    * ``ssl.CERT_REQUIRED``
*  ``ssl_ca_certs`` (optional): path to the CA certificate
*  ``ssl_certfile`` (optional): path to the client certificate
*  ``ssl_keyfile`` (optional): path to the client key


.. setting:: broker_pool_limit

``broker_pool_limit``
~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.3

Default: 10.

The maximum number of connections that can be open in the connection pool.

The pool is enabled by default since version 2.5, with a default limit of ten
connections. This number can be tweaked depending on the number of
threads/green-threads (eventlet/gevent) using a connection. For example
running eventlet with 1000 greenlets that use a connection to the broker,
contention can arise and you should consider increasing the limit.

If set to :const:`None` or 0 the connection pool will be disabled and
connections will be established and closed for every use.

.. setting:: broker_connection_timeout

``broker_connection_timeout``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: 4.0.

The default timeout in seconds before we give up establishing a connection
to the AMQP server. This setting is disabled when using
gevent.

.. note::

    The broker connection timeout only applies to a worker attempting to
    connect to the broker. It does not apply to producer sending a task, see
    :setting:`broker_transport_options` for how to provide a timeout for that
    situation.

.. setting:: broker_connection_retry

``broker_connection_retry``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Enabled.

Automatically try to re-establish the connection to the AMQP broker if lost.

The time between retries is increased for each retry, and is
not exhausted before :setting:`broker_connection_max_retries` is
exceeded.

.. setting:: broker_connection_max_retries

``broker_connection_max_retries``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: 100.

Maximum number of retries before we give up re-establishing a connection
to the AMQP broker.

If this is set to :const:`0` or :const:`None`, we'll retry forever.

.. setting:: broker_login_method

``broker_login_method``
~~~~~~~~~~~~~~~~~~~~~~~

Default: ``"AMQPLAIN"``.

Set custom amqp login method.

.. setting:: broker_transport_options

``broker_transport_options``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Default: ``{}`` (empty mapping).

A dict of additional options passed to the underlying transport.

See your transport user manual for supported options (if any).

Example setting the visibility timeout (supported by Redis and SQS
transports):

.. code-block:: python

    broker_transport_options = {'visibility_timeout': 18000}  # 5 hours

.. _conf-worker:

Worker
------

.. setting:: imports

``imports``
~~~~~~~~~~~

Default: ``[]`` (empty list).

A sequence of modules to import when the worker starts.

This is used to specify the task modules to import, but also
to import signal handlers and additional remote control commands, etc.

The modules will be imported in the original order.

.. setting:: include

``include``
~~~~~~~~~~~

Default: ``[]`` (empty list).

Exact same semantics as :setting:`imports`, but can be used as a means
to have different import categories.

The modules in this setting are imported after the modules in
:setting:`imports`.

.. _conf-concurrency:

.. setting:: worker_concurrency

``worker_concurrency``
~~~~~~~~~~~~~~~~~~~~~~

Default: Number of CPU cores.

The number of concurrent worker processes/threads/green threads executing
tasks.

If you're doing mostly I/O you can have more processes,
but if mostly CPU-bound, try to keep it close to the
number of CPUs on your machine. If not set, the number of CPUs/cores
on the host will be used.

.. setting:: worker_prefetch_multiplier

``worker_prefetch_multiplier``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: 4.

How many messages to prefetch at a time multiplied by the number of
concurrent processes. The default is 4 (four messages for each
process). The default setting is usually a good choice, however -- if you
have very long running tasks waiting in the queue and you have to start the
workers, note that the first worker to start will receive four times the
number of messages initially. Thus the tasks may not be fairly distributed
to the workers.

To disable prefetching, set :setting:`worker_prefetch_multiplier` to 1.
Changing that setting to 0 will allow the worker to keep consuming
as many messages as it wants.

For more on prefetching, read :ref:`optimizing-prefetch-limit`

.. note::

    Tasks with ETA/countdown aren't affected by prefetch limits.

.. setting:: worker_lost_wait

``worker_lost_wait``
~~~~~~~~~~~~~~~~~~~~

Default: 10.0 seconds.

In some cases a worker may be killed without proper cleanup,
and the worker may have published a result before terminating.
This value specifies how long we wait for any missing results before
raising a :exc:`@WorkerLostError` exception.

.. setting:: worker_max_tasks_per_child

``worker_max_tasks_per_child``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum number of tasks a pool worker process can execute before
it's replaced with a new one. Default is no limit.

.. setting:: worker_max_memory_per_child

``worker_max_memory_per_child``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: No limit.
Type: int (kilobytes)

Maximum amount of resident memory, in kilobytes, that may be consumed by a
worker before it will be replaced by a new worker. If a single
task causes a worker to exceed this limit, the task will be
completed, and the worker will be replaced afterwards.

Example:

.. code-block:: python

    worker_max_memory_per_child = 12000  # 12MB

.. setting:: worker_disable_rate_limits

``worker_disable_rate_limits``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled (rate limits enabled).

Disable all rate limits, even if tasks has explicit rate limits set.

.. setting:: worker_state_db

``worker_state_db``
~~~~~~~~~~~~~~~~~~~

Default: :const:`None`.

Name of the file used to stores persistent worker state (like revoked tasks).
Can be a relative or absolute path, but be aware that the suffix `.db`
may be appended to the file name (depending on Python version).

Can also be set via the :option:`celery worker --statedb` argument.

.. setting:: worker_timer_precision

``worker_timer_precision``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: 1.0 seconds.

Set the maximum time in seconds that the ETA scheduler can sleep between
rechecking the schedule.

Setting this value to 1 second means the schedulers precision will
be 1 second. If you need near millisecond precision you can set this to 0.1.

.. setting:: worker_enable_remote_control

``worker_enable_remote_control``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Enabled by default.

Specify if remote control of the workers is enabled.

.. _conf-events:

Events
------

.. setting:: worker_send_task_events

``worker_send_task_events``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled by default.

Send task-related events so that tasks can be monitored using tools like
`flower`. Sets the default value for the workers
:option:`-E <celery worker -E>` argument.

.. setting:: task_send_sent_event

``task_send_sent_event``
~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Default: Disabled by default.

If enabled, a :event:`task-sent` event will be sent for every task so tasks can be
tracked before they're consumed by a worker.

.. setting:: event_queue_ttl

``event_queue_ttl``
~~~~~~~~~~~~~~~~~~~
:transports supported: ``amqp``

Default: 5.0 seconds.

Message expiry time in seconds (int/float) for when messages sent to a monitor clients
event queue is deleted (``x-message-ttl``)

For example, if this value is set to 10 then a message delivered to this queue
will be deleted after 10 seconds.

.. setting:: event_queue_expires

``event_queue_expires``
~~~~~~~~~~~~~~~~~~~~~~~
:transports supported: ``amqp``

Default: 60.0 seconds.

Expiry time in seconds (int/float) for when after a monitor clients
event queue will be deleted (``x-expires``).

.. setting:: event_queue_prefix

``event_queue_prefix``
~~~~~~~~~~~~~~~~~~~~~~

Default: ``"celeryev"``.

The prefix to use for event receiver queue names.

.. setting:: event_serializer

``event_serializer``
~~~~~~~~~~~~~~~~~~~~

Default: ``"json"``.

Message serialization format used when sending event messages.

.. seealso::

    :ref:`calling-serializers`.


.. _conf-control:

Remote Control Commands
-----------------------

.. note::

    To disable remote control commands see
    the :setting:`worker_enable_remote_control` setting.

.. setting:: control_queue_ttl

``control_queue_ttl``
~~~~~~~~~~~~~~~~~~~~~

Default: 300.0

Time in seconds, before a message in a remote control command queue
will expire.

If using the default of 300 seconds, this means that if a remote control
command is sent and no worker picks it up within 300 seconds, the command
is discarded.

This setting also applies to remote control reply queues.

.. setting:: control_queue_expires

``control_queue_expires``
~~~~~~~~~~~~~~~~~~~~~~~~~

Default: 10.0

Time in seconds, before an unused remote control command queue is deleted
from the broker.

This setting also applies to remote control reply queues.

.. _conf-logging:

Logging
-------

.. setting:: worker_hijack_root_logger

``worker_hijack_root_logger``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Default: Enabled by default (hijack root logger).

By default any previously configured handlers on the root logger will be
removed. If you want to customize your own logging handlers, then you
can disable this behavior by setting
`worker_hijack_root_logger = False`.

.. note::

    Logging can also be customized by connecting to the
    :signal:`celery.signals.setup_logging` signal.

.. setting:: worker_log_color

``worker_log_color``
~~~~~~~~~~~~~~~~~~~~

Default: Enabled if app is logging to a terminal.

Enables/disables colors in logging output by the Celery apps.

.. setting:: worker_log_format

``worker_log_format``
~~~~~~~~~~~~~~~~~~~~~

Default:

.. code-block:: text

    "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s"

The format to use for log messages.

See the Python :mod:`logging` module for more information about log
formats.

.. setting:: worker_task_log_format

``worker_task_log_format``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Default:

.. code-block:: text

    "[%(asctime)s: %(levelname)s/%(processName)s]
        [%(task_name)s(%(task_id)s)] %(message)s"

The format to use for log messages logged in tasks.

See the Python :mod:`logging` module for more information about log
formats.

.. setting:: worker_redirect_stdouts

``worker_redirect_stdouts``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: Enabled by default.

If enabled `stdout` and `stderr` will be redirected
to the current logger.

Used by :program:`celery worker` and :program:`celery beat`.

.. setting:: worker_redirect_stdouts_level

``worker_redirect_stdouts_level``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: :const:`WARNING`.

The log level output to `stdout` and `stderr` is logged as.
Can be one of :const:`DEBUG`, :const:`INFO`, :const:`WARNING`,
:const:`ERROR`, or :const:`CRITICAL`.

.. _conf-security:

Security
--------

.. setting:: security_key

``security_key``
~~~~~~~~~~~~~~~~

Default: :const:`None`.

.. versionadded:: 2.5

The relative or absolute path to a file containing the private key
used to sign messages when :ref:`message-signing` is used.

.. setting:: security_certificate

``security_certificate``
~~~~~~~~~~~~~~~~~~~~~~~~

Default: :const:`None`.

.. versionadded:: 2.5

The relative or absolute path to an X.509 certificate file
used to sign messages when :ref:`message-signing` is used.

.. setting:: security_cert_store

``security_cert_store``
~~~~~~~~~~~~~~~~~~~~~~~

Default: :const:`None`.

.. versionadded:: 2.5

The directory containing X.509 certificates used for
:ref:`message-signing`. Can be a glob with wild-cards,
(for example :file:`/etc/certs/*.pem`).

.. _conf-custom-components:

Custom Component Classes (advanced)
-----------------------------------

.. setting:: worker_pool

``worker_pool``
~~~~~~~~~~~~~~~

Default: ``"prefork"`` (``celery.concurrency.prefork:TaskPool``).

Name of the pool class used by the worker.

.. admonition:: Eventlet/Gevent

    Never use this option to select the eventlet or gevent pool.
    You must use the :option:`-P <celery worker -P>` option to
    :program:`celery worker` instead, to ensure the monkey patches
    aren't applied too late, causing things to break in strange ways.

.. setting:: worker_pool_restarts

``worker_pool_restarts``
~~~~~~~~~~~~~~~~~~~~~~~~

Default: Disabled by default.

If enabled the worker pool can be restarted using the
:control:`pool_restart` remote control command.

.. setting:: worker_autoscaler

``worker_autoscaler``
~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.2

Default: ``"celery.worker.autoscale:Autoscaler"``.

Name of the autoscaler class to use.

.. setting:: worker_consumer

``worker_consumer``
~~~~~~~~~~~~~~~~~~~

Default: ``"celery.worker.consumer:Consumer"``.

Name of the consumer class used by the worker.

.. setting:: worker_timer

``worker_timer``
~~~~~~~~~~~~~~~~

Default: ``"kombu.asynchronous.hub.timer:Timer"``.

Name of the ETA scheduler class used by the worker.
Default is or set by the pool implementation.

.. _conf-celerybeat:

Beat Settings (:program:`celery beat`)
--------------------------------------

.. setting:: beat_schedule

``beat_schedule``
~~~~~~~~~~~~~~~~~

Default: ``{}`` (empty mapping).

The periodic task schedule used by :mod:`~celery.bin.beat`.
See :ref:`beat-entries`.

.. setting:: beat_scheduler

``beat_scheduler``
~~~~~~~~~~~~~~~~~~

Default: ``"celery.beat:PersistentScheduler"``.

The default scheduler class. May be set to
``"django_celery_beat.schedulers:DatabaseScheduler"`` for instance,
if used alongside `django-celery-beat` extension.

Can also be set via the :option:`celery beat -S` argument.

.. setting:: beat_schedule_filename

``beat_schedule_filename``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: ``"celerybeat-schedule"``.

Name of the file used by `PersistentScheduler` to store the last run times
of periodic tasks. Can be a relative or absolute path, but be aware that the
suffix `.db` may be appended to the file name (depending on Python version).

Can also be set via the :option:`celery beat --schedule` argument.

.. setting:: beat_sync_every

``beat_sync_every``
~~~~~~~~~~~~~~~~~~~

Default: 0.

The number of periodic tasks that can be called before another database sync
is issued.
A value of 0 (default) means sync based on timing - default of 3 minutes as determined by
scheduler.sync_every. If set to 1, beat will call sync after every task
message sent.

.. setting:: beat_max_loop_interval

``beat_max_loop_interval``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Default: 0.

The maximum number of seconds :mod:`~celery.bin.beat` can sleep
between checking the schedule.

The default for this value is scheduler specific.
For the default Celery beat scheduler the value is 300 (5 minutes),
but for the :pypi:`django-celery-beat` database scheduler it's 5 seconds
because the schedule may be changed externally, and so it must take
changes to the schedule into account.

Also when running Celery beat embedded (:option:`-B <celery worker -B>`)
on Jython as a thread the max interval is overridden and set to 1 so
that it's possible to shut down in a timely manner.
