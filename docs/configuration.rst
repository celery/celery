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
    BROKER_HOST = "localhost"
    BROKER_PORT = 5672
    BROKER_VHOST = "/"
    BROKER_USER = "guest"
    BROKER_PASSWORD = "guest"

    ## Worker settings
    ## If you're doing mostly I/O you can have more processes,
    ## but if mostly spending CPU, try to keep it close to the
    ## number of CPUs on your machine. If not set, the number of CPUs/cores
    ## available will be used.
    CELERYD_CONCURRENCY = 10
    # CELERYD_LOG_FILE = "celeryd.log"
    # CELERYD_LOG_LEVEL = "INFO"


Configuration Directives
========================

.. _conf-concurrency:

Concurrency settings
--------------------

.. _CELERYD_CONCURRENCY:

CELERYD_CONCURRENCY
~~~~~~~~~~~~~~~~~~~

The number of concurrent worker processes, executing tasks simultaneously.

Defaults to the number of CPUs/cores available.

.. _CELERYD_PREFETCH_MULTIPLIER:

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

.. _CELERY_RESULT_BACKEND:

CELERY_RESULT_BACKEND
~~~~~~~~~~~~~~~~~~~~~

The backend used to store task results (tombstones).
Can be one of the following:

* database (default)
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

.. warning:

    While the AMQP result backend is very efficient, you must make sure
    you only receive the same result once.  See :doc:`userguide/executing`).

.. _`SQLAlchemy`: http://sqlalchemy.org
.. _`memcached`: http://memcached.org
.. _`MongoDB`: http://mongodb.org
.. _`Redis`: http://code.google.com/p/redis/
.. _`Tokyo Tyrant`: http://1978th.net/tokyotyrant/

.. _conf-database-result-backend:

Database backend settings
-------------------------

.. _CELERY_RESULT_DBURI:

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

.. _CELERY_RESULT_ENGINE_OPTIONS:

CELERY_RESULT_ENGINE_OPTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To specify additional SQLAlchemy database engine options you can use
the :setting:`CELERY_RESULT_ENGINE_OPTIONS` setting::

    # echo enables verbose logging from SQLAlchemy.
    CELERY_RESULT_ENGINE_OPTIONS = {"echo": True}

.. _`Supported Databases`:
    http://www.sqlalchemy.org/docs/dbengine.html#supported-databases

.. _`Connection String`:
    http://www.sqlalchemy.org/docs/dbengine.html#create-engine-url-arguments

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CELERY_RESULT_BACKEND = "database"
    CELERY_RESULT_DBURI = "mysql://user:password@host/dbname"

.. _conf-amqp-result-backend:

AMQP backend settings
---------------------

.. _CELERY_AMQP_TASK_RESULT_EXPIRES:

CELERY_AMQP_TASK_RESULT_EXPIRES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The time in seconds of which the task result queues should expire.

.. note::

    AMQP result expiration requires RabbitMQ versions 2.1.0 and higher.

.. _CELERY_RESULT_EXCHANGE:

CELERY_RESULT_EXCHANGE
~~~~~~~~~~~~~~~~~~~~~~

Name of the exchange to publish results in.  Default is ``"celeryresults"``.

.. _CELERY_RESULT_EXCHANGE_TYPE:

CELERY_RESULT_EXCHANGE_TYPE
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The exchange type of the result exchange.  Default is to use a ``direct``
exchange.

.. _CELERY_RESULT_SERIALIZER:

CELERY_RESULT_SERIALIZER
~~~~~~~~~~~~~~~~~~~~~~~~

Result message serialization format.  Default is ``"pickle"``.

.. _CELERY_RESULT_PERSISTENT:

CELERY_RESULT_PERSISTENT
~~~~~~~~~~~~~~~~~~~~~~~~

If set to :const:`True`, result messages will be persistent.  This means the
messages will not be lost after a broker restart.  The default is for the
results to be transient.

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CELERY_RESULT_BACKEND = "amqp"
    CELERY_AMQP_TASK_RESULT_EXPIRES = 18000  # 5 hours.

.. _conf-cache-result-backend:

Cache backend settings
----------------------

.. note::

    The cache backend supports the `pylibmc`_ and `python-memcached`
    libraries.  The latter is used only if `pylibmc`_ is not installed.

.. _CELERY_CACHE_BACKEND:

CELERY_CACHE_BACKEND
~~~~~~~~~~~~~~~~~~~~

Using a single memcached server:

.. code-block:: python

    CELERY_CACHE_BACKEND = 'memcached://127.0.0.1:11211/'

Using multiple memcached servers:

.. code-block:: python

    CELERY_RESULT_BACKEND = "cache"
    CELERY_CACHE_BACKEND = 'memcached://172.19.26.240:11211;172.19.26.242:11211/'

.. _CELERY_CACHE_BACKEND_OPTIONS:

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

.. _TT_HOST:

TT_HOST
~~~~~~~

Hostname of the Tokyo Tyrant server.

.. _TT_PORT:

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
    http://pypi.python.org/pypi/redis/0.5.5

    To install the redis package use ``pip`` or ``easy_install``::

        $ pip install redis

This backend requires the following configuration directives to be set.

.. _REDIS_HOST:

REDIS_HOST
~~~~~~~~~~

Hostname of the Redis database server. e.g. ``"localhost"``.

.. _REDIS_PORT:

REDIS_PORT
~~~~~~~~~~

Port to the Redis database server. e.g. ``6379``.

.. _REDIS_DB:

REDIS_DB
~~~~~~~~

Database number to use. Default is 0

.. _REDIS_PASSWORD:

REDIS_PASSWORD
~~~~~~~~~~~~~~

Password used to connect to the database.

Example configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    CELERY_RESULT_BACKEND = "redis"
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_DB = "celery_results"
    REDIS_CONNECT_RETRY=True

.. _conf-mongodb-result-backend:

MongoDB backend settings
------------------------

.. note:: 

    The MongoDB backend requires the :mod:`pymongo` library:
    http://github.com/mongodb/mongo-python-driver/tree/master

.. _CELERY_MONGODB_BACKEND_SETTINGS:

CELERY_MONGODB_BACKEND_SETTINGS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a dict supporting the following keys:

* host
    Hostname of the MongoDB server. Defaults to "localhost".

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

.. _conf-messaging:

Message Routing
---------------

.. _conf-messaging-routing:

.. _CELERY_QUEUES:

CELERY_QUEUES
~~~~~~~~~~~~~

The mapping of queues the worker consumes from.  This is a dictionary
of queue name/options.  See :ref:`guide-routing` for more information.

The default is a queue/exchange/binding key of ``"celery"``, with
exchange type ``direct``.

You don't have to care about this unless you want custom routing facilities.

.. _CELERY_DEFAULT_QUEUE:

CELERY_DEFAULT_QUEUE
~~~~~~~~~~~~~~~~~~~~

The queue used by default, if no custom queue is specified.  This queue must
be listed in :setting:`CELERY_QUEUES`.  The default is: ``celery``.

.. _CELERY_DEFAULT_EXCHANGE:

CELERY_DEFAULT_EXCHANGE
~~~~~~~~~~~~~~~~~~~~~~~

Name of the default exchange to use when no custom exchange is
specified.  The default is: ``celery``.

.. _CELERY_DEFAULT_EXCHANGE_TYPE:

CELERY_DEFAULT_EXCHANGE_TYPE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default exchange type used when no custom exchange is specified.
The default is: ``direct``.

.. _CELERY_DEFAULT_ROUTING_KEY:

CELERY_DEFAULT_ROUTING_KEY
~~~~~~~~~~~~~~~~~~~~~~~~~~

The default routing key used when sending tasks.
The default is: ``celery``.

.. _CELERY_DEFAULT_DELIVERY_MODE:

CELERY_DEFAULT_DELIVERY_MODE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Can be ``transient`` or ``persistent``.  The default is to send
persistent messages.

.. _conf-broker-connection:

Broker Settings
---------------

.. _CELERY_BROKER_CONNECTION_TIMEOUT:

CELERY_BROKER_CONNECTION_TIMEOUT
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default timeout in seconds before we give up establishing a connection
to the AMQP server.  Default is 4 seconds.

.. _CELERY_BROKER_CONNECTION_RETRY:

CELERY_BROKER_CONNECTION_RETRY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Automatically try to re-establish the connection to the AMQP broker if lost.

The time between retries is increased for each retry, and is
not exhausted before :setting:`CELERY_BROKER_CONNECTION_MAX_RETRIES` is
exceeded.

This behavior is on by default.

.. _CELERY_BROKER_CONNECTION_MAX_RETRIES:

CELERY_BROKER_CONNECTION_MAX_RETRIES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum number of retries before we give up re-establishing a connection
to the AMQP broker.

If this is set to :const:`0` or :const:`None`, we will retry forever.

Default is 100 retries.

.. _conf-task-execution:

Task execution settings
-----------------------

.. _CELERY_ALWAYS_EAGER:

CELERY_ALWAYS_EAGER
~~~~~~~~~~~~~~~~~~~

If this is :const:`True`, all tasks will be executed locally by blocking
until it is finished.  ``apply_async`` and ``Task.delay`` will return
a :class:`~celery.result.EagerResult` which emulates the behavior of
:class:`~celery.result.AsyncResult`, except the result has already
been evaluated.

Tasks will never be sent to the queue, but executed locally
instead.

.. _CELERY_EAGER_PROPAGATES_EXCEPTIONS:

CELERY_EAGER_PROPAGATES_EXCEPTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If this is :const:`True`, eagerly executed tasks (using ``.apply``, or with
:setting:`CELERY_ALWAYS_EAGER` on), will raise exceptions.

It's the same as always running ``apply`` with ``throw=True``.

.. _CELERY_IGNORE_RESULT:

CELERY_IGNORE_RESULT
~~~~~~~~~~~~~~~~~~~~

Whether to store the task return values or not (tombstones).
If you still want to store errors, just not successful return values,
you can set :setting:`CELERY_STORE_ERRORS_EVEN_IF_IGNORED`.

.. _CELERY_TASK_RESULT_EXPIRES:

CELERY_TASK_RESULT_EXPIRES
~~~~~~~~~~~~~~~~~~~~~~~~~~

Time (in seconds, or a :class:`~datetime.timedelta` object) for when after
stored task tombstones will be deleted.

A built-in periodic task will delete the results after this time
(:class:`celery.task.builtins.backend_cleanup`).

.. note::

    For the moment this only works with the database, cache, redis and MongoDB
    backends. For the AMQP backend see
    :setting:`CELERY_AMQP_TASK_RESULT_EXPIRES`.

    When using the database or MongoDB backends, ``celerybeat`` must be
    running for the results to be expired.


.. _CELERY_MAX_CACHED_RESULTS:

CELERY_MAX_CACHED_RESULTS
~~~~~~~~~~~~~~~~~~~~~~~~~

Total number of results to store before results are evicted from the
result cache.  The default is 5000.

.. _CELERY_TRACK_STARTED:

CELERY_TRACK_STARTED
~~~~~~~~~~~~~~~~~~~~

If :const:`True` the task will report its status as "started" when the
task is executed by a worker.  The default value is :const:`False` as
the normal behaviour is to not report that level of granularity.  Tasks
are either pending, finished, or waiting to be retried.  Having a "started"
state can be useful for when there are long running tasks and there is a
need to report which task is currently running.

.. _CELERY_TASK_SERIALIZER:

CELERY_TASK_SERIALIZER
~~~~~~~~~~~~~~~~~~~~~~

A string identifying the default serialization method to use.  Can be
``pickle`` (default), ``json``, ``yaml``, or any custom serialization
methods that have been registered with :mod:`carrot.serialization.registry`.

.. _CELERY_DEFAULT_RATE_LIMIT:

CELERY_DEFAULT_RATE_LIMIT
~~~~~~~~~~~~~~~~~~~~~~~~~

The global default rate limit for tasks.

This value is used for tasks that does not have a custom rate limit
The default is no rate limit.

.. _CELERY_DISABLE_RATE_LIMITS:

CELERY_DISABLE_RATE_LIMITS
~~~~~~~~~~~~~~~~~~~~~~~~~~

Disable all rate limits, even if tasks has explicit rate limits set.

.. _CELERY_ACKS_LATE:

CELERY_ACKS_LATE
~~~~~~~~~~~~~~~~

Late ack means the task messages will be acknowledged **after** the task
has been executed, not *just before*, which is the default behavior.

.. seealso::

    FAQ: :ref:`faq-acks_late-vs-retry`.

.. _conf-celeryd:

Worker: celeryd
---------------

.. _CELERY_IMPORTS:

CELERY_IMPORTS
~~~~~~~~~~~~~~

A sequence of modules to import when the celery daemon starts.

This is used to specify the task modules to import, but also
to import signal handlers and additional remote control commands, etc.

.. _CELERYD_MAX_TASKS_PER_CHILD:

CELERYD_MAX_TASKS_PER_CHILD
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum number of tasks a pool worker process can execute before
it's replaced with a new one.  Default is no limit.

.. _CELERYD_TASK_TIME_LIMIT:

CELERYD_TASK_TIME_LIMIT
~~~~~~~~~~~~~~~~~~~~~~~

Task hard time limit in seconds.  The worker processing the task will
be killed and replaced with a new one when this is exceeded.

.. _CELERYD_SOFT_TASK_TIME_LIMIT:

CELERYD_SOFT_TASK_TIME_LIMIT
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Task soft time limit in seconds.

The :exc:`~celery.exceptions.SoftTimeLimitExceeded` exception will be
raised when this is exceeded.  The task can catch this to
e.g. clean up before the hard time limit comes.

Example:

.. code-block:: python

    from celery.decorators import task
    from celery.exceptions import SoftTimeLimitExceeded

    @task()
    def mytask():
        try:
            return do_work()
        except SoftTimeLimitExceeded:
            cleanup_in_a_hurry()

.. _CELERY_STORE_ERRORS_EVEN_IF_IGNORED:

CELERY_STORE_ERRORS_EVEN_IF_IGNORED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If set, the worker stores all task errors in the result store even if
:attr:`Task.ignore_result <celery.task.base.Task.ignore_result>` is on.

.. _conf-error-mails:

Error E-Mails
-------------

.. _CELERYD_SEND_TASK_ERROR_EMAILS:

CELERY_SEND_TASK_ERROR_EMAILS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If set to ``True``, errors in tasks will be sent to admins by e-mail.

.. _ADMINS:

ADMINS
~~~~~~

List of ``(name, email_address)`` tuples for the admins that should
receive error e-mails.

.. _SERVER_EMAIL:

SERVER_EMAIL
~~~~~~~~~~~~

The e-mail address this worker sends e-mails from.
Default is celery@localhost.

.. _MAIL_HOST:

MAIL_HOST
~~~~~~~~~

The mail server to use.  Default is ``"localhost"``.

.. _MAIL_HOST_USER:

MAIL_HOST_USER
~~~~~~~~~~~~~~

Username (if required) to log on to the mail server with.

.. _MAIL_HOST_PASSWORD:

MAIL_HOST_PASSWORD
~~~~~~~~~~~~~~~~~~

Password (if required) to log on to the mail server with.

.. _MAIL_PORT:

MAIL_PORT
~~~~~~~~~

The port the mail server is listening on.  Default is ``25``.

.. _conf-example-error-mail-config:

Example E-Mail configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This configuration enables the sending of error e-mails to
george@vandelay.com and kramer@vandelay.com:

.. code-block:: python

    # Enables error e-mails.
    CELERY_SEND_TASK_ERROR_EMAILS = True

    # Name and e-mail addresses of recipients
    ADMINS = (
        ("George Costanza", "george@vandelay.com"),
        ("Cosmo Kramer", "kosmo@vandelay.com"),
    )

    # E-mail address used as sender (From field).
    SERVER_EMAIL = "no-reply@vandelay.com"

    # Mailserver configuration
    EMAIL_HOST = "mail.vandelay.com"
    EMAIL_PORT = 25
    # EMAIL_HOST_USER = "servers"
    # EMAIL_HOST_PASSWORD = "s3cr3t"

.. _conf-events:

Events
------

.. _CELERY_SEND_EVENTS:

CELERY_SEND_EVENTS
~~~~~~~~~~~~~~~~~~

Send events so the worker can be monitored by tools like ``celerymon``.

.. _CELERY_EVENT_EXCHANGE:

CELERY_EVENT_EXCHANGE
~~~~~~~~~~~~~~~~~~~~~

Name of the exchange to send event messages to.  Default is ``"celeryevent"``.

.. _CELERY_EVENT_EXCHANGE_TYPE:

CELERY_EVENT_EXCHANGE_TYPE
~~~~~~~~~~~~~~~~~~~~~~~~~~

The exchange type of the event exchange.  Default is to use a ``"direct"``
exchange.

.. _CELERY_EVENT_ROUTING_KEY:

CELERY_EVENT_ROUTING_KEY
~~~~~~~~~~~~~~~~~~~~~~~~

Routing key used when sending event messages.  Default is ``"celeryevent"``.

.. _CELERY_EVENT_SERIALIZER:

CELERY_EVENT_SERIALIZER
~~~~~~~~~~~~~~~~~~~~~~~

Message serialization format used when sending event messages.
Default is ``"json"``.

.. _conf-broadcast:

Broadcast Commands
------------------

.. _CELERY_BROADCAST_QUEUE:

CELERY_BROADCAST_QUEUE
~~~~~~~~~~~~~~~~~~~~~~

Name prefix for the queue used when listening for broadcast messages.
The workers hostname will be appended to the prefix to create the final
queue name.

Default is ``"celeryctl"``.

.. _CELERY_BROADCASTS_EXCHANGE:

CELERY_BROADCAST_EXCHANGE
~~~~~~~~~~~~~~~~~~~~~~~~~

Name of the exchange used for broadcast messages.

Default is ``"celeryctl"``.

.. _CELERY_BROADCAST_EXCHANGE_TYPE:

CELERY_BROADCAST_EXCHANGE_TYPE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Exchange type used for broadcast messages.  Default is ``"fanout"``.

.. _conf-logging:

Logging
-------

.. _CELERYD_LOG_FILE:

CELERYD_LOG_FILE
~~~~~~~~~~~~~~~~

The default file name the worker daemon logs messages to.  Can be overridden
using the :option:`--logfile` option to :mod:`~celery.bin.celeryd`.

The default is :const:`None` (``stderr``)

.. _CELERYD_LOG_LEVEL:

CELERYD_LOG_LEVEL
~~~~~~~~~~~~~~~~~

Worker log level, can be any of :const:`DEBUG`, :const:`INFO`, :const:`WARNING`,
:const:`ERROR` or :const:`CRITICAL`.

Can also be set via the :option:`--loglevel` argument to
:mod:`~celery.bin.celeryd`.

See the :mod:`logging` module for more information.

.. _CELERYD_LOG_FORMAT:

CELERYD_LOG_FORMAT
~~~~~~~~~~~~~~~~~~

The format to use for log messages.

Default is ``[%(asctime)s: %(levelname)s/%(processName)s] %(message)s``

See the Python :mod:`logging` module for more information about log
formats.

.. _CELERYD_TASK_LOG_FORMAT:

CELERYD_TASK_LOG_FORMAT
~~~~~~~~~~~~~~~~~~~~~~~

The format to use for log messages logged in tasks.  Can be overridden using
the :option:`--loglevel` option to :mod:`~celery.bin.celeryd`.

Default is::

    [%(asctime)s: %(levelname)s/%(processName)s]
        [%(task_name)s(%(task_id)s)] %(message)s

See the Python :mod:`logging` module for more information about log
formats.

.. _conf-custom-components:

Custom Component Classes (advanced)
-----------------------------------

.. _CELERYD_POOL:

CELERYD_POOL
~~~~~~~~~~~~

Name of the task pool class used by the worker.
Default is :class:`celery.concurrency.processes.TaskPool`.

.. _CELERYD_LISTENER:

CELERYD_LISTENER
~~~~~~~~~~~~~~~~

Name of the listener class used by the worker.
Default is :class:`celery.worker.listener.CarrotListener`.

.. _CELERYD_MEDIATOR:

CELERYD_MEDIATOR
~~~~~~~~~~~~~~~~

Name of the mediator class used by the worker.
Default is :class:`celery.worker.controllers.Mediator`.

.. _CELERYD_ETA_SCHEDULER:

CELERYD_ETA_SCHEDULER
~~~~~~~~~~~~~~~~~~~~~

Name of the ETA scheduler class used by the worker.
Default is :class:`celery.worker.controllers.ScheduleController`.

.. _conf-celerybeat:

Periodic Task Server: celerybeat
--------------------------------

.. _CELERYBEAT_SCHEDULE:

CELERYBEAT_SCHEDULE
~~~~~~~~~~~~~~~~~~~

The periodic task schedule used by :mod:`~celery.bin.celerybeat`.
See :ref:`beat-entries`.

.. _CELERYBEAT_SCHEDULE_FILENAME:

CELERYBEAT_SCHEDULE_FILENAME
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Name of the file used to stores the last run times.
Can be a relative or absolute path, but be aware that the suffix ``.db``
may be appended to the file name (depending on Python version).

Can also be set via the :option:`--schedule` argument to
:mod:`~celery.bin.celerybeat`.

.. _CELERYBEAT_MAX_LOOP_INTERVAL:

CELERYBEAT_MAX_LOOP_INTERVAL
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The maximum number of seconds :mod:`~celery.bin.celerybeat` can sleep
between checking the schedule.  Default is 300 seconds (5 minutes).

.. _CELERYBEAT_LOG_FILE:

CELERYBEAT_LOG_FILE
~~~~~~~~~~~~~~~~~~~

The default file name to log messages to.  Can be overridden using
the `--logfile`` option to :mod:`~celery.bin.celerybeat`.

The default is :const:`None` (``stderr``).

.. _CELERYBEAT_LOG_LEVEL:

CELERYBEAT_LOG_LEVEL
~~~~~~~~~~~~~~~~~~~~

Logging level. Can be any of :const:`DEBUG`, :const:`INFO`, :const:`WARNING`,
:const:`ERROR`, or :const:`CRITICAL`.

Can also be set via the :option:`--loglevel` argument to
:mod:`~celery.bin.celerybeat`.

See the :mod:`logging` module for more information.

.. _conf-celerymon:

Monitor Server: celerymon
-------------------------

.. _CELERYMON_LOG_FILE:

CELERYMON_LOG_FILE
~~~~~~~~~~~~~~~~~~

The default file name to log messages to.  Can be overridden using
the :option:`--logfile` argument to ``celerymon``.

The default is :const:`None` (``stderr``)

.. _CELERYMON_LOG_LEVEL:

CELERYMON_LOG_LEVEL
~~~~~~~~~~~~~~~~~~~

Logging level. Can be any of :const:`DEBUG`, :const:`INFO`, :const:`WARNING`,
:const:`ERROR`, or :const:`CRITICAL`.

See the :mod:`logging` module for more information.
