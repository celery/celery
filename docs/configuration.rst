============================
 Configuration and defaults
============================

This document describes the configuration options available.

If you're using celery in a Django project these settings should be defined
in your projects ``settings.py`` file.

In a regular Python environment using the default loader you must create
the ``celeryconfig.py`` module and make sure it is available on the
Python path.


Example configuration file
==========================

This is an example configuration file to get you started,
it should contain all you need to run a basic celery set-up.

.. code-block:: python

    CELERY_BACKEND = "database"
    DATABASE_ENGINE = "sqlite3"
    DATABASE_NAME = "mydatabase.db"

    BROKER_HOST = "localhost"
    BROKER_PORT = 5672
    BROKER_VHOST = "/"
    BROKER_USER = "guest"
    BROKER_PASSWORD = "guest"

    ## If you're doing mostly I/O you can have more processes,
    ## but if mostly spending CPU, try to keep it close to the
    ## number of CPUs on your machine. If not set, the number of CPUs/cores
    ## available will be used.
    # CELERYD_CONCURRENCY = 8

    # CELERYD_LOG_FILE = "celeryd.log"
    # CELERYD_LOG_LEVEL = "INFO"

Concurrency settings
====================

* CELERYD_CONCURRENCY
    The number of concurrent worker processes, executing tasks simultaneously.

    Defaults to the number of CPUs/cores available.


* CELERYD_PREFETCH_MULTIPLIER
    How many messages to prefetch at a time multiplied by the number of
    concurrent processes. The default is 4 (four messages for each
    process). The default setting seems pretty good here, but if you have
    very long running tasks waiting in the queue and you have to start the
    workers, make note that the first worker to start will receive four times the
    number of messages initially, which might not be fairly balanced among the
    workers.


Task result backend settings
============================

* CELERY_BACKEND
    The backend used to store task results (tombstones).
    Can be one of the following:

    * database (default)
        Use a relational database supported by the Django ORM.

    * cache
        Use `memcached`_ to store the results.

    * mongodb
        Use `MongoDB`_ to store the results.

    * pyredis
        Use `Redis`_ to store the results.

    * tyrant
        Use `Tokyo Tyrant`_ to store the results.

    * amqp
        Send results back as AMQP messages
        (**WARNING** While very fast, you must make sure you only
        try to receive the result once).


.. _`memcached`: http://memcached.org
.. _`MongoDB`: http://mongodb.org
.. _`Redis`: http://code.google.com/p/redis/
.. _`Tokyo Tyrant`: http://1978th.net/tokyotyrant/

Database backend settings
=========================

Please see the Django ORM database settings documentation:
http://docs.djangoproject.com/en/dev/ref/settings/#database-engine

If you use this backend make sure to initialize the database tables
after configuration. When using celery with a Django project this
means executing::

    $ python manage.py syncdb

When using celery in a regular Python environment you have to execute::

    $ celeryinit

Example configuration
---------------------

.. code-block:: python

    CELERY_BACKEND = "database"
    DATABASE_ENGINE = "mysql"
    DATABASE_USER = "myusername"
    DATABASE_PASSWORD = "mypassword"
    DATABASE_NAME = "mydatabase"
    DATABASE_HOST = "localhost"

AMQP backend settings
=====================

The AMQP backend does not have any settings yet.

Example configuration
---------------------

    CELERY_BACKEND = "amqp"

Cache backend settings
======================

Please see the documentation for the Django cache framework settings:
http://docs.djangoproject.com/en/dev/topics/cache/#memcached

To use a custom cache backend for Celery, while using another for Django,
you should use the ``CELERY_CACHE_BACKEND`` setting instead of the regular
django ``CACHE_BACKEND`` setting.

Example configuration
---------------------

Using a single memcached server:

.. code-block:: python

    CACHE_BACKEND = 'memcached://127.0.0.1:11211/'

Using multiple memcached servers:

.. code-block:: python

    CELERY_BACKEND = "cache"
    CACHE_BACKEND = 'memcached://172.19.26.240:11211;172.19.26.242:11211/'


Tokyo Tyrant backend settings
=============================

**NOTE** The Tokyo Tyrant backend requires the :mod:`pytyrant` library:
    http://pypi.python.org/pypi/pytyrant/

This backend requires the following configuration directives to be set:

* TT_HOST
    Hostname of the Tokyo Tyrant server.

* TT_PORT
    The port the Tokyo Tyrant server is listening to.


Example configuration
---------------------

.. code-block:: python

    CELERY_BACKEND = "tyrant"
    TT_HOST = "localhost"
    TT_PORT = 1978

Redis backend settings
======================

**NOTE** The Redis backend requires the :mod:`redis` library:
    http://pypi.python.org/pypi/redis/0.5.5

To install the redis package use ``pip`` or ``easy_install``::

    $ pip install redis

This backend requires the following configuration directives to be set:

* REDIS_HOST

    Hostname of the Redis database server. e.g. ``"localhost"``.

* REDIS_PORT

    Port to the Redis database server. e.g. ``6379``.

Also, the following optional configuration directives are available:

* REDIS_DB

    Name of the database to use. Default is ``celery_results``.

* REDIS_TIMEOUT

    Timeout in seconds before we give up establishing a connection
    to the Redis server.

* REDIS_CONNECT_RETRY

    Retry connecting if an connection could not be established. Default is
    false.


Example configuration
---------------------

.. code-block:: python

    CELERY_BACKEND = "pyredis"
    REDIS_HOST = "localhost"
    REDIS_PORT = 6739
    REDIS_DATABASE = "celery_results"
    REDIS_CONNECT_RETRY=True

MongoDB backend settings
========================

**NOTE** The MongoDB backend requires the :mod:`pymongo` library:
    http://github.com/mongodb/mongo-python-driver/tree/master

* CELERY_MONGODB_BACKEND_SETTINGS

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


Example configuration
---------------------

.. code-block:: python

    CELERY_BACKEND = "mongodb"
    CELERY_MONGODB_BACKEND_SETTINGS = {
        "host": "192.168.1.100",
        "port": 30000,
        "database": "mydb",
        "taskmeta_collection": "my_taskmeta_collection",
    }


Messaging settings
==================

Routing
-------

* CELERY_QUEUES
  The mapping of queues the worker consumes from. This is a dictionary
  of queue name/options. See :doc:`userguide/routing` for more information.

  The default is a queue/exchange/binding key of ``"celery"``, with
  exchange type ``direct``.

  You don't have to care about this unless you want custom routing facilities.

* CELERY_DEFAULT_QUEUE
    The queue used by default, if no custom queue is specified.
    This queue must be listed in ``CELERY_QUEUES``.
    The default is: ``celery``.

* CELERY_DEFAULT_EXCHANGE
    Name of the default exchange to use when no custom exchange
    is specified.
    The default is: ``celery``.

* CELERY_DEFAULT_EXCHANGE_TYPE
    Default exchange type used when no custom exchange is specified.
    The default is: ``direct``.

* CELERY_DEFAULT_ROUTING_KEY
    The default routing key used when sending tasks.
    The default is: ``celery``.

Connection
----------

* CELERY_BROKER_CONNECTION_TIMEOUT
    The timeout in seconds before we give up establishing a connection
    to the AMQP server. Default is 4 seconds.

* CELERY_BROKER_CONNECTION_RETRY
    Automatically try to re-establish the connection to the AMQP broker if
    it's lost.

    The time between retries is increased for each retry, and is
    not exhausted before ``CELERY_BROKER_CONNECTION_MAX_RETRIES`` is exceeded.

    This behavior is on by default.

* CELERY_BROKER_CONNECTION_MAX_RETRIES
    Maximum number of retries before we give up re-establishing a connection
    to the AMQP broker.

    If this is set to ``0`` or ``None``, we will retry forever.

    Default is 100 retries.

Task execution settings
=======================

* CELERY_ALWAYS_EAGER
    If this is ``True``, all tasks will be executed locally by blocking
    until it is finished. ``apply_async`` and ``Task.delay`` will return
    a :class:`celery.result.EagerResult` which emulates the behavior of
    :class:`celery.result.AsyncResult`, except the result has already
    been evaluated.

    Tasks will never be sent to the queue, but executed locally
    instead.

* CELERY_IGNORE_RESULT

    Whether to store the task return values or not (tombstones).
    If you still want to store errors, just not successful return values,
    you can set ``CELERY_STORE_ERRORS_EVEN_IF_IGNORED``.

* CELERY_TASK_RESULT_EXPIRES
    Time (in seconds, or a :class:`datetime.timedelta` object) for when after
    stored task tombstones are deleted.

    **NOTE**: For the moment this only works for the database and MongoDB
    backends., except the result has already
    been evaluated.

* CELERY_TASK_SERIALIZER
    A string identifying the default serialization
    method to use. Can be ``pickle`` (default),
    ``json``, ``yaml``, or any custom serialization methods that have
    been registered with :mod:`carrot.serialization.registry`.

    Default is ``pickle``.

Worker: celeryd
===============

* CELERY_IMPORTS
    A sequence of modules to import when the celery daemon starts.  This is
    useful to add tasks if you are not using django or cannot use task
    auto-discovery.

* CELERY_SEND_EVENTS
    Send events so the worker can be monitored by tools like ``celerymon``.

* CELERY_SEND_TASK_ERROR_EMAILS
    If set to ``True``, errors in tasks will be sent to admins by e-mail.
    If unset, it will send the e-mails if ``settings.DEBUG`` is False.

* CELERY_STORE_ERRORS_EVEN_IF_IGNORED
    If set, the worker stores all task errors in the result store even if
    ``Task.ignore_result`` is on.

Logging
-------

* CELERYD_LOG_FILE
    The default file name the worker daemon logs messages to, can be
    overridden using the `--logfile`` option to ``celeryd``.

    The default is ``None`` (``stderr``)
    Can also be set via the ``--logfile`` argument.

* CELERYD_LOG_LEVEL
    Worker log level, can be any of ``DEBUG``, ``INFO``, ``WARNING``,
    ``ERROR``, ``CRITICAL``.

    Can also be set via the ``--loglevel`` argument.

    See the :mod:`logging` module for more information.

* CELERYD_LOG_FORMAT
    The format to use for log messages. Can be overridden using
    the ``--loglevel`` option to ``celeryd``.

    Default is ``[%(asctime)s: %(levelname)s/%(processName)s] %(message)s``

    See the Python :mod:`logging` module for more information about log
    formats.

Periodic Task Server: celerybeat
================================

* CELERYBEAT_SCHEDULE_FILENAME

    Name of the file celerybeat stores the current schedule in.
    Can be a relative or absolute path, but be aware that the suffix ``.db``
    will be appended to the file name.

    Can also be set via the ``--schedule`` argument.

* CELERYBEAT_MAX_LOOP_INTERVAL

    The maximum number of seconds celerybeat can sleep between checking
    the schedule. Default is 300 seconds (5 minutes).

* CELERYBEAT_LOG_FILE
    The default file name to log messages to, can be
    overridden using the `--logfile`` option.

    The default is ``None`` (``stderr``).
    Can also be set via the ``--logfile`` argument.

* CELERYBEAT_LOG_LEVEL
    Logging level. Can be any of ``DEBUG``, ``INFO``, ``WARNING``,
    ``ERROR``, or ``CRITICAL``.

    Can also be set via the ``--loglevel`` argument.

    See the :mod:`logging` module for more information.

Monitor Server: celerymon
=========================

* CELERYMON_LOG_FILE
    The default file name to log messages to, can be
    overridden using the `--logfile`` option.

    The default is ``None`` (``stderr``)
    Can also be set via the ``--logfile`` argument.

* CELERYMON_LOG_LEVEL
    Logging level. Can be any of ``DEBUG``, ``INFO``, ``WARNING``,
    ``ERROR``, or ``CRITICAL``.

    See the :mod:`logging` module for more information.
