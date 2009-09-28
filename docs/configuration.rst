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

    AMQP_HOST = "localhost"
    AMQP_PORT = 5672
    AMQP_VHOST = "/"
    AMQP_USER = "guest"
    AMQP_PASSWORD = "guest"

    ## If you're doing mostly I/O you can have higher concurrency,
    ## if mostly spending time in the CPU, try to keep it close to the
    ## number of CPUs on your machine.
    # CELERYD_CONCURRENCY = 8

    CELERYD_LOG_FILE = "celeryd.log"
    CELERYD_PID_FILE = "celeryd.pid"
    CELERYD_DAEMON_LOG_LEVEL = "INFO"

Concurrency settings
====================

* CELERYD_CONCURRENCY
    The number of concurrent worker processes, executing tasks simultaneously.

    Defaults to the number of CPUs in the system.


Task result backend settings
============================

* CELERY_BACKEND
    The backend used to store task results (tombstones).
    Can be one of the following:

    * database (default)
        Use a relational database supported by the Django ORM.

    * cache
        Use memcached to store the results.

    * mongodb
        Use MongoDB to store the results.

    * tyrant
        Use Tokyo Tyrant to store the results.


* CELERY_PERIODIC_STATUS_BACKEND
    The backend used to store the status of periodic tasks.
    Can be one of the following:

    * database (default)
        Use a relational database supported by the Django ORM.

    * mongodb
        Use MongoDB.


Database backend settings
=========================

This applies to both the result store backend and the periodic status
backend.

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

    DATABASE_ENGINE="mysql"
    DATABASE_USER="myusername"
    DATABASE_PASSWORD="mypassword"
    DATABASE_NAME="mydatabase"
    DATABASE_HOST="localhost"

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

    CACHE_BACKEND = 'memcached://172.19.26.240:11211;172.19.26.242:11211/'


Tokyo Tyrant backend settings
=============================

**NOTE** The Tokyo Tyrant backend requires the :mod:`pytyrant` library:
    http://pypi.python.org/pypi/pytyrant/

This backend requires the following configuration variables to be set:

* TT_HOST
    Hostname of the Tokyo Tyrant server.

* TT_PORT
    The port the Tokyo Tyrant server is listening to.


Example configuration
---------------------

.. code-block:: python

    TT_HOST = "localhost"
    TT_PORT = 1978


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
        Username to authenticate to the MongoDB server as (optional).

    * password
        Password to authenticate to the MongoDB server (optional).

    * database
        The database name to connect to. Defaults to "celery".

    * taskmeta_collection
        The collection name to store task metadata.
        Defaults to "celery_taskmeta".

    * periodictaskmeta_collection
        The collection name to store periodic task metadata.
        Defaults to "celery_periodictaskmeta".


Example configuration
---------------------

.. code-block:: python

    CELERY_MONGODB_BACKEND_SETTINGS = {
        "host": "192.168.1.100",
        "port": 30000,
        "database": "mydb",
        "taskmeta_collection": "my_taskmeta_collection",
    }


Broker settings
===============

* CELERY_AMQP_EXCHANGE

    Name of the AMQP exchange.

* CELERY_AMQP_EXCHANGE_TYPE
    The type of exchange. If the exchange type is ``direct``, all messages
    receives all tasks. However, if the exchange type is ``topic``, you can
    route e.g. some tasks to one server, and others to the rest.
    See `Exchange types and the effect of bindings`_.

    .. _`Exchange types and the effect of bindings`:
        http://bit.ly/wpamqpexchanges

* CELERY_AMQP_PUBLISHER_ROUTING_KEY
    The default AMQP routing key used when publishing tasks.

* CELERY_AMQP_CONSUMER_ROUTING_KEY
    The AMQP routing key used when consuming tasks.

* CELERY_AMQP_CONSUMER_QUEUE
    The name of the AMQP queue.

* CELERY_AMQP_CONSUMER_QUEUES
    Dictionary defining multiple AMQP queues.

* CELERY_AMQP_CONNECTION_TIMEOUT
    The timeout in seconds before we give up establishing a connection
    to the AMQP server. Default is 4 seconds.

* CELERY_AMQP_CONNECTION_RETRY
    Automatically try to re-establish the connection to the AMQP broker if
    it's lost.

    The time between retries is increased for each retry, and is
    not exhausted before ``CELERY_AMQP_CONNECTION_MAX_RETRIES`` is exceeded.

    This behaviour is on by default.

* CELERY_AMQP_CONNECTION_MAX_RETRIES
    Maximum number of retries before we give up re-establishing a connection
    to the AMQP broker.

    If this is set to ``0`` or ``None``, we will retry forever.

    Default is 100 retries.

Task execution settings
=======================

* SEND_CELERY_TASK_ERROR_EMAILS
    If set to ``True``, errors in tasks will be sent to admins by e-mail.
    If unset, it will send the e-mails if ``settings.DEBUG`` is False.

* CELERY_ALWAYS_EAGER
    If this is ``True``, all tasks will be executed locally by blocking
    until it is finished. ``apply_async`` and ``delay_task`` will return
    a :class:`celery.result.EagerResult` which emulates the behaviour of
    an :class:`celery.result.AsyncResult`.

    Tasks will never be sent to the queue, but executed locally
    instead.

* CELERY_TASK_RESULT_EXPIRES
    Time (in seconds, or a :class:`datetime.timedelta` object) for when after
    stored task tombstones are deleted.

    **NOTE**: For the moment this only works for the database and MongoDB
    backends.

* CELERY_TASK_SERIALIZER
    A string identifying the default serialization
    method to use. Can be ``pickle`` (default),
    ``json``, ``yaml``, or any custom serialization methods that have
    been registered with :mod:`carrot.serialization.registry`.

    Default is ``pickle``.

Logging settings
================

* CELERYD_LOG_FILE
    The default filename the worker daemon logs messages to, can be
    overridden using the `--logfile`` option to ``celeryd``.

    The default is to log using ``stderr`` if running in the foreground,
    when running in the background, detached as a daemon, the default
    logfile is ``celeryd.log``.

* CELERYD_DAEMON_LOG_LEVEL
    Worker log level, can be any of ``DEBUG``, ``INFO``, ``WARNING``,
    ``ERROR``, ``CRITICAL``, or ``FATAL``.

    See the :mod:`logging` module for more information.

* CELERYD_DAEMON_LOG_FORMAT
    The format to use for log messages. Can be overridden using
    the ``--loglevel`` option to ``celeryd``.

    Default is ``[%(asctime)s: %(levelname)s/%(processName)s] %(message)s``

    See the Python :mod:`logging` module for more information about log
    formats.

Process settings
================

* CELERYD_PID_FILE
    Full path to the daemon pid file. Default is ``celeryd.pid``.
    Can be overridden using the ``--pidfile`` option to ``celeryd``.

