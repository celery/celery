from celery.registry import tasks
"""celery.conf"""
from celery.loaders import settings
from datetime import timedelta
import logging

DEFAULT_AMQP_EXCHANGE = "celery"
DEFAULT_AMQP_PUBLISHER_ROUTING_KEY = "celery"
DEFAULT_AMQP_CONSUMER_ROUTING_KEY = "celery"
DEFAULT_AMQP_CONSUMER_QUEUE = "celery"
DEFAULT_AMQP_EXCHANGE_TYPE = "direct"
DEFAULT_DAEMON_CONCURRENCY = 0 # defaults to cpu count
DEFAULT_DAEMON_PID_FILE = "celeryd.pid"
DEFAULT_LOG_FMT = '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'
DEFAULT_DAEMON_LOG_LEVEL = "INFO"
DEFAULT_DAEMON_LOG_FILE = "celeryd.log"
DEFAULT_AMQP_CONNECTION_TIMEOUT = 4
DEFAULT_STATISTICS = False
DEFAULT_ALWAYS_EAGER = False
DEFAULT_TASK_RESULT_EXPIRES = timedelta(days=5)
DEFAULT_AMQP_CONNECTION_RETRY = True
DEFAULT_AMQP_CONNECTION_MAX_RETRIES = 100
DEFAULT_TASK_SERIALIZER = "pickle"
DEFAULT_BACKEND = "database"


"""
.. data:: LOG_LEVELS

    Mapping of log level names to :mod:`logging` module constants.

"""
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.FATAL,
}

"""
.. data:: LOG_FORMAT

    The format to use for log messages.

"""
LOG_FORMAT = getattr(settings, "CELERYD_DAEMON_LOG_FORMAT",
                     DEFAULT_LOG_FMT)

"""
.. data:: DAEMON_LOG_FILE

    Filename of the daemon log file.

"""
DAEMON_LOG_FILE = getattr(settings, "CELERYD_LOG_FILE",
                          DEFAULT_DAEMON_LOG_FILE)

"""
.. data:: DAEMON_LOG_LEVEL


"""
DAEMON_LOG_LEVEL = LOG_LEVELS[getattr(settings, "CELERYD_DAEMON_LOG_LEVEL",
                                      DEFAULT_DAEMON_LOG_LEVEL).upper()]

"""
.. data:: DAEMON_PID_FILE

    Full path to the daemon pidfile.

"""
DAEMON_PID_FILE = getattr(settings, "CELERYD_PID_FILE",
                          DEFAULT_DAEMON_PID_FILE)

"""
.. data:: DAEMON_CONCURRENCY

    The number of concurrent worker processes.

"""
DAEMON_CONCURRENCY = getattr(settings, "CELERYD_CONCURRENCY",
                             DEFAULT_DAEMON_CONCURRENCY)

"""
.. data:: AMQP_EXCHANGE

    Name of the AMQP exchange.

"""
AMQP_EXCHANGE = getattr(settings, "CELERY_AMQP_EXCHANGE",
                        DEFAULT_AMQP_EXCHANGE)


"""
.. data:: AMQP_EXCHANGE_TYPE

The exchange type.

"""
AMQP_EXCHANGE_TYPE = getattr(settings, "CELERY_AMQP_EXCHANGE_TYPE",
                        DEFAULT_AMQP_EXCHANGE_TYPE)

"""
.. data:: AMQP_PUBLISHER_ROUTING_KEY

    The default AMQP routing key used when publishing tasks.

"""
AMQP_PUBLISHER_ROUTING_KEY = getattr(settings,
                                "CELERY_AMQP_PUBLISHER_ROUTING_KEY",
                                DEFAULT_AMQP_PUBLISHER_ROUTING_KEY)

"""
.. data:: AMQP_CONSUMER_ROUTING_KEY

    The AMQP routing key used when consuming tasks.

"""
AMQP_CONSUMER_ROUTING_KEY = getattr(settings,
                                "CELERY_AMQP_CONSUMER_ROUTING_KEY",
                                DEFAULT_AMQP_CONSUMER_ROUTING_KEY)

"""
.. data:: AMQP_CONSUMER_QUEUE

    The name of the AMQP queue.

"""
AMQP_CONSUMER_QUEUE = getattr(settings, "CELERY_AMQP_CONSUMER_QUEUE",
                              DEFAULT_AMQP_CONSUMER_QUEUE)


"""
.. data:: AMQP_CONSUMER_QUEUES

    Dictionary defining multiple AMQP queues.

"""
DEFAULT_AMQP_CONSUMER_QUEUES = {
        AMQP_CONSUMER_QUEUE: {
            "exchange": AMQP_EXCHANGE,
            "routing_key": AMQP_CONSUMER_ROUTING_KEY,
            "exchange_type": AMQP_EXCHANGE_TYPE,
        }
}

AMQP_CONSUMER_QUEUES = getattr(settings, "CELERY_AMQP_CONSUMER_QUEUES",
                              DEFAULT_AMQP_CONSUMER_QUEUES)

"""
.. data:: AMQP_CONNECTION_TIMEOUT

    The timeout in seconds before we give up establishing a connection
    to the AMQP server.

"""
AMQP_CONNECTION_TIMEOUT = getattr(settings, "CELERY_AMQP_CONNECTION_TIMEOUT",
                                  DEFAULT_AMQP_CONNECTION_TIMEOUT)

"""
.. data:: SEND_CELERY_TASK_ERROR_EMAILS

    If set to ``True``, errors in tasks will be sent to admins by e-mail.
    If unset, it will send the e-mails if ``settings.DEBUG`` is False.

"""
SEND_CELERY_TASK_ERROR_EMAILS = getattr(settings,
                                        "SEND_CELERY_TASK_ERROR_EMAILS",
                                        not settings.DEBUG)

"""
.. data:: ALWAYS_EAGER

    Always execute tasks locally, don't send to the queue.

"""
ALWAYS_EAGER = getattr(settings, "CELERY_ALWAYS_EAGER",
                       DEFAULT_ALWAYS_EAGER)

"""
.. data: TASK_RESULT_EXPIRES

    Task tombstone expire time in seconds.

"""
TASK_RESULT_EXPIRES = getattr(settings, "CELERY_TASK_RESULT_EXPIRES",
                              DEFAULT_TASK_RESULT_EXPIRES)

# Make sure TASK_RESULT_EXPIRES is a timedelta.
if isinstance(TASK_RESULT_EXPIRES, int):
    TASK_RESULT_EXPIRES = timedelta(seconds=TASK_RESULT_EXPIRES)

"""
.. data:: AMQP_CONNECTION_RETRY

Automatically try to re-establish the connection to the AMQP broker if
it's lost.

"""
AMQP_CONNECTION_RETRY = getattr(settings, "CELERY_AMQP_CONNECTION_RETRY",
                                DEFAULT_AMQP_CONNECTION_RETRY)

"""
.. data:: AMQP_CONNECTION_MAX_RETRIES

Maximum number of retries before we give up re-establishing a connection
to the AMQP broker.

If this is set to ``0`` or ``None``, we will retry forever.

Default is ``100`` retries.

"""
AMQP_CONNECTION_MAX_RETRIES = getattr(settings,
                                      "CELERY_AMQP_CONNECTION_MAX_RETRIES",
                                      DEFAULT_AMQP_CONNECTION_MAX_RETRIES)

"""
.. data:: TASK_SERIALIZER

A string identifying the default serialization
method to use. Can be ``pickle`` (default),
``json``, ``yaml``, or any custom serialization methods that have
been registered with :mod:`carrot.serialization.registry`.

Default is ``pickle``.

"""
TASK_SERIALIZER = getattr(settings, "CELERY_TASK_SERIALIZER",
                          DEFAULT_TASK_SERIALIZER)


"""

.. data:: CELERY_BACKEND

The backend used to store task results (tombstones).

"""
CELERY_BACKEND = getattr(settings, "CELERY_BACKEND", DEFAULT_BACKEND)


"""

"""

.. data:: CELERY_CACHE_BACKEND

Use a custom cache backend for celery. If not set the django-global
cache backend in ``CACHE_BACKEND`` will be used.

"""
CELERY_CACHE_BACKEND = getattr(settings, "CELERY_CACHE_BACKEND", None)
