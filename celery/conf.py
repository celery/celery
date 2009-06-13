"""celery.conf"""
from django.conf import settings
import logging

DEFAULT_AMQP_EXCHANGE = "celery"
DEFAULT_AMQP_PUBLISHER_ROUTING_KEY = "celery"
DEFAULT_AMQP_CONSUMER_ROUTING_KEY = "celery"
DEFAULT_AMQP_CONSUMER_QUEUE = "celery"
DEFAULT_AMQP_EXCHANGE_TYPE = "direct"
DEFAULT_DAEMON_CONCURRENCY = 10
DEFAULT_DAEMON_PID_FILE = "celeryd.pid"
DEFAULT_LOG_FMT = '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'
DEFAULT_DAEMON_LOG_LEVEL = "INFO"
DEFAULT_DAEMON_LOG_FILE = "celeryd.log"

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
    Default is ``[%(asctime)s: %(levelname)s/%(processName)s] %(message)s``

"""
LOG_FORMAT = getattr(settings, "CELERYD_DAEMON_LOG_FORMAT",
                     DEFAULT_LOG_FMT)

"""
.. data:: DAEMON_LOG_FILE

    The path to the deamon log file (if not set, ``stderr`` is used).

"""
DAEMON_LOG_FILE = getattr(settings, "CELERYD_LOG_FILE",
                          DEFAULT_DAEMON_LOG_FILE)

"""
.. data:: DAEMON_LOG_LEVEL

    Celery daemon log level, can be any of ``DEBUG``, ``INFO``, ``WARNING``,
    ``ERROR``, ``CRITICAL``, or ``FATAL``. See the :mod:`logging` module
    for more information.

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

    The number of concurrent worker processes, executing tasks simultaneously.

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

The type of exchange. If the exchange type is ``direct``, all messages
receives all tasks. However, if the exchange type is ``topic``, you can
route e.g. some tasks to one server, and others to the rest.
See `Exchange types and the effect of bindings`_.

.. _`Exchange types and the effect of bindings`: http://bit.ly/wpamqpexchanges

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
.. data:: SEND_CELERY_TASK_ERROR_EMAILS

    If set to ``True``, errors in tasks will be sent to admins by e-mail.
    If unset, it will send the e-mails if DEBUG is False.

"""
SEND_CELERY_TASK_ERROR_EMAILS = getattr(settings,
                                        "SEND_CELERY_TASK_ERROR_EMAILS",
                                        not settings.DEBUG)
