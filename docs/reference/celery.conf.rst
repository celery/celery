============================
Configuration - celery.conf
============================

.. currentmodule:: celery.conf

.. automodule:: celery.conf
    :members:

.. data:: AMQP_EXCHANGE

    The AMQP exchange.

.. data:: AMQP_ROUTING_KEY
   
    The AMQP routing key.

.. data:: AMQP_CONSUMER_QUEUE
   
    The name of the AMQP queue.

.. data:: DAEMON_CONCURRENCY
   
    The number of worker processes, that should work simultaenously.

.. data:: DAEMON_PID_FILE
   
    Full path to the daemon pid file.

.. data:: EMPTY_MSG_EMIT_EVERY
   
    How often the celery daemon should write a log message saying there are no
    messages in the queue. If this is ``None`` or ``0``, it will never print
    this message.

.. data:: QUEUE_WAKEUP_AFTER
   
    The time (in seconds) the celery daemon should sleep when there are no messages
    left on the queue. After the time is slept, the worker wakes up and
    checks the queue again.

.. data:: DAEMON_LOG_LEVEL
   
    Celery daemon log level, could be any of ``DEBUG``, ``INFO``, ``WARNING``,
    ``ERROR``, ``CRITICAL``, or ``FATAL``.

.. data:: DAEMON_LOG_FILE
   
    The path to the deamon log file (if not set, ``stderr`` is used).

.. data:: LOG_FORMAT
   
    The format to use for log messages.
    Default is ``[%(asctime)s: %(levelname)s/%(processName)s] %(message)s``

.. data:: LOG_LEVELS
   
    Mapping of log level names to ``logging`` module constants.
