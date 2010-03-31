============================
Configuration - celery.conf
============================

.. data:: QUEUES

    Queue name/options mapping.

.. data:: DEFAULT_QUEUE

    Name of the default queue.

.. data:: DEFAULT_EXCHANGE

    Default exchange.

.. data:: DEFAULT_EXCHANGE_TYPE

    Default exchange type.

.. data:: DEFAULT_ROUTING_KEY

    Default routing key used when sending tasks.

.. data:: BROKER_CONNECTION_TIMEOUT

    The timeout in seconds before we give up establishing a connection
    to the AMQP server.

.. data:: CELERY_SEND_TASK_ERROR_EMAILS

    If set to ``True``, errors in tasks will be sent to admins by e-mail.
    If unset, it will send the e-mails if ``settings.DEBUG`` is False.

.. data:: ALWAYS_EAGER

    Always execute tasks locally, don't send to the queue.

.. data:: TASK_RESULT_EXPIRES

    Task tombstone expire time in seconds.

.. data:: BROKER_CONNECTION_RETRY

    Automatically try to re-establish the connection to the AMQP broker if
    it's lost.

.. data:: BROKER_CONNECTION_MAX_RETRIES

    Maximum number of retries before we give up re-establishing a connection
    to the broker.

    If this is set to ``0`` or ``None``, we will retry forever.

    Default is ``100`` retries.

.. data:: TASK_SERIALIZER

    A string identifying the default serialization
    method to use. Can be ``pickle`` (default),
    ``json``, ``yaml``, or any custom serialization methods that have
    been registered with :mod:`carrot.serialization.registry`.

    Default is ``pickle``.

.. data:: RESULT_BACKEND

    The backend used to store task results (tombstones).

.. data:: CELERY_CACHE_BACKEND

    Use a custom cache backend for celery. If not set the django-global
    cache backend in ``CACHE_BACKEND`` will be used.

.. data:: CELERY_SEND_EVENTS

    If set, celery will send events that can be captured by monitors like
    ``celerymon``.
    Default is: ``False``.

.. data:: DEFAULT_RATE_LIMIT

    The default rate limit applied to all tasks which doesn't have a custom
    rate limit defined. (Default: None)

.. data:: DISABLE_RATE_LIMITS

    If ``True`` all rate limits will be disabled and all tasks will be executed
    as soon as possible.

.. data:: CELERYBEAT_LOG_LEVEL

    Default log level for celerybeat.
    Default is: ``INFO``.

.. data:: CELERYBEAT_LOG_FILE

    Default log file for celerybeat.
    Default is: ``None`` (stderr)

.. data:: CELERYBEAT_SCHEDULE_FILENAME

    Name of the persistent schedule database file.
    Default is: ``celerybeat-schedule``.

.. data:: CELERYBEAT_MAX_LOOP_INTERVAL

    The maximum number of seconds celerybeat is allowed to sleep between
    checking the schedule. The default is 5 minutes, which means celerybeat can
    only sleep a maximum of 5 minutes after checking the schedule run-times for a
    periodic task to apply. If you change the run_times of periodic tasks at
    run-time, you may consider lowering this value for changes to take effect
    faster (A value of 5 minutes, means the changes will take effect in 5 minutes
    at maximum).

.. data:: CELERYMON_LOG_LEVEL

    Default log level for celerymon.
    Default is: ``INFO``.

.. data:: CELERYMON_LOG_FILE

    Default log file for celerymon.
    Default is: ``None`` (stderr)

.. data:: LOG_LEVELS

    Mapping of log level names to :mod:`logging` module constants.

.. data:: LOG_FORMAT

    The format to use for log messages.

.. data:: CELERYD_LOG_FILE

    Filename of the daemon log file.
    Default is: ``None`` (stderr)

.. data:: CELERYD_LOG_LEVEL

    Default log level for daemons. (``WARN``)

.. data:: CELERYD_CONCURRENCY

    The number of concurrent worker processes.
    If set to ``0``, the total number of available CPUs/cores will be used.
