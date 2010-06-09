============================
Configuration - celery.conf
============================

.. contents::
    :local:
.. currentmodule:: celery.conf

.. data:: QUEUES

    Queue name/options mapping.

.. data:: DEFAULT_QUEUE

    Name of the default queue.

.. data:: DEFAULT_EXCHANGE

    Default exchange.

.. data:: DEFAULT_EXCHANGE_TYPE

    Default exchange type.

.. data:: DEFAULT_DELIVERY_MODE

    Default delivery mode (``"persistent"`` or ``"non-persistent"``).
    Default is ``"persistent"``.

.. data:: DEFAULT_ROUTING_KEY

    Default routing key used when sending tasks.

.. data:: BROKER_CONNECTION_TIMEOUT

    The timeout in seconds before we give up establishing a connection
    to the AMQP server.

.. data:: BROADCAST_QUEUE

    Name prefix for the queue used when listening for
    broadcast messages. The workers hostname will be appended
    to the prefix to create the final queue name.

    Default is ``"celeryctl"``.

.. data:: BROADCAST_EXCHANGE

    Name of the exchange used for broadcast messages.

    Default is ``"celeryctl"``.

.. data:: BROADCAST_EXCHANGE_TYPE

    Exchange type used for broadcast messages. Default is ``"fanout"``.

.. data:: EVENT_QUEUE

    Name of queue used to listen for event messages. Default is
    ``"celeryevent"``.

.. data:: EVENT_EXCHANGE

    Exchange used to send event messages. Default is ``"celeryevent"``.

.. data:: EVENT_EXCHANGE_TYPE

    Exchange type used for the event exchange. Default is ``"topic"``.

.. data:: EVENT_ROUTING_KEY

    Routing key used for events. Default is ``"celeryevent"``.

.. data:: EVENT_SERIALIZER

    Type of serialization method used to serialize events. Default is
    ``"json"``.

.. data:: RESULT_EXCHANGE

    Exchange used by the AMQP result backend to publish task results.
    Default is ``"celeryresult"``.

.. data:: CELERY_SEND_TASK_ERROR_EMAILS

    If set to ``True``, errors in tasks will be sent to admins by e-mail.
    If unset, it will send the e-mails if ``settings.DEBUG`` is ``True``.

.. data:: ALWAYS_EAGER

    Always execute tasks locally, don't send to the queue.

.. data:: EAGER_PROPAGATES_EXCEPTIONS

    If set to ``True``, :func:`celery.execute.apply` will re-raise task exceptions.
    It's the same as always running apply with ``throw=True``.

.. data:: TASK_RESULT_EXPIRES

    Task tombstone expire time in seconds.

.. data:: IGNORE_RESULT

    If enabled, the default behavior will be to not store task results.

.. data:: TRACK_STARTED

    If enabled, the default behavior will be to track when tasks starts by
    storing the :const:`STARTED` state.

.. data:: ACKS_LATE

    If enabled, the default behavior will be to acknowledge task messages
    after the task is executed.

.. data:: STORE_ERRORS_EVEN_IF_IGNORED

    If enabled, task errors will be stored even though ``Task.ignore_result``
    is enabled.

.. data:: MAX_CACHED_RESULTS

    Total number of results to store before results are evicted from the
    result cache.

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

    Celery cache backend.

.. data:: SEND_EVENTS

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

.. data:: CELERYD_LOG_FORMAT

    The format to use for log messages.

.. data:: CELERYD_TASK_LOG_FORMAT

    The format to use for task log messages.

.. data:: CELERYD_LOG_FILE

    Filename of the daemon log file.
    Default is: ``None`` (stderr)

.. data:: CELERYD_LOG_LEVEL

    Default log level for daemons. (``WARN``)

.. data:: CELERYD_CONCURRENCY

    The number of concurrent worker processes.
    If set to ``0``, the total number of available CPUs/cores will be used.

.. data:: CELERYD_PREFETCH_MULTIPLIER

    The number of concurrent workers is multipled by this number to yield
    the wanted AMQP QoS message prefetch count.

.. data:: CELERYD_POOL

    Name of the task pool class used by the worker.
    Default is ``"celery.concurrency.processes.TaskPool"``.

.. data:: CELERYD_LISTENER

    Name of the listener class used by the worker.
    Default is ``"celery.worker.listener.CarrotListener"``.

.. data:: CELERYD_MEDIATOR

    Name of the mediator class used by the worker.
    Default is ``"celery.worker.controllers.Mediator"``.

.. data:: CELERYD_ETA_SCHEDULER

    Name of the ETA scheduler class used by the worker.
    Default is ``"celery.worker.controllers.ScheduleController"``.
