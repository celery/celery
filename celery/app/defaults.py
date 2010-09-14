from datetime import timedelta

DEFAULT_PROCESS_LOG_FMT = """
    [%(asctime)s: %(levelname)s/%(processName)s] %(message)s
""".strip()
DEFAULT_LOG_FMT = '[%(asctime)s: %(levelname)s] %(message)s'
DEFAULT_TASK_LOG_FMT = " ".join("""
    [%(asctime)s: %(levelname)s/%(processName)s]
    [%(task_name)s(%(task_id)s)] %(message)s
""".strip().split())


DEFAULTS = {
    "BROKER_CONNECTION_TIMEOUT": 4,
    "BROKER_CONNECTION_RETRY": True,
    "BROKER_CONNECTION_MAX_RETRIES": 100,
    "BROKER_HOST": "localhost",
    "BROKER_PORT": None,
    "BROKER_USER": "guest",
    "BROKER_PASSWORD": "guest",
    "BROKER_INSIST": False,
    "BROKER_USE_SSL": False,
    "BROKER_VHOST": "/",
    "CELERY_RESULT_BACKEND": "database",
    "CELERY_ALWAYS_EAGER": False,
    "CELERY_EAGER_PROPAGATES_EXCEPTIONS": False,
    "CELERY_TASK_RESULT_EXPIRES": timedelta(days=1),
    "CELERY_SEND_EVENTS": False,
    "CELERY_IGNORE_RESULT": False,
    "CELERY_STORE_ERRORS_EVEN_IF_IGNORED": False,
    "CELERY_TASK_SERIALIZER": "pickle",
    "CELERY_DEFAULT_RATE_LIMIT": None,
    "CELERY_DISABLE_RATE_LIMITS": False,
    "CELERYD_TASK_TIME_LIMIT": None,
    "CELERYD_TASK_SOFT_TIME_LIMIT": None,
    "CELERYD_MAX_TASKS_PER_CHILD": None,
    "CELERY_ROUTES": None,
    "CELERY_CREATE_MISSING_QUEUES": True,
    "CELERY_DEFAULT_ROUTING_KEY": "celery",
    "CELERY_DEFAULT_QUEUE": "celery",
    "CELERY_DEFAULT_EXCHANGE": "celery",
    "CELERY_DEFAULT_EXCHANGE_TYPE": "direct",
    "CELERY_DEFAULT_DELIVERY_MODE": 2, # persistent
    "CELERY_ACKS_LATE": False,
    "CELERY_CACHE_BACKEND": None,
    "CELERY_CACHE_BACKEND_OPTIONS": {},
    "CELERYD_POOL_PUTLOCKS": True,
    "CELERYD_POOL": "celery.concurrency.processes.TaskPool",
    "CELERYD_MEDIATOR": "celery.worker.controllers.Mediator",
    "CELERYD_ETA_SCHEDULER": "celery.utils.timer2.Timer",
    "CELERYD_LISTENER": "celery.worker.listener.CarrotListener",
    "CELERYD_CONCURRENCY": 0, # defaults to cpu count
    "CELERYD_PREFETCH_MULTIPLIER": 4,
    "CELERYD_LOG_FORMAT": DEFAULT_PROCESS_LOG_FMT,
    "CELERYD_TASK_LOG_FORMAT": DEFAULT_TASK_LOG_FMT,
    "CELERYD_LOG_COLOR": None,
    "CELERYD_LOG_LEVEL": "WARN",
    "CELERYD_LOG_FILE": None, # stderr
    "CELERYBEAT_SCHEDULE": {},
    "CELERYD_STATE_DB": None,
    "CELERYD_ETA_SCHEDULER_PRECISION": 1,
    "CELERYBEAT_SCHEDULE_FILENAME": "celerybeat-schedule",
    "CELERYBEAT_MAX_LOOP_INTERVAL": 5 * 60, # five minutes.
    "CELERYBEAT_LOG_LEVEL": "INFO",
    "CELERYBEAT_LOG_FILE": None, # stderr
    "CELERYMON_LOG_LEVEL": "INFO",
    "CELERYMON_LOG_FILE": None, # stderr
    "CELERYMON_LOG_FORMAT": DEFAULT_LOG_FMT,
    "CELERY_BROADCAST_QUEUE": "celeryctl",
    "CELERY_BROADCAST_EXCHANGE": "celeryctl",
    "CELERY_BROADCAST_EXCHANGE_TYPE": "fanout",
    "CELERY_EVENT_QUEUE": "celeryevent",
    "CELERY_EVENT_EXCHANGE": "celeryevent",
    "CELERY_EVENT_EXCHANGE_TYPE": "direct",
    "CELERY_EVENT_ROUTING_KEY": "celeryevent",
    "CELERY_EVENT_SERIALIZER": "json",
    "CELERY_RESULT_DBURI": None,
    "CELERY_RESULT_ENGINE_OPTIONS": None,
    "CELERY_RESULT_EXCHANGE": "celeryresults",
    "CELERY_RESULT_EXCHANGE_TYPE": "direct",
    "CELERY_RESULT_SERIALIZER": "pickle",
    "CELERY_RESULT_PERSISTENT": False,
    "CELERY_MAX_CACHED_RESULTS": 5000,
    "CELERY_TRACK_STARTED": False,

    # Default e-mail settings.
    "SERVER_EMAIL": "celery@localhost",
    "EMAIL_HOST": "localhost",
    "EMAIL_PORT": 25,
    "EMAIL_HOST_USER": None,
    "EMAIL_HOST_PASSWORD": None,
    "ADMINS": (),
}
