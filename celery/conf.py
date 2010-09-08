import sys
import logging
import warnings
from datetime import timedelta

from celery import routes
from celery.loaders import load_settings

DEFAULT_PROCESS_LOG_FMT = """
    [%(asctime)s: %(levelname)s/%(processName)s] %(message)s
""".strip()
DEFAULT_LOG_FMT = '[%(asctime)s: %(levelname)s] %(message)s'
DEFAULT_TASK_LOG_FMT = " ".join("""
    [%(asctime)s: %(levelname)s/%(processName)s]
    [%(task_name)s(%(task_id)s)] %(message)s
""".strip().split())

LOG_LEVELS = dict(logging._levelNames)
LOG_LEVELS["FATAL"] = logging.FATAL
LOG_LEVELS[logging.FATAL] = "FATAL"

settings = load_settings()

_DEFAULTS = {
    "BROKER_CONNECTION_TIMEOUT": 4,
    "BROKER_CONNECTION_RETRY": True,
    "BROKER_CONNECTION_MAX_RETRIES": 100,
    "BROKER_HOST": "localhost",
    "BROKER_PORT": None,
    "BROKER_USER": "guest",
    "BROKER_PASSWORD": "guest",
    "BROKER_VHOST": "/",
    "CELERY_RESULT_BACKEND": "database",
    "CELERY_ALWAYS_EAGER": False,
    "CELERY_EAGER_PROPAGATES_EXCEPTIONS": False,
    "CELERY_TASK_RESULT_EXPIRES": timedelta(days=1),
    "CELERY_SEND_EVENTS": False,
    "CELERY_IGNORE_RESULT": False,
    "CELERY_STORE_ERRORS_EVEN_IF_IGNORED": False,
    "CELERY_TASK_SERIALIZER": "pickle",
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
    "CELERYD_POOL_PUTLOCKS": True,
    "CELERYD_POOL": "celery.concurrency.processes.TaskPool",
    "CELERYD_MEDIATOR": "celery.worker.controllers.Mediator",
    "CELERYD_ETA_SCHEDULER": "celery.utils.timer2.Timer",
    "CELERYD_LISTENER": "celery.worker.listener.CarrotListener",
    "CELERYD_CONCURRENCY": 0, # defaults to cpu count
    "CELERYD_PREFETCH_MULTIPLIER": 4,
    "CELERYD_LOG_FORMAT": DEFAULT_PROCESS_LOG_FMT,
    "CELERYD_TASK_LOG_FORMAT": DEFAULT_TASK_LOG_FMT,
    "CELERYD_LOG_COLOR": False,
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
    "ADMINS": (),
}


def isatty(fh):
    # Fixes bug with mod_wsgi:
    #   mod_wsgi.Log object has no attribute isatty.
    return getattr(fh, "isatty", None) and fh.isatty()


_DEPRECATION_FMT = """
%s is deprecated in favor of %s and is scheduled for removal in celery v1.4.
""".strip()

def prepare(m, source=settings, defaults=_DEFAULTS):

    def _get(name, default=None, compat=None):
        compat = compat or []
        if default is None:
            default = defaults.get(name)
        compat = [name] + compat
        for i, alias in enumerate(compat):
            try:
                value = getattr(source, alias)
                i > 0 and warnings.warn(
                        DeprecationWarning(_DEPRECATION_FMT % (alias, name)))
                return value
            except AttributeError:
                pass
        return default

    # <--- Task                                        <-   --   --- - ----- -- #
    m.ALWAYS_EAGER = _get("CELERY_ALWAYS_EAGER")
    m.EAGER_PROPAGATES_EXCEPTIONS = _get("CELERY_EAGER_PROPAGATES_EXCEPTIONS")
    m.RESULT_BACKEND = _get("CELERY_RESULT_BACKEND", compat=["CELERY_BACKEND"])
    m.CELERY_BACKEND = RESULT_BACKEND # FIXME Remove in 1.4
    m.CACHE_BACKEND = _get("CELERY_CACHE_BACKEND") or _get("CACHE_BACKEND")
    m.CACHE_BACKEND_OPTIONS = _get("CELERY_CACHE_BACKEND_OPTIONS") or {}
    m.TASK_SERIALIZER = _get("CELERY_TASK_SERIALIZER")
    m.TASK_RESULT_EXPIRES = _get("CELERY_TASK_RESULT_EXPIRES")
    m.IGNORE_RESULT = _get("CELERY_IGNORE_RESULT")
    m.TRACK_STARTED = _get("CELERY_TRACK_STARTED")
    m.ACKS_LATE = _get("CELERY_ACKS_LATE")

    # Make sure TASK_RESULT_EXPIRES is a timedelta.
    if isinstance(m.TASK_RESULT_EXPIRES, int):
        m.TASK_RESULT_EXPIRES = timedelta(seconds=m.TASK_RESULT_EXPIRES)

    # <--- SQLAlchemy                                  <-   --   --- - ----- -- #
    m.RESULT_DBURI = _get("CELERY_RESULT_DBURI")
    m.RESULT_ENGINE_OPTIONS = _get("CELERY_RESULT_ENGINE_OPTIONS")

    # <--- Client                                      <-   --   --- - ----- -- #

    m.MAX_CACHED_RESULTS = _get("CELERY_MAX_CACHED_RESULTS")

    # <--- Worker                                      <-   --   --- - ----- -- #

    m.SEND_EVENTS = _get("CELERY_SEND_EVENTS")
    m.DEFAULT_RATE_LIMIT = _get("CELERY_DEFAULT_RATE_LIMIT")
    m.DISABLE_RATE_LIMITS = _get("CELERY_DISABLE_RATE_LIMITS")
    m.CELERYD_TASK_TIME_LIMIT = _get("CELERYD_TASK_TIME_LIMIT")
    m.CELERYD_TASK_SOFT_TIME_LIMIT = _get("CELERYD_TASK_SOFT_TIME_LIMIT")
    m.CELERYD_MAX_TASKS_PER_CHILD = _get("CELERYD_MAX_TASKS_PER_CHILD")
    m.STORE_ERRORS_EVEN_IF_IGNORED = _get("CELERY_STORE_ERRORS_EVEN_IF_IGNORED")
    m.CELERY_SEND_TASK_ERROR_EMAILS = _get("CELERY_SEND_TASK_ERROR_EMAILS",
                                           False,
                                    compat=["SEND_CELERY_TASK_ERROR_EMAILS"])
    m.CELERY_TASK_ERROR_WHITELIST = _get("CELERY_TASK_ERROR_WHITELIST")
    m.CELERYD_LOG_FORMAT = _get("CELERYD_LOG_FORMAT",
                          compat=["CELERYD_DAEMON_LOG_FORMAT"])
    m.CELERYD_TASK_LOG_FORMAT = _get("CELERYD_TASK_LOG_FORMAT")
    m.CELERYD_LOG_FILE = _get("CELERYD_LOG_FILE")
    m.CELERYD_LOG_COLOR = _get("CELERYD_LOG_COLOR",
                       CELERYD_LOG_FILE is None and isatty(sys.stderr))
    m.CELERYD_LOG_LEVEL = _get("CELERYD_LOG_LEVEL",
                            compat=["CELERYD_DAEMON_LOG_LEVEL"])
    if not isinstance(m.CELERYD_LOG_LEVEL, int):
        m.CELERYD_LOG_LEVEL = LOG_LEVELS[m.CELERYD_LOG_LEVEL.upper()]
    m.CELERYD_STATE_DB = _get("CELERYD_STATE_DB")
    m.CELERYD_CONCURRENCY = _get("CELERYD_CONCURRENCY")
    m.CELERYD_PREFETCH_MULTIPLIER = _get("CELERYD_PREFETCH_MULTIPLIER")
    m.CELERYD_POOL_PUTLOCKS = _get("CELERYD_POOL_PUTLOCKS")

    m.CELERYD_POOL = _get("CELERYD_POOL")
    m.CELERYD_LISTENER = _get("CELERYD_LISTENER")
    m.CELERYD_MEDIATOR = _get("CELERYD_MEDIATOR")
    m.CELERYD_ETA_SCHEDULER = _get("CELERYD_ETA_SCHEDULER")
    m.CELERYD_ETA_SCHEDULER_PRECISION = _get("CELERYD_ETA_SCHEDULER_PRECISION")

    # :--- Email settings                               <-   --   --- - ----- -- #
    m.ADMINS = _get("ADMINS")
    m.SERVER_EMAIL = _get("SERVER_EMAIL")
    m.EMAIL_HOST = _get("EMAIL_HOST")
    m.EMAIL_HOST_USER = _get("EMAIL_HOST_USER")
    m.EMAIL_HOST_PASSWORD = _get("EMAIL_HOST_PASSWORD")
    m.EMAIL_PORT = _get("EMAIL_PORT")

    # :--- Broker connections                           <-   --   --- - ----- -- #
    m.BROKER_HOST = _get("BROKER_HOST")
    m.BROKER_PORT = _get("BROKER_PORT")
    m.BROKER_USER = _get("BROKER_USER")
    m.BROKER_PASSWORD = _get("BROKER_PASSWORD")
    m.BROKER_VHOST = _get("BROKER_VHOST")
    m.BROKER_USE_SSL = _get("BROKER_USE_SSL")
    m.BROKER_INSIST = _get("BROKER_INSIST")
    m.BROKER_CONNECTION_TIMEOUT = _get("BROKER_CONNECTION_TIMEOUT",
                                compat=["CELERY_BROKER_CONNECTION_TIMEOUT"])
    m.BROKER_CONNECTION_RETRY = _get("BROKER_CONNECTION_RETRY",
                                compat=["CELERY_BROKER_CONNECTION_RETRY"])
    m.BROKER_CONNECTION_MAX_RETRIES = _get("BROKER_CONNECTION_MAX_RETRIES",
                            compat=["CELERY_BROKER_CONNECTION_MAX_RETRIES"])
    m.BROKER_BACKEND = _get("BROKER_TRANSPORT") or \
                            _get("BROKER_BACKEND") or \
                                _get("CARROT_BACKEND")

    # <--- Message routing                             <-   --   --- - ----- -- #
    m.DEFAULT_QUEUE = _get("CELERY_DEFAULT_QUEUE")
    m.DEFAULT_ROUTING_KEY = _get("CELERY_DEFAULT_ROUTING_KEY")
    m.DEFAULT_EXCHANGE = _get("CELERY_DEFAULT_EXCHANGE")
    m.DEFAULT_EXCHANGE_TYPE = _get("CELERY_DEFAULT_EXCHANGE_TYPE")
    m.DEFAULT_DELIVERY_MODE = _get("CELERY_DEFAULT_DELIVERY_MODE")
    m.QUEUES = _get("CELERY_QUEUES") or {DEFAULT_QUEUE: {
                                       "exchange": DEFAULT_EXCHANGE,
                                       "exchange_type": DEFAULT_EXCHANGE_TYPE,
                                       "binding_key": DEFAULT_ROUTING_KEY}}
    m.CREATE_MISSING_QUEUES = _get("CELERY_CREATE_MISSING_QUEUES")
    m.ROUTES = routes.prepare(_get("CELERY_ROUTES") or [])
    # :--- Broadcast queue settings                     <-   --   --- - ----- -- #

    m.BROADCAST_QUEUE = _get("CELERY_BROADCAST_QUEUE")
    m.BROADCAST_EXCHANGE = _get("CELERY_BROADCAST_EXCHANGE")
    m.BROADCAST_EXCHANGE_TYPE = _get("CELERY_BROADCAST_EXCHANGE_TYPE")

# :--- Event queue settings                         <-   --   --- - ----- -- #

    m.EVENT_QUEUE = _get("CELERY_EVENT_QUEUE")
    m.EVENT_EXCHANGE = _get("CELERY_EVENT_EXCHANGE")
    m.EVENT_EXCHANGE_TYPE = _get("CELERY_EVENT_EXCHANGE_TYPE")
    m.EVENT_ROUTING_KEY = _get("CELERY_EVENT_ROUTING_KEY")
    m.EVENT_SERIALIZER = _get("CELERY_EVENT_SERIALIZER")

# :--- AMQP Backend settings                        <-   --   --- - ----- -- #

    m.RESULT_EXCHANGE = _get("CELERY_RESULT_EXCHANGE")
    m.RESULT_EXCHANGE_TYPE = _get("CELERY_RESULT_EXCHANGE_TYPE")
    m.RESULT_SERIALIZER = _get("CELERY_RESULT_SERIALIZER")
    m.RESULT_PERSISTENT = _get("CELERY_RESULT_PERSISTENT")

# :--- Celery Beat                                  <-   --   --- - ----- -- #
    m.CELERYBEAT_LOG_LEVEL = _get("CELERYBEAT_LOG_LEVEL")
    m.CELERYBEAT_LOG_FILE = _get("CELERYBEAT_LOG_FILE")
    m.CELERYBEAT_SCHEDULE = _get("CELERYBEAT_SCHEDULE")
    m.CELERYBEAT_SCHEDULE_FILENAME = _get("CELERYBEAT_SCHEDULE_FILENAME")
    m.CELERYBEAT_MAX_LOOP_INTERVAL = _get("CELERYBEAT_MAX_LOOP_INTERVAL")

# :--- Celery Monitor                               <-   --   --- - ----- -- #
    m.CELERYMON_LOG_LEVEL = _get("CELERYMON_LOG_LEVEL")
    m.CELERYMON_LOG_FILE = _get("CELERYMON_LOG_FILE")

prepare(sys.modules[__name__])

def _init_queues(queues):
    """Convert configuration mapping to a table of queues digestible
    by a :class:`carrot.messaging.ConsumerSet`."""

    def _defaults(opts):
        opts.setdefault("exchange", DEFAULT_EXCHANGE),
        opts.setdefault("exchange_type", DEFAULT_EXCHANGE_TYPE)
        opts.setdefault("binding_key", DEFAULT_EXCHANGE)
        opts.setdefault("routing_key", opts.get("binding_key"))
        return opts

    return dict((queue, _defaults(opts)) for queue, opts in queues.items())


def get_queues():
    return _init_queues(QUEUES)
