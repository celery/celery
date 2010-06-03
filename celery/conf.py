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
    "CELERY_RESULT_BACKEND": "database",
    "CELERY_ALWAYS_EAGER": False,
    "CELERY_EAGER_PROPAGATES_EXCEPTIONS": False,
    "CELERY_TASK_RESULT_EXPIRES": timedelta(days=5),
    "CELERY_SEND_EVENTS": False,
    "CELERY_IGNORE_RESULT": False,
    "CELERY_STORE_ERRORS_EVEN_IF_IGNORED": False,
    "CELERY_TASK_SERIALIZER": "pickle",
    "CELERY_DISABLE_RATE_LIMITS": False,
    "CELERYD_TASK_TIME_LIMIT": None,
    "CELERYD_TASK_SOFT_TIME_LIMIT": None,
    "CELERYD_MAX_TASKS_PER_CHILD": None,
    "CELERY_ROUTES": None,
    "CELERY_DEFAULT_ROUTING_KEY": "celery",
    "CELERY_DEFAULT_QUEUE": "celery",
    "CELERY_DEFAULT_EXCHANGE": "celery",
    "CELERY_DEFAULT_EXCHANGE_TYPE": "direct",
    "CELERY_DEFAULT_DELIVERY_MODE": 2, # persistent
    "CELERY_BROKER_CONNECTION_TIMEOUT": 4,
    "CELERY_BROKER_CONNECTION_RETRY": True,
    "CELERY_BROKER_CONNECTION_MAX_RETRIES": 100,
    "CELERY_ACKS_LATE": False,
    "CELERYD_POOL_PUTLOCKS": True,
    "CELERYD_POOL": "celery.worker.pool.TaskPool",
    "CELERYD_MEDIATOR": "celery.worker.controllers.Mediator",
    "CELERYD_ETA_SCHEDULER": "celery.worker.controllers.ScheduleController",
    "CELERYD_LISTENER": "celery.worker.listener.CarrotListener",
    "CELERYD_CONCURRENCY": 0, # defaults to cpu count
    "CELERYD_PREFETCH_MULTIPLIER": 4,
    "CELERYD_LOG_FORMAT": DEFAULT_PROCESS_LOG_FMT,
    "CELERYD_TASK_LOG_FORMAT": DEFAULT_TASK_LOG_FMT,
    "CELERYD_LOG_COLOR": False,
    "CELERYD_LOG_LEVEL": "WARN",
    "CELERYD_LOG_FILE": None, # stderr
    "CELERYBEAT_SCHEDULE": {},
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
}


_DEPRECATION_FMT = """
%s is deprecated in favor of %s and is scheduled for removal in celery v1.4.
""".strip()

def _get(name, default=None, compat=None):
    compat = compat or []
    if default is None:
        default = _DEFAULTS.get(name)
    compat = [name] + compat
    for i, alias in enumerate(compat):
        try:
            value = getattr(settings, alias)
            i > 0 and warnings.warn(DeprecationWarning(_DEPRECATION_FMT % (
                                                        alias, name)))
            return value
        except AttributeError:
            pass
    return default

# <--- Task                                        <-   --   --- - ----- -- #
ALWAYS_EAGER = _get("CELERY_ALWAYS_EAGER")
EAGER_PROPAGATES_EXCEPTIONS = _get("CELERY_EAGER_PROPAGATES_EXCEPTIONS")
RESULT_BACKEND = _get("CELERY_RESULT_BACKEND", compat=["CELERY_BACKEND"])
CELERY_BACKEND = RESULT_BACKEND # FIXME Remove in 1.4
CELERY_CACHE_BACKEND = _get("CELERY_CACHE_BACKEND")
TASK_SERIALIZER = _get("CELERY_TASK_SERIALIZER")
TASK_RESULT_EXPIRES = _get("CELERY_TASK_RESULT_EXPIRES")
IGNORE_RESULT = _get("CELERY_IGNORE_RESULT")
TRACK_STARTED = _get("CELERY_TRACK_STARTED")
ACKS_LATE = _get("CELERY_ACKS_LATE")
# Make sure TASK_RESULT_EXPIRES is a timedelta.
if isinstance(TASK_RESULT_EXPIRES, int):
    TASK_RESULT_EXPIRES = timedelta(seconds=TASK_RESULT_EXPIRES)

# <--- SQLAlchemy                                  <-   --   --- - ----- -- #
RESULT_DBURI = _get("CELERY_RESULT_DBURI")
RESULT_ENGINE_OPTIONS = _get("CELERY_RESULT_ENGINE_OPTIONS")


# <--- Client                                      <-   --   --- - ----- -- #

MAX_CACHED_RESULTS = _get("CELERY_MAX_CACHED_RESULTS")

# <--- Worker                                      <-   --   --- - ----- -- #

SEND_EVENTS = _get("CELERY_SEND_EVENTS")
DEFAULT_RATE_LIMIT = _get("CELERY_DEFAULT_RATE_LIMIT")
DISABLE_RATE_LIMITS = _get("CELERY_DISABLE_RATE_LIMITS")
CELERYD_TASK_TIME_LIMIT = _get("CELERYD_TASK_TIME_LIMIT")
CELERYD_TASK_SOFT_TIME_LIMIT = _get("CELERYD_TASK_SOFT_TIME_LIMIT")
CELERYD_MAX_TASKS_PER_CHILD = _get("CELERYD_MAX_TASKS_PER_CHILD")
STORE_ERRORS_EVEN_IF_IGNORED = _get("CELERY_STORE_ERRORS_EVEN_IF_IGNORED")
CELERY_SEND_TASK_ERROR_EMAILS = _get("CELERY_SEND_TASK_ERROR_EMAILS",
                                     not settings.DEBUG,
                                     compat=["SEND_CELERY_TASK_ERROR_EMAILS"])
CELERYD_LOG_FORMAT = _get("CELERYD_LOG_FORMAT",
                          compat=["CELERYD_DAEMON_LOG_FORMAT"])
CELERYD_TASK_LOG_FORMAT = _get("CELERYD_TASK_LOG_FORMAT")
CELERYD_LOG_FILE = _get("CELERYD_LOG_FILE")
CELERYD_LOG_COLOR = _get("CELERYD_LOG_COLOR", \
                       CELERYD_LOG_FILE is None and sys.stderr.isatty())
CELERYD_LOG_LEVEL = _get("CELERYD_LOG_LEVEL",
                            compat=["CELERYD_DAEMON_LOG_LEVEL"])
CELERYD_LOG_LEVEL = LOG_LEVELS[CELERYD_LOG_LEVEL.upper()]
CELERYD_CONCURRENCY = _get("CELERYD_CONCURRENCY")
CELERYD_PREFETCH_MULTIPLIER = _get("CELERYD_PREFETCH_MULTIPLIER")
CELERYD_POOL_PUTLOCKS = _get("CELERYD_POOL_PUTLOCKS")

CELERYD_POOL = _get("CELERYD_POOL")
CELERYD_LISTENER = _get("CELERYD_LISTENER")
CELERYD_MEDIATOR = _get("CELERYD_MEDIATOR")
CELERYD_ETA_SCHEDULER = _get("CELERYD_ETA_SCHEDULER")

# <--- Message routing                             <-   --   --- - ----- -- #
DEFAULT_QUEUE = _get("CELERY_DEFAULT_QUEUE")
DEFAULT_ROUTING_KEY = _get("CELERY_DEFAULT_ROUTING_KEY")
DEFAULT_EXCHANGE = _get("CELERY_DEFAULT_EXCHANGE")
DEFAULT_EXCHANGE_TYPE = _get("CELERY_DEFAULT_EXCHANGE_TYPE")
DEFAULT_DELIVERY_MODE = _get("CELERY_DEFAULT_DELIVERY_MODE")
QUEUES = _get("CELERY_QUEUES") or {DEFAULT_QUEUE: {
                                       "exchange": DEFAULT_EXCHANGE,
                                       "exchange_type": DEFAULT_EXCHANGE_TYPE,
                                       "binding_key": DEFAULT_ROUTING_KEY}}
ROUTES = routes.prepare(_get("CELERY_ROUTES") or [])
# :--- Broadcast queue settings                     <-   --   --- - ----- -- #

BROADCAST_QUEUE = _get("CELERY_BROADCAST_QUEUE")
BROADCAST_EXCHANGE = _get("CELERY_BROADCAST_EXCHANGE")
BROADCAST_EXCHANGE_TYPE = _get("CELERY_BROADCAST_EXCHANGE_TYPE")

# :--- Event queue settings                         <-   --   --- - ----- -- #

EVENT_QUEUE = _get("CELERY_EVENT_QUEUE")
EVENT_EXCHANGE = _get("CELERY_EVENT_EXCHANGE")
EVENT_EXCHANGE_TYPE = _get("CELERY_EVENT_EXCHANGE_TYPE")
EVENT_ROUTING_KEY = _get("CELERY_EVENT_ROUTING_KEY")
EVENT_SERIALIZER = _get("CELERY_EVENT_SERIALIZER")

# :--- Broker connections                           <-   --   --- - ----- -- #
BROKER_CONNECTION_TIMEOUT = _get("CELERY_BROKER_CONNECTION_TIMEOUT",
                                compat=["CELERY_AMQP_CONNECTION_TIMEOUT"])
BROKER_CONNECTION_RETRY = _get("CELERY_BROKER_CONNECTION_RETRY",
                                compat=["CELERY_AMQP_CONNECTION_RETRY"])
BROKER_CONNECTION_MAX_RETRIES = _get("CELERY_BROKER_CONNECTION_MAX_RETRIES",
                                compat=["CELERY_AMQP_CONNECTION_MAX_RETRIES"])

# :--- AMQP Backend settings                        <-   --   --- - ----- -- #

RESULT_EXCHANGE = _get("CELERY_RESULT_EXCHANGE")
RESULT_EXCHANGE_TYPE = _get("CELERY_RESULT_EXCHANGE_TYPE")
RESULT_SERIALIZER = _get("CELERY_RESULT_SERIALIZER")
RESULT_PERSISTENT = _get("CELERY_RESULT_PERSISTENT")

# :--- Celery Beat                                  <-   --   --- - ----- -- #
CELERYBEAT_LOG_LEVEL = _get("CELERYBEAT_LOG_LEVEL")
CELERYBEAT_LOG_FILE = _get("CELERYBEAT_LOG_FILE")
CELERYBEAT_SCHEDULE = _get("CELERYBEAT_SCHEDULE")
CELERYBEAT_SCHEDULE_FILENAME = _get("CELERYBEAT_SCHEDULE_FILENAME")
CELERYBEAT_MAX_LOOP_INTERVAL = _get("CELERYBEAT_MAX_LOOP_INTERVAL")

# :--- Celery Monitor                               <-   --   --- - ----- -- #
CELERYMON_LOG_LEVEL = _get("CELERYMON_LOG_LEVEL")
CELERYMON_LOG_FILE = _get("CELERYMON_LOG_FILE")


def _init_routing_table(queues):
    """Convert configuration mapping to a table of queues digestible
    by a :class:`carrot.messaging.ConsumerSet`."""

    def _defaults(opts):
        opts.setdefault("exchange", DEFAULT_EXCHANGE),
        opts.setdefault("exchange_type", DEFAULT_EXCHANGE_TYPE)
        opts.setdefault("binding_key", DEFAULT_EXCHANGE)
        return opts

    return dict((queue, _defaults(opts)) for queue, opts in queues.items())


def get_routing_table():
    return _init_routing_table(QUEUES)
