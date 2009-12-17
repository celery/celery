import logging
import warnings
from datetime import timedelta

from celery.registry import tasks
from celery.loaders import settings

DEFAULT_LOG_FMT = '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'

LOG_LEVELS = dict(logging._levelNames)
LOG_LEVELS["FATAL"] = logging.FATAL
LOG_LEVELS[logging.FATAL] = "FATAL"

_DEFAULTS = {
    "CELERY_DEFAULT_ROUTING_KEY": "celery",
    "CELERY_DEFAULT_QUEUE": "celery",
    "CELERY_DEFAULT_EXCHANGE": "celery",
    "CELERY_DEFAULT_EXCHANGE_TYPE": "direct",
    "CELERYD_CONCURRENCY": 0, # defaults to cpu count
    "CELERYD_PID_FILE": "celeryd.pid",
    "CELERYD_DAEMON_LOG_FORMAT": DEFAULT_LOG_FMT,
    "CELERYD_DAEMON_LOG_LEVEL": "WARN",
    "CELERYD_LOG_FILE": "celeryd.log",
    "CELERY_ALWAYS_EAGER": False,
    "CELERY_TASK_RESULT_EXPIRES": timedelta(days=5),
    "CELERY_AMQP_CONNECTION_TIMEOUT": 4,
    "CELERY_AMQP_CONNECTION_RETRY": True,
    "CELERY_AMQP_CONNECTION_MAX_RETRIES": 100,
    "CELERY_TASK_SERIALIZER": "pickle",
    "CELERY_BACKEND": "database",
    "CELERY_DISABLE_RATE_LIMITS": False,
    "CELERYBEAT_PID_FILE": "celerybeat.pid",
    "CELERYBEAT_LOG_LEVEL": "INFO",
    "CELERYBEAT_LOG_FILE": "celerybeat.log",
    "CELERYBEAT_SCHEDULE_FILENAME": "celerybeat-schedule",
    "CELERYBEAT_MAX_LOOP_INTERVAL": 5 * 60, # five minutes.
    "CELERYMON_PID_FILE": "celerymon.pid",
    "CELERYMON_LOG_LEVEL": "INFO",
    "CELERYMON_LOG_FILE": "celerymon.log",
    "CELERY_SEND_EVENTS": False,
    "CELERY_STORE_ERRORS_EVEN_IF_IGNORED": False,
}

def _get(name, default=None):
    if default is None:
        default = _DEFAULTS.get(name)
    return getattr(settings, name, default)

# <--- Task options                                <-   --   --- - ----- -- #
ALWAYS_EAGER = _get("CELERY_ALWAYS_EAGER")
CELERY_BACKEND = _get("CELERY_BACKEND")
CELERY_CACHE_BACKEND = _get("CELERY_CACHE_BACKEND")
TASK_SERIALIZER = _get("CELERY_TASK_SERIALIZER")
TASK_RESULT_EXPIRES = _get("CELERY_TASK_RESULT_EXPIRES")
# Make sure TASK_RESULT_EXPIRES is a timedelta.
if isinstance(TASK_RESULT_EXPIRES, int):
    TASK_RESULT_EXPIRES = timedelta(seconds=TASK_RESULT_EXPIRES)

# <--- Worker                                      <-   --   --- - ----- -- #

SEND_EVENTS = _get("CELERY_SEND_EVENTS")
DEFAULT_RATE_LIMIT = _get("CELERY_DEFAULT_RATE_LIMIT")
DISABLE_RATE_LIMITS = _get("CELERY_DISABLE_RATE_LIMITS")
STORE_ERRORS_EVEN_IF_IGNORED = _get("CELERY_STORE_ERRORS_EVEN_IF_IGNORED")
SEND_CELERY_TASK_ERROR_EMAILS = _get("SEND_CELERY_TASK_ERROR_EMAILS",
                                     not settings.DEBUG)
LOG_FORMAT = _get("CELERYD_DAEMON_LOG_FORMAT")
DAEMON_LOG_FILE = _get("CELERYD_LOG_FILE")
DAEMON_LOG_LEVEL = _get("CELERYD_DAEMON_LOG_LEVEL")
DAEMON_LOG_LEVEL = LOG_LEVELS[DAEMON_LOG_LEVEL.upper()]
DAEMON_PID_FILE = _get("CELERYD_PID_FILE")
DAEMON_CONCURRENCY = _get("CELERYD_CONCURRENCY")

# <--- Message routing                             <-   --   --- - ----- -- #
QUEUES = _get("CELERY_QUEUES")
DEFAULT_QUEUE = _get("CELERY_DEFAULT_QUEUE")
DEFAULT_ROUTING_KEY = _get("CELERY_DEFAULT_ROUTING_KEY")
DEFAULT_EXCHANGE = _get("CELERY_DEFAULT_EXCHANGE")
DEFAULT_EXCHANGE_TYPE = _get("CELERY_DEFAULT_EXCHANGE_TYPE")

_DEPRECATIONS = {"CELERY_AMQP_CONSUMER_QUEUES": "CELERY_QUEUES",
                 "CELERY_AMQP_CONSUMER_QUEUE": "CELERY_QUEUES",
                 "CELERY_AMQP_EXCHANGE": "CELERY_DEFAULT_EXCHANGE",
                 "CELERY_AMQP_EXCHANGE_TYPE": "CELERY_DEFAULT_EXCHANGE_TYPE",
                 "CELERY_AMQP_CONSUMER_ROUTING_KEY": "CELERY_QUEUES",
                 "CELERY_AMQP_PUBLISHER_ROUTING_KEY":
                 "CELERY_DEFAULT_ROUTING_KEY"}


_DEPRECATED_QUEUE_SETTING_FMT = """
%s is deprecated in favor of %s and scheduled for removal in celery v1.0.
Please visit http://bit.ly/5DsSuX for more information.

We're sorry for the inconvenience.
""".strip()


def _find_deprecated_queue_settings():
    global DEFAULT_QUEUE, DEFAULT_ROUTING_KEY
    global DEFAULT_EXCHANGE, DEFAULT_EXCHANGE_TYPE
    binding_key = None

    multi = _get("CELERY_AMQP_CONSUMER_QUEUES")
    if multi:
        return multi

    single = _get("CELERY_AMQP_CONSUMER_QUEUE")
    if single:
        DEFAULT_QUEUE = single
        DEFAULT_EXCHANGE = _get("CELERY_AMQP_EXCHANGE", DEFAULT_EXCHANGE)
        DEFAULT_EXCHANGE_TYPE = _get("CELERY_AMQP_EXCHANGE_TYPE",
                                     DEFAULT_EXCHANGE_TYPE)
        binding_key = _get("CELERY_AMQP_CONSUMER_ROUTING_KEY",
                            DEFAULT_ROUTING_KEY)
        DEFAULT_ROUTING_KEY = _get("CELERY_AMQP_PUBLISHER_ROUTING_KEY",
                                   DEFAULT_ROUTING_KEY)
    binding_key = binding_key or DEFAULT_ROUTING_KEY
    return {DEFAULT_QUEUE: {"exchange": DEFAULT_EXCHANGE,
                            "exchange_type": DEFAULT_EXCHANGE_TYPE,
                            "binding_key": binding_key}}

def _warn_if_deprecated_queue_settings():
    for setting, new_setting in _DEPRECATIONS.items():
        if _get(setting):
            warnings.warn(DeprecationWarning(_DEPRECATED_QUEUE_SETTING_FMT % (
                setting, _DEPRECATIONS[setting])))
            break

_warn_if_deprecated_queue_settings()
if not QUEUES:
    QUEUES = _find_deprecated_queue_settings()

# :--- Broker connections                           <-   --   --- - ----- -- #
AMQP_CONNECTION_TIMEOUT = _get("CELERY_AMQP_CONNECTION_TIMEOUT")
AMQP_CONNECTION_RETRY = _get("CELERY_AMQP_CONNECTION_RETRY")
AMQP_CONNECTION_MAX_RETRIES = _get("CELERY_AMQP_CONNECTION_MAX_RETRIES")


# :--- Celery Beat                                  <-   --   --- - ----- -- #
CELERYBEAT_PID_FILE = _get("CELERYBEAT_PID_FILE")
CELERYBEAT_LOG_LEVEL = _get("CELERYBEAT_LOG_LEVEL")
CELERYBEAT_LOG_FILE = _get("CELERYBEAT_LOG_FILE")
CELERYBEAT_SCHEDULE_FILENAME = _get("CELERYBEAT_SCHEDULE_FILENAME")
CELERYBEAT_MAX_LOOP_INTERVAL = _get("CELERYBEAT_MAX_LOOP_INTERVAL")

# :--- Celery Monitor                               <-   --   --- - ----- -- #
CELERYMON_PID_FILE = _get("CELERYMON_PID_FILE")
CELERYMON_LOG_LEVEL = _get("CELERYMON_LOG_LEVEL")
CELERYMON_LOG_FILE = _get("CELERYMON_LOG_FILE")
