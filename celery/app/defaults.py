import sys

from datetime import timedelta

is_jython = sys.platform.startswith("java")
is_pypy = hasattr(sys, "pypy_version_info")

DEFAULT_POOL = "processes"
if is_jython:
    DEFAULT_POOL = "threads"
elif is_pypy:
    DEFAULT_POOL = "solo"

DEFAULT_PROCESS_LOG_FMT = """
    [%(asctime)s: %(levelname)s/%(processName)s] %(message)s
""".strip()
DEFAULT_LOG_FMT = '[%(asctime)s: %(levelname)s] %(message)s'
DEFAULT_TASK_LOG_FMT = """[%(asctime)s: %(levelname)s/%(processName)s] \
%(task_name)s[%(task_id)s]: %(message)s"""


def str_to_bool(term, table={"false": False, "no": False, "0": False,
                             "true":  True, "yes": True,  "1": True}):
    try:
        return table[term.lower()]
    except KeyError:
        raise TypeError("%r can not be converted to type bool" % (term, ))


class Option(object):
    typemap = dict(string=str, int=int, float=float, any=lambda v: v,
                   bool=str_to_bool, dict=dict, tuple=tuple)

    def __init__(self, default=None, *args, **kwargs):
        self.default = default
        self.type = kwargs.get("type") or "string"

    def to_python(self, value):
        return self.typemap[self.type](value)


NAMESPACES = {
    "BROKER": {
        "HOST": Option("localhost"),
        "PORT": Option(type="int"),
        "USER": Option("guest"),
        "PASSWORD": Option("guest"),
        "VHOST": Option("/"),
        "BACKEND": Option(),
        "CONNECTION_TIMEOUT": Option(4, type="int"),
        "CONNECTION_RETRY": Option(True, type="bool"),
        "CONNECTION_MAX_RETRIES": Option(100, type="int"),
        "INSIST": Option(False, type="bool"),
        "USE_SSL": Option(False, type="bool"),
        "TRANSPORT_OPTIONS": Option({}, type="dict")
    },
    "CELERY": {
        "ACKS_LATE": Option(False, type="bool"),
        "ALWAYS_EAGER": Option(False, type="bool"),
        "AMQP_TASK_RESULT_EXPIRES": Option(type="int"),
        "AMQP_TASK_RESULT_CONNECTION_MAX": Option(1, type="int"),
        "BROADCAST_QUEUE": Option("celeryctl"),
        "BROADCAST_EXCHANGE": Option("celeryctl"),
        "BROADCAST_EXCHANGE_TYPE": Option("fanout"),
        "CACHE_BACKEND": Option(),
        "CACHE_BACKEND_OPTIONS": Option({}, type="dict"),
        "CREATE_MISSING_QUEUES": Option(True, type="bool"),
        "DEFAULT_RATE_LIMIT": Option(type="string"),
        "DISABLE_RATE_LIMITS": Option(False, type="bool"),
        "DEFAULT_ROUTING_KEY": Option("celery"),
        "DEFAULT_QUEUE": Option("celery"),
        "DEFAULT_EXCHANGE": Option("celery"),
        "DEFAULT_EXCHANGE_TYPE": Option("direct"),
        "DEFAULT_DELIVERY_MODE": Option(2, type="string"),
        "EAGER_PROPAGATES_EXCEPTIONS": Option(False, type="bool"),
        "EVENT_SERIALIZER": Option("json"),
        "IMPORTS": Option((), type="tuple"),
        "IGNORE_RESULT": Option(False, type="bool"),
        "MAX_CACHED_RESULTS": Option(5000, type="int"),
        "MESSAGE_COMPRESSION": Option(None, type="string"),
        "RESULT_BACKEND": Option("amqp"),
        "RESULT_DBURI": Option(),
        "RESULT_ENGINE_OPTIONS": Option(None, type="dict"),
        "RESULT_EXCHANGE": Option("celeryresults"),
        "RESULT_EXCHANGE_TYPE": Option("direct"),
        "RESULT_SERIALIZER": Option("pickle"),
        "RESULT_PERSISTENT": Option(False, type="bool"),
        "ROUTES": Option(None, type="any"),
        "SEND_EVENTS": Option(False, type="bool"),
        "SEND_TASK_ERROR_EMAILS": Option(False, type="bool"),
        "SEND_TASK_SENT_EVENT": Option(False, type="bool"),
        "STORE_ERRORS_EVEN_IF_IGNORED": Option(False, type="bool"),
        "TASK_ERROR_WHITELIST": Option((), type="tuple"),
        "TASK_PUBLISH_RETRY": Option(False, type="bool"),
        "TASK_PUBLISH_RETRY_POLICY": Option({
                "max_retries": 3,
                "interval_start": 0,
                "interval_max": 0.2,
                "interval_step": 0.2}, type="dict"),
        "TASK_RESULT_EXPIRES": Option(timedelta(days=1), type="int"),
        "TASK_SERIALIZER": Option("pickle"),
        "TRACK_STARTED": Option(False, type="bool"),
        "REDIRECT_STDOUTS": Option(True, type="bool"),
        "REDIRECT_STDOUTS_LEVEL": Option("WARNING"),
        "QUEUES": Option(None, type="dict"),
    },
    "CELERYD": {
        "AUTOSCALER": Option("celery.worker.autoscale.Autoscaler"),
        "CONCURRENCY": Option(0, type="int"),
        "ETA_SCHEDULER": Option(None, type="str"),
        "ETA_SCHEDULER_PRECISION": Option(1.0, type="float"),
        "HIJACK_ROOT_LOGGER": Option(True, type="bool"),
        "CONSUMER": Option("celery.worker.consumer.Consumer"),
        "LOG_FORMAT": Option(DEFAULT_PROCESS_LOG_FMT),
        "LOG_COLOR": Option(type="bool"),
        "LOG_LEVEL": Option("WARN"),
        "LOG_FILE": Option(),
        "MEDIATOR": Option("celery.worker.mediator.Mediator"),
        "MAX_TASKS_PER_CHILD": Option(type="int"),
        "POOL": Option(DEFAULT_POOL),
        "POOL_PUTLOCKS": Option(True, type="bool"),
        "PREFETCH_MULTIPLIER": Option(4, type="int"),
        "STATE_DB": Option(),
        "TASK_LOG_FORMAT": Option(DEFAULT_TASK_LOG_FMT),
        "TASK_SOFT_TIME_LIMIT": Option(type="int"),
        "TASK_TIME_LIMIT": Option(type="int"),
    },
    "CELERYBEAT": {
        "SCHEDULE": Option({}, type="dict"),
        "SCHEDULER": Option("celery.beat.PersistentScheduler"),
        "SCHEDULE_FILENAME": Option("celerybeat-schedule"),
        "MAX_LOOP_INTERVAL": Option(5 * 60, type="int"),
        "LOG_LEVEL": Option("INFO"),
        "LOG_FILE": Option(),
    },
    "CELERYMON": {
        "LOG_LEVEL": Option("INFO"),
        "LOG_FILE": Option(),
        "LOG_FORMAT": Option(DEFAULT_LOG_FMT),
    },
    "EMAIL": {
        "HOST": Option("localhost"),
        "PORT": Option(25, type="int"),
        "HOST_USER": Option(None),
        "HOST_PASSWORD": Option(None),
        "TIMEOUT": Option(2, type="int"),
    },
    "SERVER_EMAIL": Option("celery@localhost"),
    "ADMINS": Option((), type="tuple"),
}


def _flatten(d, ns=""):
    acc = []
    for key, value in d.iteritems():
        if isinstance(value, dict):
            acc.extend(_flatten(value, ns=key + '_'))
        else:
            acc.append((ns + key, value.default))
    return acc

DEFAULTS = dict(_flatten(NAMESPACES))
