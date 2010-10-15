from datetime import timedelta

DEFAULT_PROCESS_LOG_FMT = """
    [%(asctime)s: %(levelname)s/%(processName)s] %(message)s
""".strip()
DEFAULT_LOG_FMT = '[%(asctime)s: %(levelname)s] %(message)s'
DEFAULT_TASK_LOG_FMT = " ".join("""
    [%(asctime)s: %(levelname)s/%(processName)s]
    [%(task_name)s(%(task_id)s)] %(message)s
""".strip().split())


def str_to_bool(s):
    s = s.lower()
    if s in ("false", "no", "0"):
        return False
    if s in ("true", "yes", "1"):
        return True
    raise TypeError("%r can not be converted to type bool" % (s, ))


class Option(object):
    typemap = {"string": str,
               "int": int,
               "float": float,
               "bool": str_to_bool,
               "dict": dict,
               "tuple": tuple}

    def __init__(self, default=None, *args, **kwargs):
        self.default = default
        kwargs.setdefault("type", "string")
        self.type = kwargs["type"]
        self.args = args
        self.kwargs = kwargs

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
    },
    "CELERY": {
        "ACKS_LATE": Option(False, type="bool"),
        "ALWAYS_EAGER": Option(False, type="bool"),
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
        "EVENT_QUEUE": Option("celeryevent"),
        "EVENT_EXCHANGE": Option("celeryevent"),
        "EVENT_EXCHANGE_TYPE": Option("direct"),
        "EVENT_ROUTING_KEY": Option("celeryevent"),
        "EVENT_SERIALIZER": Option("json"),
        "IMPORTS": Option((), type="tuple"),
        "IGNORE_RESULT": Option(False, type="bool"),
        "MAX_CACHED_RESULTS": Option(5000, type="int"),
        "RESULT_BACKEND": Option("amqp"),
        "RESULT_DBURI": Option(),
        "RESULT_ENGINE_OPTIONS": Option(None, type="dict"),
        "RESULT_EXCHANGE": Option("celeryresults"),
        "RESULT_EXCHANGE_TYPE": Option("direct"),
        "RESULT_SERIALIZER": Option("pickle"),
        "RESULT_PERSISTENT": Option(False, type="bool"),
        "SEND_EVENTS": Option(False, type="bool"),
        "SEND_TASK_ERROR_EMAILS": Option(False, type="bool"),
        "STORE_ERRORS_EVEN_IF_IGNORED": Option(False, type="bool"),
        "TASK_RESULT_EXPIRES": Option(timedelta(days=1), type="int"),
        "AMQP_TASK_RESULT_EXPIRES": Option(type="int"),
        "TASK_ERROR_WHITELIST": Option((), type="tuple"),
        "TASK_SERIALIZER": Option("pickle"),
        "TRACK_STARTED": Option(False, type="bool"),
        "REDIRECT_STDOUTS": Option(True, type="bool"),
        "REDIRECT_STDOUTS_LEVEL": Option("WARNING"),
    },
    "CELERYD": {
        "CONCURRENCY": Option(0, type="int"),
        "ETA_SCHEDULER": Option("celery.utils.timer2.Timer"),
        "ETA_SCHEDULER_PRECISION": Option(1.0, type="float"),
        "LISTENER": Option("celery.worker.listener.CarrotListener"),
        "LOG_FORMAT": Option(DEFAULT_PROCESS_LOG_FMT),
        "LOG_COLOR": Option(type="bool"),
        "LOG_LEVEL": Option("WARN"),
        "LOG_FILE": Option(),
        "MEDIATOR": Option("celery.worker.controllers.Mediator"),
        "MAX_TASKS_PER_CHILD": Option(type="int"),
        "POOL": Option("celery.concurrency.processes.TaskPool"),
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
