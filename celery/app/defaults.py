# -*- coding: utf-8 -*-
"""
    celery.app.defaults
    ~~~~~~~~~~~~~~~~~~~

    Configuration introspection and defaults.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import sys

from collections import deque
from datetime import timedelta

from ..utils.functional import memoize

is_jython = sys.platform.startswith("java")
is_pypy = hasattr(sys, "pypy_version_info")

DEFAULT_POOL = "processes"
if is_jython:
    DEFAULT_POOL = "threads"
elif is_pypy:
    if sys.pypy_version_info[0:3] < (1, 5, 0):
        DEFAULT_POOL = "solo"
    else:
        DEFAULT_POOL = "processes"


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
        raise TypeError("Can't coerce %r to type bool" % (term, ))


class Option(object):
    alt = None
    deprecate_by = None
    remove_by = None
    typemap = dict(string=str, int=int, float=float, any=lambda v: v,
                   bool=str_to_bool, dict=dict, tuple=tuple)

    def __init__(self, default=None, *args, **kwargs):
        self.default = default
        self.type = kwargs.get("type") or "string"
        for attr, value in kwargs.iteritems():
            setattr(self, attr, value)

    def to_python(self, value):
        return self.typemap[self.type](value)


NAMESPACES = {
    "BROKER": {
        "URL": Option(None, type="string"),
        "HOST": Option(None, type="string"),
        "PORT": Option(type="int"),
        "USER": Option(None, type="string"),
        "PASSWORD": Option(None, type="string"),
        "VHOST": Option(None, type="string"),
        "CONNECTION_TIMEOUT": Option(4, type="int"),
        "CONNECTION_RETRY": Option(True, type="bool"),
        "CONNECTION_MAX_RETRIES": Option(100, type="int"),
        "POOL_LIMIT": Option(10, type="int"),
        "INSIST": Option(False, type="bool",
                         deprecate_by="2.4", remove_by="3.0"),
        "USE_SSL": Option(False, type="bool"),
        "TRANSPORT": Option(None, type="string"),
        "TRANSPORT_OPTIONS": Option({}, type="dict"),
    },
    "CASSANDRA": {
        "COLUMN_FAMILY": Option(None, type="string"),
        "DETAILED_MODE": Option(False, type="bool"),
        "KEYSPACE": Option(None, type="string"),
        "READ_CONSISTENCY": Option(None, type="string"),
        "SERVERS": Option(None, type="list"),
        "WRITE_CONSISTENCY": Option(None, type="string"),
    },
    "CELERY": {
        "ACKS_LATE": Option(False, type="bool"),
        "ALWAYS_EAGER": Option(False, type="bool"),
        "AMQP_TASK_RESULT_EXPIRES": Option(type="int",
                deprecate_by="2.5", remove_by="3.0",
                alt="CELERY_TASK_RESULT_EXPIRES"),
        "AMQP_TASK_RESULT_CONNECTION_MAX": Option(1, type="int",
            remove_by="2.5", alt="BROKER_POOL_LIMIT"),
        "ANNOTATIONS": Option(type="any"),
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
        "ENABLE_UTC": Option(False, type="bool"),
        "EVENT_SERIALIZER": Option("json"),
        "IMPORTS": Option((), type="tuple"),
        "IGNORE_RESULT": Option(False, type="bool"),
        "MAX_CACHED_RESULTS": Option(5000, type="int"),
        "MESSAGE_COMPRESSION": Option(None, type="string"),
        "MONGODB_BACKEND_SETTINGS": Option(None, type="dict"),
        "REDIS_HOST": Option(None, type="string"),
        "REDIS_PORT": Option(None, type="int"),
        "REDIS_DB": Option(None, type="int"),
        "REDIS_PASSWORD": Option(None, type="string"),
        "REDIS_MAX_CONNECTIONS": Option(None, type="int"),
        "RESULT_BACKEND": Option(None, type="string"),
        "RESULT_DB_SHORT_LIVED_SESSIONS": Option(False, type="bool"),
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
        "TASK_ERROR_WHITELIST": Option((), type="tuple",
            deprecate_by="2.5", remove_by="3.0"),
        "TASK_PUBLISH_RETRY": Option(True, type="bool"),
        "TASK_PUBLISH_RETRY_POLICY": Option({
                "max_retries": 100,
                "interval_start": 0,
                "interval_max": 1,
                "interval_step": 0.2}, type="dict"),
        "TASK_RESULT_EXPIRES": Option(timedelta(days=1), type="int"),
        "TASK_SERIALIZER": Option("pickle"),
        "TIMEZONE": Option(None, type="string"),
        "TRACK_STARTED": Option(False, type="bool"),
        "REDIRECT_STDOUTS": Option(True, type="bool"),
        "REDIRECT_STDOUTS_LEVEL": Option("WARNING"),
        "QUEUES": Option(None, type="dict"),
        "SECURITY_KEY": Option(None, type="string"),
        "SECURITY_CERTIFICATE": Option(None, type="string"),
        "SECURITY_CERT_STORE": Option(None, type="string"),
    },
    "CELERYD": {
        "AUTOSCALER": Option("celery.worker.autoscale.Autoscaler"),
        "AUTORELOADER": Option("celery.worker.autoreload.Autoreloader"),
        "BOOT_STEPS": Option((), type="tuple"),
        "CONCURRENCY": Option(0, type="int"),
        "ETA_SCHEDULER": Option(None, type="string"),
        "ETA_SCHEDULER_PRECISION": Option(1.0, type="float"),
        "FORCE_EXECV": Option(False, type="bool"),
        "HIJACK_ROOT_LOGGER": Option(True, type="bool"),
        "CONSUMER": Option("celery.worker.consumer.Consumer"),
        "LOG_FORMAT": Option(DEFAULT_PROCESS_LOG_FMT),
        "LOG_COLOR": Option(type="bool"),
        "LOG_LEVEL": Option("WARN", deprecate_by="2.4", remove_by="3.0",
                            alt="--loglevel argument"),
        "LOG_FILE": Option(deprecate_by="2.4", remove_by="3.0"),
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
        "LOG_LEVEL": Option("INFO", deprecate_by="2.4", remove_by="3.0"),
        "LOG_FILE": Option(deprecate_by="2.4", remove_by="3.0"),
    },
    "CELERYMON": {
        "LOG_LEVEL": Option("INFO", deprecate_by="2.4", remove_by="3.0"),
        "LOG_FILE": Option(deprecate_by="2.4", remove_by="3.0"),
        "LOG_FORMAT": Option(DEFAULT_LOG_FMT),
    },
    "EMAIL": {
        "HOST": Option("localhost"),
        "PORT": Option(25, type="int"),
        "HOST_USER": Option(None),
        "HOST_PASSWORD": Option(None),
        "TIMEOUT": Option(2, type="int"),
        "USE_SSL": Option(False, type="bool"),
        "USE_TLS": Option(False, type="bool"),
    },
    "SERVER_EMAIL": Option("celery@localhost"),
    "ADMINS": Option((), type="tuple"),
    "TT": {
        "HOST": Option(None, type="string"),
        "PORT": Option(None, type="int"),
    },
}


def flatten(d, ns=""):
    stack = deque([(ns, d)])
    while stack:
        name, space = stack.popleft()
        for key, value in space.iteritems():
            if isinstance(value, dict):
                stack.append((name + key + '_', value))
            else:
                yield name + key, value
DEFAULTS = dict((key, value.default) for key, value in flatten(NAMESPACES))


def find_deprecated_settings(source):
    from celery.utils import warn_deprecated
    for name, opt in flatten(NAMESPACES):
        if (opt.deprecate_by or opt.remove_by) and getattr(source, name, None):
            warn_deprecated(description="The %r setting" % (name, ),
                            deprecation=opt.deprecate_by,
                            removal=opt.remove_by,
                            alternative=opt.alt)


@memoize(maxsize=None)
def find(name, namespace="celery"):
    # - Try specified namespace first.
    namespace = namespace.upper()
    try:
        return namespace, name.upper(), NAMESPACES[namespace][name.upper()]
    except KeyError:
        # - Try all the other namespaces.
        for ns, keys in NAMESPACES.iteritems():
            if ns.upper() == name.upper():
                return None, ns, keys
            elif isinstance(keys, dict):
                try:
                    return ns, name.upper(), keys[name.upper()]
                except KeyError:
                    pass
    # - See if name is a qualname last.
    return None, name.upper(), DEFAULTS[name.upper()]
