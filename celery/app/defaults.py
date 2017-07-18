# -*- coding: utf-8 -*-
"""Configuration introspection and defaults."""
from __future__ import absolute_import, unicode_literals
import sys
from collections import deque, namedtuple
from datetime import timedelta
from celery.five import items, keys, python_2_unicode_compatible
from celery.utils.functional import memoize
from celery.utils.serialization import strtobool

__all__ = ['Option', 'NAMESPACES', 'flatten', 'find']

is_jython = sys.platform.startswith('java')
is_pypy = hasattr(sys, 'pypy_version_info')

DEFAULT_POOL = 'prefork'
if is_jython:
    DEFAULT_POOL = 'solo'
elif is_pypy:
    if sys.pypy_version_info[0:3] < (1, 5, 0):
        DEFAULT_POOL = 'solo'
    else:
        DEFAULT_POOL = 'prefork'

DEFAULT_ACCEPT_CONTENT = ['json']
DEFAULT_PROCESS_LOG_FMT = """
    [%(asctime)s: %(levelname)s/%(processName)s] %(message)s
""".strip()
DEFAULT_TASK_LOG_FMT = """[%(asctime)s: %(levelname)s/%(processName)s] \
%(task_name)s[%(task_id)s]: %(message)s"""

OLD_NS = {'celery_{0}'}
OLD_NS_BEAT = {'celerybeat_{0}'}
OLD_NS_WORKER = {'celeryd_{0}'}

searchresult = namedtuple('searchresult', ('namespace', 'key', 'type'))


def Namespace(__old__=None, **options):
    if __old__ is not None:
        for key, opt in items(options):
            if not opt.old:
                opt.old = {o.format(key) for o in __old__}
    return options


def old_ns(ns):
    return {'{0}_{{0}}'.format(ns)}


@python_2_unicode_compatible
class Option(object):
    """Decribes a Celery configuration option."""

    alt = None
    deprecate_by = None
    remove_by = None
    old = set()
    typemap = dict(string=str, int=int, float=float, any=lambda v: v,
                   bool=strtobool, dict=dict, tuple=tuple)

    def __init__(self, default=None, *args, **kwargs):
        self.default = default
        self.type = kwargs.get('type') or 'string'
        for attr, value in items(kwargs):
            setattr(self, attr, value)

    def to_python(self, value):
        return self.typemap[self.type](value)

    def __repr__(self):
        return '<Option: type->{0} default->{1!r}>'.format(self.type,
                                                           self.default)


NAMESPACES = Namespace(
    accept_content=Option(DEFAULT_ACCEPT_CONTENT, type='list', old=OLD_NS),
    enable_utc=Option(True, type='bool'),
    imports=Option((), type='tuple', old=OLD_NS),
    include=Option((), type='tuple', old=OLD_NS),
    timezone=Option(type='string', old=OLD_NS),
    beat=Namespace(
        __old__=OLD_NS_BEAT,

        max_loop_interval=Option(0, type='float'),
        schedule=Option({}, type='dict'),
        scheduler=Option('celery.beat:PersistentScheduler'),
        schedule_filename=Option('celerybeat-schedule'),
        sync_every=Option(0, type='int'),
    ),
    broker=Namespace(
        url=Option(None, type='string'),
        read_url=Option(None, type='string'),
        write_url=Option(None, type='string'),
        transport=Option(type='string'),
        transport_options=Option({}, type='dict'),
        connection_timeout=Option(4, type='float'),
        connection_retry=Option(True, type='bool'),
        connection_max_retries=Option(100, type='int'),
        failover_strategy=Option(None, type='string'),
        heartbeat=Option(120, type='int'),
        heartbeat_checkrate=Option(3.0, type='int'),
        login_method=Option(None, type='string'),
        pool_limit=Option(10, type='int'),
        use_ssl=Option(False, type='bool'),

        host=Option(type='string'),
        port=Option(type='int'),
        user=Option(type='string'),
        password=Option(type='string'),
        vhost=Option(type='string'),
    ),
    cache=Namespace(
        __old__=old_ns('celery_cache'),

        backend=Option(),
        backend_options=Option({}, type='dict'),
    ),
    cassandra=Namespace(
        entry_ttl=Option(type='float'),
        keyspace=Option(type='string'),
        port=Option(type='string'),
        read_consistency=Option(type='string'),
        servers=Option(type='list'),
        table=Option(type='string'),
        write_consistency=Option(type='string'),
        auth_provider=Option(type='string'),
        auth_kwargs=Option(type='string'),
    ),
    control=Namespace(
        queue_ttl=Option(300.0, type='float'),
        queue_expires=Option(10.0, type='float'),
    ),
    couchbase=Namespace(
        __old__=old_ns('celery_couchbase'),

        backend_settings=Option(None, type='dict'),
    ),
    mongodb=Namespace(
        __old__=old_ns('celery_mongodb'),

        backend_settings=Option(type='dict'),
    ),
    event=Namespace(
        __old__=old_ns('celery_event'),

        queue_expires=Option(60.0, type='float'),
        queue_ttl=Option(5.0, type='float'),
        queue_prefix=Option('celeryev'),
        serializer=Option('json'),
    ),
    redis=Namespace(
        __old__=old_ns('celery_redis'),

        backend_use_ssl=Option(type='dict'),
        db=Option(type='int'),
        host=Option(type='string'),
        max_connections=Option(type='int'),
        password=Option(type='string'),
        port=Option(type='int'),
        socket_timeout=Option(120.0, type='float'),
        socket_connect_timeout=Option(None, type='float'),
    ),
    result=Namespace(
        __old__=old_ns('celery_result'),

        backend=Option(type='string'),
        cache_max=Option(
            -1,
            type='int', old={'celery_max_cached_results'},
        ),
        compression=Option(type='str'),
        exchange=Option('celeryresults'),
        exchange_type=Option('direct'),
        expires=Option(
            timedelta(days=1),
            type='float', old={'celery_task_result_expires'},
        ),
        persistent=Option(None, type='bool'),
        serializer=Option('json'),
    ),
    elasticsearch=Namespace(
        __old__=old_ns('celery_elasticsearch'),

        retry_on_timeout=Option(type='bool'),
        max_retries=Option(type='int'),
        timeout=Option(type='float'),
    ),
    riak=Namespace(
        __old__=old_ns('celery_riak'),

        backend_settings=Option(type='dict'),
    ),
    security=Namespace(
        __old__=old_ns('celery_security'),

        certificate=Option(type='string'),
        cert_store=Option(type='string'),
        key=Option(type='string'),
    ),
    database=Namespace(
        url=Option(old={'celery_result_dburi'}),
        engine_options=Option(
            type='dict', old={'celery_result_engine_options'},
        ),
        short_lived_sessions=Option(
            False, type='bool', old={'celery_result_db_short_lived_sessions'},
        ),
        table_names=Option(type='dict', old={'celery_result_db_tablenames'}),
    ),
    task=Namespace(
        __old__=OLD_NS,
        acks_late=Option(False, type='bool'),
        always_eager=Option(False, type='bool'),
        annotations=Option(type='any'),
        compression=Option(type='string', old={'celery_message_compression'}),
        create_missing_queues=Option(True, type='bool'),
        default_delivery_mode=Option(2, type='string'),
        default_queue=Option('celery'),
        default_exchange=Option(None, type='string'),  # taken from queue
        default_exchange_type=Option('direct'),
        default_routing_key=Option(None, type='string'),  # taken from queue
        default_rate_limit=Option(type='string'),
        eager_propagates=Option(
            False, type='bool', old={'celery_eager_propagates_exceptions'},
        ),
        ignore_result=Option(False, type='bool'),
        protocol=Option(2, type='int', old={'celery_task_protocol'}),
        publish_retry=Option(
            True, type='bool', old={'celery_task_publish_retry'},
        ),
        publish_retry_policy=Option(
            {'max_retries': 3,
             'interval_start': 0,
             'interval_max': 1,
             'interval_step': 0.2},
            type='dict', old={'celery_task_publish_retry_policy'},
        ),
        queues=Option(type='dict'),
        queue_ha_policy=Option(None, type='string'),
        queue_max_priority=Option(None, type='int'),
        reject_on_worker_lost=Option(type='bool'),
        remote_tracebacks=Option(False, type='bool'),
        routes=Option(type='any'),
        send_sent_event=Option(
            False, type='bool', old={'celery_send_task_sent_event'},
        ),
        serializer=Option('json', old={'celery_task_serializer'}),
        soft_time_limit=Option(
            type='float', old={'celeryd_task_soft_time_limit'},
        ),
        time_limit=Option(
            type='float', old={'celeryd_task_time_limit'},
        ),
        store_errors_even_if_ignored=Option(False, type='bool'),
        track_started=Option(False, type='bool'),
    ),
    worker=Namespace(
        __old__=OLD_NS_WORKER,
        agent=Option(None, type='string'),
        autoscaler=Option('celery.worker.autoscale:Autoscaler'),
        concurrency=Option(0, type='int'),
        consumer=Option('celery.worker.consumer:Consumer', type='string'),
        direct=Option(False, type='bool', old={'celery_worker_direct'}),
        disable_rate_limits=Option(
            False, type='bool', old={'celery_disable_rate_limits'},
        ),
        enable_remote_control=Option(
            True, type='bool', old={'celery_enable_remote_control'},
        ),
        hijack_root_logger=Option(True, type='bool'),
        log_color=Option(type='bool'),
        log_format=Option(DEFAULT_PROCESS_LOG_FMT),
        lost_wait=Option(10.0, type='float', old={'celeryd_worker_lost_wait'}),
        max_memory_per_child=Option(type='int'),
        max_tasks_per_child=Option(type='int'),
        pool=Option(DEFAULT_POOL),
        pool_putlocks=Option(True, type='bool'),
        pool_restarts=Option(False, type='bool'),
        prefetch_multiplier=Option(4, type='int'),
        redirect_stdouts=Option(
            True, type='bool', old={'celery_redirect_stdouts'},
        ),
        redirect_stdouts_level=Option(
            'WARNING', old={'celery_redirect_stdouts_level'},
        ),
        send_task_events=Option(
            False, type='bool', old={'celery_send_events'},
        ),
        state_db=Option(),
        task_log_format=Option(DEFAULT_TASK_LOG_FMT),
        timer=Option(type='string'),
        timer_precision=Option(1.0, type='float'),
    ),
)


def _flatten_keys(ns, key, opt):
    return [(ns + key, opt)]


def _to_compat(ns, key, opt):
    if opt.old:
        return [
            (oldkey.format(key).upper(), ns + key, opt)
            for oldkey in opt.old
        ]
    return [((ns + key).upper(), ns + key, opt)]


def flatten(d, root='', keyfilter=_flatten_keys):
    """Flatten settings."""
    stack = deque([(root, d)])
    while stack:
        ns, options = stack.popleft()
        for key, opt in items(options):
            if isinstance(opt, dict):
                stack.append((ns + key + '_', opt))
            else:
                for ret in keyfilter(ns, key, opt):
                    yield ret


DEFAULTS = {
    key: opt.default for key, opt in flatten(NAMESPACES)
}
__compat = list(flatten(NAMESPACES, keyfilter=_to_compat))
_OLD_DEFAULTS = {old_key: opt.default for old_key, _, opt in __compat}
_TO_OLD_KEY = {new_key: old_key for old_key, new_key, _ in __compat}
_TO_NEW_KEY = {old_key: new_key for old_key, new_key, _ in __compat}
__compat = None

SETTING_KEYS = set(keys(DEFAULTS))
_OLD_SETTING_KEYS = set(keys(_TO_NEW_KEY))


def find_deprecated_settings(source):  # pragma: no cover
    from celery.utils import deprecated
    for name, opt in flatten(NAMESPACES):
        if (opt.deprecate_by or opt.remove_by) and getattr(source, name, None):
            deprecated.warn(description='The {0!r} setting'.format(name),
                            deprecation=opt.deprecate_by,
                            removal=opt.remove_by,
                            alternative='Use the {0.alt} instead'.format(opt))
    return source


@memoize(maxsize=None)
def find(name, namespace='celery'):
    """Find setting by name."""
    # - Try specified name-space first.
    namespace = namespace.lower()
    try:
        return searchresult(
            namespace, name.lower(), NAMESPACES[namespace][name.lower()],
        )
    except KeyError:
        # - Try all the other namespaces.
        for ns, opts in items(NAMESPACES):
            if ns.lower() == name.lower():
                return searchresult(None, ns, opts)
            elif isinstance(opts, dict):
                try:
                    return searchresult(ns, name.lower(), opts[name.lower()])
                except KeyError:
                    pass
    # - See if name is a qualname last.
    return searchresult(None, name.lower(), DEFAULTS[name.lower()])
