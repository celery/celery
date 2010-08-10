from celery import conf
from celery.utils import get_cls_by_name
from celery.utils.functional import curry
from celery.loaders import current_loader

BACKEND_ALIASES = {
    "amqp": "celery.backends.amqp.AMQPBackend",
    "cache": "celery.backends.cache.CacheBackend",
    "redis": "celery.backends.pyredis.RedisBackend",
    "mongodb": "celery.backends.mongodb.MongoBackend",
    "tyrant": "celery.backends.tyrant.TyrantBackend",
    "database": "celery.backends.database.DatabaseBackend",
    "cassandra": "celery.backends.cassandra.CassandraBackend",
}

_backend_cache = {}


def get_backend_cls(backend):
    """Get backend class by name/alias"""
    if backend not in _backend_cache:
        aliases = dict(BACKEND_ALIASES, **current_loader().override_backends)
        _backend_cache[backend] = get_cls_by_name(backend, aliases)
    return _backend_cache[backend]


"""
.. function:: get_default_backend_cls()

    Get the backend class specified in the ``CELERY_RESULT_BACKEND`` setting.

"""
get_default_backend_cls = curry(get_backend_cls, conf.RESULT_BACKEND)


"""
.. class:: DefaultBackend

    The default backend class used for storing task results and status,
    specified in the ``CELERY_RESULT_BACKEND`` setting.

"""
DefaultBackend = get_default_backend_cls()

"""
.. data:: default_backend

    An instance of :class:`DefaultBackend`.

"""
default_backend = DefaultBackend()
