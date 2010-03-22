import importlib

from billiard.utils.functional import curry
from carrot.utils import rpartition

from celery import conf

BACKEND_ALIASES = {
    "amqp": "celery.backends.amqp.AMQPBackend",
    "database": "celery.backends.database.DatabaseBackend",
    "db": "celery.backends.database.DatabaseBackend",
    "redis": "celery.backends.pyredis.RedisBackend",
    "cache": "celery.backends.cache.CacheBackend",
    "mongodb": "celery.backends.mongodb.MongoBackend",
    "tyrant": "celery.backends.tyrant.TyrantBackend",
}

_backend_cache = {}


def resolve_backend(backend):
    backend = BACKEND_ALIASES.get(backend, backend)
    backend_module_name, _, backend_cls_name = rpartition(backend, ".")
    return backend_module_name, backend_cls_name


def _get_backend_cls(backend):
    backend_module_name, backend_cls_name = resolve_backend(backend)
    backend_module = importlib.import_module(backend_module_name)
    return getattr(backend_module, backend_cls_name)


def get_backend_cls(backend):
    """Get backend class by name/alias"""
    if backend not in _backend_cache:
        _backend_cache[backend] = _get_backend_cls(backend)
    return _backend_cache[backend]


"""
.. function:: get_default_backend_cls()

    Get the backend class specified in :setting:`CELERY_RESULT_BACKEND`.

"""
get_default_backend_cls = curry(get_backend_cls, conf.RESULT_BACKEND)


"""
.. class:: DefaultBackend

    The default backend class used for storing task results and status,
    specified in :setting:`CELERY_RESULT_BACKEND`.

"""
DefaultBackend = get_default_backend_cls()

"""
.. data:: default_backend

    An instance of :class:`DefaultBackend`.

"""
default_backend = DefaultBackend()
