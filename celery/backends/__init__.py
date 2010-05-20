from billiard.utils.functional import curry

from celery import conf
from celery.utils import get_cls_by_name
from celery.loaders import current_loader

BACKEND_ALIASES = {
    "amqp": "celery.backends.amqp.AMQPBackend",
    "redis": "celery.backends.pyredis.RedisBackend",
    "mongodb": "celery.backends.mongodb.MongoBackend",
    "tyrant": "celery.backends.tyrant.TyrantBackend",
    "database": "celery.backends.database.DatabaseBackend",
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
