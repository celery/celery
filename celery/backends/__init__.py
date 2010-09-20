from celery.app import app_or_default
from celery.utils import get_cls_by_name

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


def get_backend_cls(backend, loader=None):
    """Get backend class by name/alias"""
    loader = loader or app_or_default().loader
    if backend not in _backend_cache:
        aliases = dict(BACKEND_ALIASES, **loader.override_backends)
        _backend_cache[backend] = get_cls_by_name(backend, aliases)
    return _backend_cache[backend]
