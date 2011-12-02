# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .. import current_app
from ..local import Proxy
from ..utils import get_cls_by_name
from ..utils.functional import memoize

BACKEND_ALIASES = {
    "amqp": "celery.backends.amqp.AMQPBackend",
    "cache": "celery.backends.cache.CacheBackend",
    "redis": "celery.backends.redis.RedisBackend",
    "mongodb": "celery.backends.mongodb.MongoBackend",
    "tyrant": "celery.backends.tyrant.TyrantBackend",
    "database": "celery.backends.database.DatabaseBackend",
    "cassandra": "celery.backends.cassandra.CassandraBackend",
    "disabled": "celery.backends.base.DisabledBackend",
}


@memoize(100)
def get_backend_cls(backend=None, loader=None):
    """Get backend class by name/alias"""
    backend = backend or "disabled"
    loader = loader or current_app.loader
    aliases = dict(BACKEND_ALIASES, **loader.override_backends)
    try:
        return get_cls_by_name(backend, aliases)
    except ValueError, exc:
        raise ValueError("Unknown result backend: %r.  "
                         "Did you spell it correctly?  (%s)" % (backend, exc))


# deprecate this.
default_backend = Proxy(lambda: current_app.backend)
