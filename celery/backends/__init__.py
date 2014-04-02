# -*- coding: utf-8 -*-
"""
    celery.backends
    ~~~~~~~~~~~~~~~

    Backend abstract factory (...did I just say that?) and alias definitions.

"""
from __future__ import absolute_import

import sys

from kombu.utils.url import _parse_url

from celery.local import Proxy
from celery._state import current_app
from celery.five import reraise
from celery.utils.imports import symbol_by_name

__all__ = ['get_backend_cls', 'get_backend_by_url']

UNKNOWN_BACKEND = """\
Unknown result backend: {0!r}.  Did you spell that correctly? ({1!r})\
"""

BACKEND_ALIASES = {
    'amqp': 'celery.backends.amqp:AMQPBackend',
    'rpc': 'celery.backends.rpc.RPCBackend',
    'cache': 'celery.backends.cache:CacheBackend',
    'redis': 'celery.backends.redis:RedisBackend',
    'mongodb': 'celery.backends.mongodb:MongoBackend',
    'db': 'celery.backends.database:DatabaseBackend',
    'database': 'celery.backends.database:DatabaseBackend',
    'cassandra': 'celery.backends.cassandra:CassandraBackend',
    'couchbase': 'celery.backends.couchbase:CouchBaseBackend',
    'disabled': 'celery.backends.base:DisabledBackend',
}

#: deprecated alias to ``current_app.backend``.
default_backend = Proxy(lambda: current_app.backend)


def get_backend_cls(backend=None, loader=None):
    """Get backend class by name/alias"""
    backend = backend or 'disabled'
    loader = loader or current_app.loader
    aliases = dict(BACKEND_ALIASES, **loader.override_backends)
    try:
        return symbol_by_name(backend, aliases)
    except ValueError as exc:
        reraise(ValueError, ValueError(UNKNOWN_BACKEND.format(
            backend, exc)), sys.exc_info()[2])


def get_backend_by_url(backend=None, loader=None):
    url = None
    if backend and '://' in backend:
        url = backend
        if '+' in url[:url.index('://')]:
            backend, url = url.split('+', 1)
        else:
            backend, _, _, _, _, _, _ = _parse_url(url)
    return get_backend_cls(backend, loader), url
