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
from celery.utils.imports import symbol_by_name
from celery.utils.functional import memoize

UNKNOWN_BACKEND = """\
Unknown result backend: %r.  Did you spell that correctly? (%r)\
"""

BACKEND_ALIASES = {
    'amqp': 'celery.backends.amqp:AMQPBackend',
    'cache': 'celery.backends.cache:CacheBackend',
    'redis': 'celery.backends.redis:RedisBackend',
    'mongodb': 'celery.backends.mongodb:MongoBackend',
    'database': 'celery.backends.database:DatabaseBackend',
    'cassandra': 'celery.backends.cassandra:CassandraBackend',
    'disabled': 'celery.backends.base:DisabledBackend',
}

#: deprecated alias to ``current_app.backend``.
default_backend = Proxy(lambda: current_app.backend)


@memoize(100)
def get_backend_cls(backend=None, loader=None):
    """Get backend class by name/alias"""
    backend = backend or 'disabled'
    loader = loader or current_app.loader
    aliases = dict(BACKEND_ALIASES, **loader.override_backends)
    try:
        return symbol_by_name(backend, aliases)
    except ValueError, exc:
        raise ValueError, ValueError(UNKNOWN_BACKEND % (
            backend, exc)), sys.exc_info()[2]


def get_backend_by_url(backend=None, loader=None):
    url = None
    if backend and '://' in backend:
        url = backend
        backend, _, _, _, _, _, _ = _parse_url(url)
    return get_backend_cls(backend, loader), url
