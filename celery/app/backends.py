# -*- coding: utf-8 -*-
"""Backend selection."""
from __future__ import absolute_import, unicode_literals

import sys
import types

from celery._state import current_app
from celery.exceptions import ImproperlyConfigured
from celery.five import reraise
from celery.utils.imports import load_extension_class_names, symbol_by_name

__all__ = ('by_name', 'by_url')

UNKNOWN_BACKEND = """
Unknown result backend: {0!r}.  Did you spell that correctly? ({1!r})
"""

BACKEND_ALIASES = {
    'amqp': 'celery.backends.amqp:AMQPBackend',
    'rpc': 'celery.backends.rpc.RPCBackend',
    'cache': 'celery.backends.cache:CacheBackend',
    'redis': 'celery.backends.redis:RedisBackend',
    'rediss': 'celery.backends.redis:RedisBackend',
    'sentinel': 'celery.backends.redis:SentinelBackend',
    'mongodb': 'celery.backends.mongodb:MongoBackend',
    'db': 'celery.backends.database:DatabaseBackend',
    'database': 'celery.backends.database:DatabaseBackend',
    'elasticsearch': 'celery.backends.elasticsearch:ElasticsearchBackend',
    'cassandra': 'celery.backends.cassandra:CassandraBackend',
    'couchbase': 'celery.backends.couchbase:CouchbaseBackend',
    'couchdb': 'celery.backends.couchdb:CouchBackend',
    'riak': 'celery.backends.riak:RiakBackend',
    'file': 'celery.backends.filesystem:FilesystemBackend',
    'disabled': 'celery.backends.base:DisabledBackend',
    'consul': 'celery.backends.consul:ConsulBackend',
    'dynamodb': 'celery.backends.dynamodb:DynamoDBBackend',
}


def by_name(backend=None, loader=None,
            extension_namespace='celery.result_backends'):
    """Get backend class by name/alias."""
    backend = backend or 'disabled'
    loader = loader or current_app.loader
    aliases = dict(BACKEND_ALIASES, **loader.override_backends)
    aliases.update(
        load_extension_class_names(extension_namespace) or {})
    try:
        cls = symbol_by_name(backend, aliases)
    except ValueError as exc:
        reraise(ImproperlyConfigured, ImproperlyConfigured(
            UNKNOWN_BACKEND.strip().format(backend, exc)), sys.exc_info()[2])
    if isinstance(cls, types.ModuleType):
        raise ImproperlyConfigured(UNKNOWN_BACKEND.strip().format(
            backend, 'is a Python module, not a backend class.'))
    return cls


def by_url(backend=None, loader=None):
    """Get backend class by URL.

    Return a class object and the url to provide to the backend when it is
    created.
    """
    orig_backend = backend
    url = None
    if backend and '://' in backend:
        # There are two types of urls we'll see:
        # * <backend>+<backend-defined-ext>:// e.g., mongodb+srv://
        # * <backend>+<original_scheme>:// e.g., cache+memory://...,
        #   db+postgresql+psycopg2://..., etc.
        url = backend
        scheme, _, _ = url.partition('://')
        if '+' in scheme:
            backend, url = url.split('+', 1)
        else:
            backend = scheme
    cls = by_name(backend, loader)

    if getattr(cls, 'expects_url_munging', True) or url is None:
        return cls, url
    else:
        return cls, orig_backend
