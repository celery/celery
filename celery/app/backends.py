# -*- coding: utf-8 -*-
"""Backend selection."""
<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

import sys
import types

from celery._state import current_app
from celery.exceptions import ImproperlyConfigured
from celery.five import reraise
=======
import sys
import types
from typing import Mapping, Tuple, Union
from celery.exceptions import ImproperlyConfigured
from celery._state import current_app
from celery.types import LoaderT
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from celery.utils.imports import load_extension_class_names, symbol_by_name

__all__ = ('by_name', 'by_url')

UNKNOWN_BACKEND = """
Unknown result backend: {0!r}.  Did you spell that correctly? ({1!r})
"""

BACKEND_ALIASES: Mapping[str, str] = {
    'amqp': 'celery.backends.amqp:AMQPBackend',
    'rpc': 'celery.backends.rpc.RPCBackend',
    'cache': 'celery.backends.cache:CacheBackend',
    'redis': 'celery.backends.redis:RedisBackend',
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


def by_name(backend: Union[str, type] = None,
            loader: LoaderT = None,
            extension_namespace: str = 'celery.result_backends') -> type:
    """Get backend class by name/alias."""
    backend = backend or 'disabled'
    loader = loader or current_app.loader
    aliases = dict(BACKEND_ALIASES, **loader.override_backends)
    aliases.update(
        load_extension_class_names(extension_namespace) or {})
    try:
        cls = symbol_by_name(backend, aliases)
    except ValueError as exc:
        raise ImproperlyConfigured(UNKNOWN_BACKEND.format(
            backend, exc)).with_traceback(sys.exc_info()[2])
    if isinstance(cls, types.ModuleType):
        raise ImproperlyConfigured(UNKNOWN_BACKEND.strip().format(
            backend, 'is a Python module, not a backend class.'))
    return cls


def by_url(backend: Union[str, type] = None,
           loader: LoaderT = None) -> Tuple[type, str]:
    """Get backend class by URL."""
    url = None
    if backend and '://' in backend:
        url = backend
        scheme, _, _ = url.partition('://')
        if '+' in scheme:
            backend, url = url.split('+', 1)
        else:
            backend = scheme
    return by_name(backend, loader), url
