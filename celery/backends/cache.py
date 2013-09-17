# -*- coding: utf-8 -*-
"""
    celery.backends.cache
    ~~~~~~~~~~~~~~~~~~~~~

    Memcache and in-memory cache result backend.

"""
from __future__ import absolute_import

from kombu.utils import cached_property

from celery.exceptions import ImproperlyConfigured
from celery.utils.functional import LRUCache

from .base import KeyValueStoreBackend

__all__ = ['CacheBackend']

_imp = [None]

REQUIRES_BACKEND = """\
The memcached backend requires either pylibmc or python-memcached.\
"""

UNKNOWN_BACKEND = """\
The cache backend {0!r} is unknown,
Please use one of the following backends instead: {1}\
"""


def import_best_memcache():
    if _imp[0] is None:
        is_pylibmc = False
        try:
            import pylibmc as memcache
            is_pylibmc = True
        except ImportError:
            try:
                import memcache  # noqa
            except ImportError:
                raise ImproperlyConfigured(REQUIRES_BACKEND)
        _imp[0] = (is_pylibmc, memcache)
    return _imp[0]


def get_best_memcache(*args, **kwargs):
    behaviors = kwargs.pop('behaviors', None)
    is_pylibmc, memcache = import_best_memcache()
    client = memcache.Client(*args, **kwargs)
    if is_pylibmc and behaviors is not None:
        client.behaviors = behaviors
    return client


class DummyClient(object):

    def __init__(self, *args, **kwargs):
        self.cache = LRUCache(limit=5000)

    def get(self, key, *args, **kwargs):
        return self.cache.get(key)

    def get_multi(self, keys):
        cache = self.cache
        return dict((k, cache[k]) for k in keys if k in cache)

    def set(self, key, value, *args, **kwargs):
        self.cache[key] = value

    def delete(self, key, *args, **kwargs):
        self.cache.pop(key, None)

    def incr(self, key, delta=1):
        return self.cache.incr(key, delta)


backends = {'memcache': lambda: get_best_memcache,
            'memcached': lambda: get_best_memcache,
            'pylibmc': lambda: get_best_memcache,
            'memory': lambda: DummyClient}


class CacheBackend(KeyValueStoreBackend):
    servers = None
    supports_autoexpire = True
    supports_native_join = True
    implements_incr = True

    def __init__(self, app, expires=None, backend=None,
                 options={}, url=None, **kwargs):
        super(CacheBackend, self).__init__(app, **kwargs)

        self.options = dict(self.app.conf.CELERY_CACHE_BACKEND_OPTIONS,
                            **options)

        self.backend = url or backend or self.app.conf.CELERY_CACHE_BACKEND
        if self.backend:
            self.backend, _, servers = self.backend.partition('://')
            self.servers = servers.rstrip('/').split(';')
        self.expires = self.prepare_expires(expires, type=int)
        try:
            self.Client = backends[self.backend]()
        except KeyError:
            raise ImproperlyConfigured(UNKNOWN_BACKEND.format(
                self.backend, ', '.join(backends)))

    def get(self, key):
        return self.client.get(key)

    def mget(self, keys):
        return self.client.get_multi(keys)

    def set(self, key, value):
        return self.client.set(key, value, self.expires)

    def delete(self, key):
        return self.client.delete(key)

    def on_chord_apply(self, group_id, body, result=None, **kwargs):
        self.client.set(self.get_key_for_chord(group_id), '0', time=86400)
        self.save_group(group_id, self.app.GroupResult(group_id, result))

    def incr(self, key):
        return self.client.incr(key)

    @cached_property
    def client(self):
        return self.Client(self.servers, **self.options)

    def __reduce__(self, args=(), kwargs={}):
        servers = ';'.join(self.servers)
        backend = '{0}://{1}/'.format(self.backend, servers)
        kwargs.update(
            dict(backend=backend,
                 expires=self.expires,
                 options=self.options))
        return super(CacheBackend, self).__reduce__(args, kwargs)
