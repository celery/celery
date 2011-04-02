from datetime import timedelta

from kombu.utils import partition, cached_property

from celery.backends.base import KeyValueStoreBackend
from celery.exceptions import ImproperlyConfigured
from celery.utils import timeutils
from celery.datastructures import LocalCache

_imp = [None]


def import_best_memcache():
    if _imp[0] is None:
        is_pylibmc = False
        try:
            import pylibmc as memcache
            is_pylibmc = True
        except ImportError:
            try:
                import memcache
            except ImportError:
                raise ImproperlyConfigured(
                        "Memcached backend requires either the 'pylibmc' "
                        "or 'memcache' library")
        _imp[0] = is_pylibmc, memcache
    return _imp[0]


def get_best_memcache(*args, **kwargs):
    behaviors = kwargs.pop("behaviors", None)
    is_pylibmc, memcache = import_best_memcache()
    client = memcache.Client(*args, **kwargs)
    if is_pylibmc and behaviors is not None:
        client.behaviors = behaviors
    return client


class DummyClient(object):

    def __init__(self, *args, **kwargs):
        self.cache = LocalCache(5000)

    def get(self, key, *args, **kwargs):
        return self.cache.get(key)

    def set(self, key, value, *args, **kwargs):
        self.cache[key] = value

    def delete(self, key, *args, **kwargs):
        self.cache.pop(key, None)


backends = {"memcache": lambda: get_best_memcache,
            "memcached": lambda: get_best_memcache,
            "pylibmc": lambda: get_best_memcache,
            "memory": lambda: DummyClient}


class CacheBackend(KeyValueStoreBackend):

    def __init__(self, expires=None, backend=None, options={}, **kwargs):
        super(CacheBackend, self).__init__(self, **kwargs)

        self.expires = expires or self.app.conf.CELERY_TASK_RESULT_EXPIRES
        if isinstance(self.expires, timedelta):
            self.expires = timeutils.timedelta_seconds(self.expires)
        self.options = dict(self.app.conf.CELERY_CACHE_BACKEND_OPTIONS,
                            **options)

        backend = backend or self.app.conf.CELERY_CACHE_BACKEND
        self.expires = int(self.expires)
        self.backend, _, servers = partition(backend, "://")
        self.servers = servers.rstrip('/').split(";")
        try:
            self.Client = backends[self.backend]()
        except KeyError:
            raise ImproperlyConfigured(
                    "Unknown cache backend: %s. Please use one of the "
                    "following backends: %s" % (self.backend,
                                                ", ".join(backends.keys())))

    def get(self, key):
        return self.client.get(key)

    def set(self, key, value):
        return self.client.set(key, value, self.expires)

    def delete(self, key):
        return self.client.delete(key)

    @cached_property
    def client(self):
        return self.Client(self.servers, **self.options)
