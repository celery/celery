from datetime import timedelta

from carrot.utils import partition

from celery import conf
from celery.backends.base import KeyValueStoreBackend
from celery.exceptions import ImproperlyConfigured
from celery.utils import timeutils
from celery.datastructures import LocalCache


def get_best_memcache(*args, **kwargs):
    try:
        import pylibmc as memcache
    except ImportError:
        try:
            import memcache
        except ImportError:
            raise ImproperlyConfigured("Memcached backend requires either "
                                       "the 'memcache' or 'pylibmc' library")
    return memcache.Client(*args, **kwargs)


class DummyClient(object):

    def __init__(self, *args, **kwargs):
        self.cache = LocalCache(5000)

    def get(self, key, *args, **kwargs):
        return self.cache.get(key)

    def set(self, key, value, *args, **kwargs):
        self.cache[key] = value


backends = {"memcache": get_best_memcache,
            "memcached": get_best_memcache,
            "pylibmc": get_best_memcache,
            "memory": DummyClient}

class CacheBackend(KeyValueStoreBackend):
    _client = None

    def __init__(self, expires=conf.TASK_RESULT_EXPIRES,
            backend=conf.CACHE_BACKEND, options={}, **kwargs):
        super(CacheBackend, self).__init__(self, **kwargs)
        if isinstance(expires, timedelta):
            expires = timeutils.timedelta_seconds(expires)
        self.expires = expires
        self.options = dict(conf.CACHE_BACKEND_OPTIONS, **options)
        self.backend, _, servers = partition(backend, "://")
        self.servers = servers.split(";")
        try:
            self.Client = backends[self.backend]
        except KeyError:
            raise ImproperlyConfigured(
                    "Unknown cache backend: %s. Please use one of the "
                    "following backends: %s" % (self.backend,
                                                ", ".join(backends.keys())))

    def get(self, key):
        return self.client.get(key)

    def set(self, key, value):
        return self.client.set(key, value, self.expires)

    @property
    def client(self):
        if self._client is None:
            self._client = self.Client(self.servers, **self.options)
        return self._client
