from datetime import timedelta

from carrot.utils import partition

from celery import conf
from celery.backends.base import KeyValueStoreBackend
from celery.exceptions import ImproperlyConfigured
from celery.utils import timeutils

try:
    import pylibmc as memcache
except ImportError:
    try:
        import memcache
    except ImportError:
        raise ImproperlyConfigured("Memcached backend requires either "
                                   "the 'memcache' or 'pylibmc' library")


class CacheBackend(KeyValueStoreBackend):
    Client = memcache.Client

    _client = None

    def __init__(self, expires=conf.TASK_RESULT_EXPIRES,
            backend=conf.CACHE_BACKEND, options={}, **kwargs):
        super(CacheBackend, self).__init__(self, **kwargs)
        if isinstance(expires, timedelta):
            expires = timeutils.timedelta_seconds(expires)
        self.expires = expires
        self.options = dict(conf.CACHE_BACKEND_OPTIONS, options)
        self.backend, _, servers = partition(backend, "://")
        self.servers = servers.split(";")

    def get(self, key):
        return self.client.get(key)

    def set(self, key, value):
        return self.client.set(key, value, self.expires)

    @property
    def client(self):
        if self._client is None:
            self._client = self.Client(self.servers, **self.options)
        return self._client



