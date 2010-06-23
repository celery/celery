import pylibmc

from carrot.utils import partition

from celery import conf
from celery.backends.base import KeyValueStoreBackend
from celery.utils import timeutils


class CacheBackend(KeyValueStoreBackend):
    Client = pylibmc.Client

    _client = None

    def __init__(self, expires=conf.TASK_RESULT_EXPIRES,
            backend=conf.CELERY_CACHE_BACKEND, options={}, **kwargs):
        super(CacheBackend, self).__init__(self, **kwargs)
        if isinstance(expires, timedelta):
            expires = timeutils.timedelta_seconds(expires)
        self.expires = expires
        self.options = dict(conf.CELERY_CACHE_BACKEND_OPTIONS, options)
        self.backend, _, servers = partition(backend, "://")
        self.servers = servers.split(";")
        self.client = pylibmc.Client(servers, **options)

        assert self.backend == "pylibmc"

    def get(self, key):
        return self.client.get(key)

    def set(self, key, value):
        return self.client.set(key, value, self.expires)

    @property
    def client(self):
        if self._client is None:
            self._client = self.Client(self.servers, **self.options)
        return self._client



