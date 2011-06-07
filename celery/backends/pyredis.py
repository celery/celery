from datetime import timedelta

from kombu.utils import cached_property

from celery.backends.base import KeyValueStoreBackend
from celery.exceptions import ImproperlyConfigured
from celery.utils import timeutils

try:
    import redis
    from redis.exceptions import ConnectionError
except ImportError:
    redis = None
    ConnectionError = None


class RedisBackend(KeyValueStoreBackend):
    """Redis task result store."""

    #: redis-py client module.
    redis = redis

    #: default Redis server hostname (`localhost`).
    redis_host = "localhost"

    #: default Redis server port (6379)
    redis_port = 6379
    redis_db = 0

    #: default Redis password (:const:`None`)
    redis_password = None

    def __init__(self, redis_host=None, redis_port=None, redis_db=None,
            redis_password=None,
            expires=None, **kwargs):
        super(RedisBackend, self).__init__(**kwargs)
        if self.redis is None:
            raise ImproperlyConfigured(
                    "You need to install the redis library in order to use "
                  + "Redis result store backend.")

        self.redis_host = (redis_host or
                           self.app.conf.get("REDIS_HOST") or
                           self.redis_host)
        self.redis_port = (redis_port or
                           self.app.conf.get("REDIS_PORT") or
                           self.redis_port)
        self.redis_db = (redis_db or
                         self.app.conf.get("REDIS_DB") or
                         self.redis_db)
        self.redis_password = (redis_password or
                               self.app.conf.get("REDIS_PASSWORD") or
                               self.redis_password)
        self.expires = expires
        if self.expires is None:
            self.expires = self.app.conf.CELERY_TASK_RESULT_EXPIRES
        if isinstance(self.expires, timedelta):
            self.expires = timeutils.timedelta_seconds(self.expires)
        if self.expires is not None:
            self.expires = int(self.expires)
        self.redis_port = int(self.redis_port)

    def get(self, key):
        return self.client.get(key)

    def set(self, key, value):
        client = self.client
        client.set(key, value)
        if self.expires is not None:
            client.expire(key, self.expires)

    def delete(self, key):
        self.client.delete(key)

    def process_cleanup(self):
        pass

    @cached_property
    def client(self):
        return self.redis.Redis(host=self.redis_host,
                                port=self.redis_port,
                                db=self.redis_db,
                                password=self.redis_password)
