from datetime import timedelta

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
    """Redis based task backend store.

    .. attribute:: redis_host

        The hostname to the Redis server.

    .. attribute:: redis_port

        The port to the Redis server.

        Raises :class:`celery.exceptions.ImproperlyConfigured` if
        the :setting:`REDIS_HOST` or :setting:`REDIS_PORT` settings is not set.

    """
    redis = redis
    redis_host = "localhost"
    redis_port = 6379
    redis_db = 0
    redis_password = None

    def __init__(self, redis_host=None, redis_port=None, redis_db=None,
            redis_password=None,
            expires=None, **kwargs):
        super(RedisBackend, self).__init__(**kwargs)
        if redis is None:
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

        if self.redis_port:
            self.redis_port = int(self.redis_port)
        if not self.redis_host or not self.redis_port:
            raise ImproperlyConfigured(
                "In order to use the Redis result store backend, you have to "
                "set the REDIS_HOST and REDIS_PORT settings")
        self._connection = None

    def open(self):
        """Get :class:`redis.Redis` instance with the current
        server configuration.

        The connection is then cached until you do an
        explicit :meth:`close`.

        """
        # connection overrides bool()
        if self._connection is None:
            self._connection = self.redis.Redis(host=self.redis_host,
                                                port=self.redis_port,
                                                db=self.redis_db,
                                                password=self.redis_password)
        return self._connection

    def close(self):
        """Close the connection to redis."""
        if self._connection is not None:
            self._connection.connection.disconnect()
            self._connection = None

    def process_cleanup(self):
        self.close()

    def get(self, key):
        return self.open().get(key)

    def set(self, key, value):
        r = self.open()
        r.set(key, value)
        if self.expires is not None:
            r.expire(key, self.expires)

    def delete(self, key):
        self.open().delete(key)
