"""celery.backends.tyrant"""
from django.core.exceptions import ImproperlyConfigured
from celery.backends.base import KeyValueStoreBackend
try:
    import redis
except ImportError:
    redis = None

from celery.loaders import settings


class RedisBackend(KeyValueStoreBackend):
    """Redis based task backend store.

    .. attribute:: redis_host

        The hostname to the Redis server.

    .. attribute:: redis_port

        The port to the Redis server.

        Raises :class:`django.core.exceptions.ImproperlyConfigured` if
        :setting:`REDIS_HOST` or :setting:`REDIS_PORT` is not set.

    """
    redis_host = None
    redis_port = None
    redis_db = "celery_results"
    redis_timeout = None
    redis_connect_retry = None

    def __init__(self, redis_host=None, redis_port=None, redis_db=None,
            redis_timeout=None,
            redis_connect_retry=None,
            redis_connect_timeout=None):
        if not redis:
            raise ImproperlyConfigured(
                    "You need to install the redis library in order to use "
                  + "Redis result store backend.")
        self.redis_host = redis_host or \
                            getattr(settings, "REDIS_HOST", self.redis_host)
        self.redis_port = redis_port or \
                            getattr(settings, "REDIS_PORT", self.redis_port)
        self.redis_db = redis_db or \
                            getattr(settings, "REDIS_DB", self.redis_db)
        self.redis_timeout = redis_timeout or \
                            getattr(settings, "REDIS_TIMEOUT",
                                    self.redis_timeout)
        self.redis_connect_retry = redis_connect_retry or \
                            getattr(settings, "REDIS_CONNECT_RETRY",
                                    self.redis_connect_retry)
        if self.redis_port:
            self.redis_port = int(self.redis_port)
        if not self.redis_host or not self.redis_port:
            raise ImproperlyConfigured(
                "In order to use the Redis result store backend, you have to "
                "set the REDIS_HOST and REDIS_PORT settings")
        super(RedisBackend, self).__init__()
        self._connection = None

    def open(self):
        """Get :class:`redis.Redis`` instance with the current
        server configuration.

        The connection is then cached until you do an
        explicit :meth:`close`.

        """
        # connection overrides bool()
        if self._connection is None:
            self._connection = redis.Redis(host=self.redis_host,
                                    port=self.redis_port,
                                    db=self.redis_db,
                                    timeout=self.redis_timeout,
                                    connect_retry=self.redis_connect_retry)
        return self._connection

    def close(self):
        """Close the redis connection and remove the cache."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def process_cleanup(self):
        self.close()

    def get(self, key):
        return self.open().get(key)

    def set(self, key, value):
        self.open().set(key, value)
