import warnings

from django.core.exceptions import ImproperlyConfigured

from celery.backends.base import KeyValueStoreBackend
from celery.loaders import load_settings

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

        Raises :class:`django.core.exceptions.ImproperlyConfigured` if
        :setting:`REDIS_HOST` or :setting:`REDIS_PORT` is not set.

    """
    redis_host = "localhost"
    redis_port = 6379
    redis_db = "celery_results"
    redis_password = None
    redis_timeout = None
    redis_connect_retry = None

    deprecated_settings = frozenset(["REDIS_TIMEOUT",
                                     "REDIS_CONNECT_RETRY"])

    def __init__(self, redis_host=None, redis_port=None, redis_db=None,
            redis_timeout=None,
            redis_password=None,
            redis_connect_retry=None,
            redis_connect_timeout=None):
        if redis is None:
            raise ImproperlyConfigured(
                    "You need to install the redis library in order to use "
                  + "Redis result store backend.")

        settings = load_settings()
        self.redis_host = redis_host or \
                            getattr(settings, "REDIS_HOST", self.redis_host)
        self.redis_port = redis_port or \
                            getattr(settings, "REDIS_PORT", self.redis_port)
        self.redis_db = redis_db or \
                            getattr(settings, "REDIS_DB", self.redis_db)
        self.redis_password = redis_password or \
                            getattr(settings, "REDIS_PASSWORD",
                                    self.redis_password)

        for setting_name in self.deprecated_settings:
            if getattr(settings, setting_name, None) is not None:
                warnings.warn(
                    "The setting '%s' is no longer supported by the "
                    "python Redis client!" % setting_name.upper(),
                    DeprecationWarning)

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
                                    password=self.redis_password)
        return self._connection

    def close(self):
        """Close the connection to redis."""
        if self._connection is not None:
            self._connection.save()
            self._connection.connection.disconnect()
            self._connection = None

    def process_cleanup(self):
        self.close()

    def get(self, key):
        return self.open().get(key)

    def set(self, key, value):
        self.open().set(key, value)
