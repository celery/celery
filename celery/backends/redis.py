"""Redis result store backend."""
from celery.utils.log import get_logger

from .base_keyval import BaseKeyValueBackend, BaseSentinelBackend

try:
    import redis.connection
except ImportError:
    redis = None

try:
    import redis.sentinel
except ImportError:
    pass

__all__ = ('RedisBackend', 'SentinelBackend')


class RedisBackend(BaseKeyValueBackend):
    """Redis task result store.

    It makes use of the following commands:
    GET, MGET, DEL, INCRBY, EXPIRE, SET, SETEX
    """
    #: :pypi:`redis` client module.
    backend = redis
    backend_module = redis
    backend_name = "redis"

    #: Maximal length of string value in Redis.
    #: 512 MB - https://redis.io/topics/data-types
    _MAX_STR_VALUE_SIZE = 536870912


if getattr(redis, "sentinel", None):
    class SentinelManagedSSLConnection(
            redis.sentinel.SentinelManagedConnection,
            redis.SSLConnection):
        """Connect to a Redis server using Sentinel + TLS.

        Use Sentinel to identify which Redis server is the current master
        to connect to and when connecting to the Master server, use an
        SSL Connection.
        """


class SentinelBackend(BaseSentinelBackend, RedisBackend):
    """Redis sentinel task result store."""

    sentinel = getattr(redis, "sentinel", None)
    connection_class_ssl = SentinelManagedSSLConnection if sentinel else None

