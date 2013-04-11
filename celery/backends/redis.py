# -*- coding: utf-8 -*-
"""
    celery.backends.redis
    ~~~~~~~~~~~~~~~~~~~~~

    Redis result store backend.

"""
from __future__ import absolute_import

from kombu.utils import cached_property
from kombu.utils.url import _parse_url

from celery.exceptions import ImproperlyConfigured

from .base import KeyValueStoreBackend

try:
    import redis
    from redis.exceptions import ConnectionError
except ImportError:         # pragma: no cover
    redis = None            # noqa
    ConnectionError = None  # noqa


class RedisBackend(KeyValueStoreBackend):
    """Redis task result store."""

    #: redis-py client module.
    redis = redis

    #: default Redis server hostname (`localhost`).
    host = 'localhost'

    #: default Redis server port (6379)
    port = 6379

    #: default Redis db number (0)
    db = 0

    #: default Redis password (:const:`None`)
    password = None

    #: Maximium number of connections in the pool.
    max_connections = None

    supports_autoexpire = True
    supports_native_join = True
    implements_incr = True

    def __init__(self, host=None, port=None, db=None, password=None,
                 expires=None, max_connections=None, url=None, **kwargs):
        super(RedisBackend, self).__init__(**kwargs)
        conf = self.app.conf
        if self.redis is None:
            raise ImproperlyConfigured(
                'You need to install the redis library in order to use '
                'the Redis result store backend.')

        # For compatibility with the old REDIS_* configuration keys.
        def _get(key):
            for prefix in 'CELERY_REDIS_%s', 'REDIS_%s':
                try:
                    return conf[prefix % key]
                except KeyError:
                    pass
        if host and '://' in host:
            url, host = host, None
        self.url = url
        uhost = uport = upass = udb = None
        if url:
            _, uhost, uport, _, upass, udb, _ = _parse_url(url)
            udb = udb.strip('/') if udb else 0
        self.host = uhost or host or _get('HOST') or self.host
        self.port = int(uport or port or _get('PORT') or self.port)
        self.db = udb or db or _get('DB') or self.db
        self.password = upass or password or _get('PASSWORD') or self.password
        self.expires = self.prepare_expires(expires, type=int)
        self.max_connections = (max_connections
                                or _get('MAX_CONNECTIONS')
                                or self.max_connections)

    def get(self, key):
        return self.client.get(key)

    def mget(self, keys):
        return self.client.mget(keys)

    def set(self, key, value):
        client = self.client
        if self.expires is not None:
            client.setex(key, value, self.expires)
        else:
            client.set(key, value)
        client.publish(key, value)

    def delete(self, key):
        self.client.delete(key)

    def incr(self, key):
        return self.client.incr(key)

    def expire(self, key, value):
        return self.client.expire(key, value)

    @cached_property
    def client(self):
        pool = self.redis.ConnectionPool(host=self.host, port=self.port,
                                         db=self.db, password=self.password,
                                         max_connections=self.max_connections)
        return self.redis.Redis(connection_pool=pool)

    def __reduce__(self, args=(), kwargs={}):
        kwargs.update(
            dict(host=self.host,
                 port=self.port,
                 db=self.db,
                 password=self.password,
                 expires=self.expires,
                 max_connections=self.max_connections))
        return super(RedisBackend, self).__reduce__(args, kwargs)
