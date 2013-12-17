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
from celery.five import string_t
from celery.utils import deprecated_property
from celery.utils.functional import dictfilter

from .base import KeyValueStoreBackend

try:
    import redis
    from redis.exceptions import ConnectionError
except ImportError:         # pragma: no cover
    redis = None            # noqa
    ConnectionError = None  # noqa

__all__ = ['RedisBackend']

REDIS_MISSING = """\
You need to install the redis library in order to use \
the Redis result store backend."""


class RedisBackend(KeyValueStoreBackend):
    """Redis task result store."""

    #: redis-py client module.
    redis = redis

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
            raise ImproperlyConfigured(REDIS_MISSING)

        # For compatibility with the old REDIS_* configuration keys.
        def _get(key):
            for prefix in 'CELERY_REDIS_{0}', 'REDIS_{0}':
                try:
                    return conf[prefix.format(key)]
                except KeyError:
                    pass
        if host and '://' in host:
            url = host
            host = None

        self.max_connections = (
            max_connections or _get('MAX_CONNECTIONS') or self.max_connections
        )

        self.connparams = {
            'host': _get('HOST') or 'localhost',
            'port': _get('PORT') or 6379,
            'db': _get('DB') or 0,
            'password': _get('PASSWORD'),
            'max_connections': max_connections,
        }
        if url:
            self.connparams = self._params_from_url(url, self.connparams)
        self.url = url
        self.expires = self.prepare_expires(expires, type=int)

    def _params_from_url(self, url, defaults):
        scheme, host, port, user, password, path, query = _parse_url(url)
        connparams = dict(
            defaults, **dictfilter({
                'host': host, 'port': port, 'password': password,
                'db': query.pop('virtual_host', None)})
        )

        if scheme == 'socket':
            # use 'path' as path to the socket… in this case
            # the database number should be given in 'query'
            connparams.update({
                'connection_class': self.redis.UnixDomainSocketConnection,
                'path': '/' + path,
            })
            # host+port are invalid options when using this connection type.
            connparams.pop('host', None)
            connparams.pop('port', None)
        else:
            connparams['db'] = path

        # db may be string and start with / like in kombu.
        db = connparams.get('db') or 0
        db = db.strip('/') if isinstance(db, string_t) else db
        connparams['db'] = int(db)

        # Query parameters override other parameters
        connparams.update(query)
        return connparams

    def get(self, key):
        return self.client.get(key)

    def mget(self, keys):
        return self.client.mget(keys)

    def set(self, key, value):
        client = self.client
        if self.expires:
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
        return self.redis.Redis(
            connection_pool=self.redis.ConnectionPool(**self.connparams))

    def __reduce__(self, args=(), kwargs={}):
        return super(RedisBackend, self).__reduce__(
            (self.url, ), {'expires': self.expires},
        )

    @deprecated_property(3.2, 3.3)
    def host(self):
        return self.connparams['host']

    @deprecated_property(3.2, 3.3)
    def port(self):
        return self.connparams['port']

    @deprecated_property(3.2, 3.3)
    def db(self):
        return self.connparams['db']

    @deprecated_property(3.2, 3.3)
    def password(self):
        return self.connparams['password']
