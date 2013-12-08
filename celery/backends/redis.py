# -*- coding: utf-8 -*-
"""
    celery.backends.redis
    ~~~~~~~~~~~~~~~~~~~~~

    Redis result store backend.

"""
from __future__ import absolute_import

import re

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

__all__ = ['RedisBackend']

REDIS_MISSING = """\
You need to install the redis library in order to use \
the Redis result store backend."""

class RedisConnectionParams(object):
    """
    Bulky class/module that handles taking Redis connection
    parameters from a url, and mixing them from different default
    sources. Should probably be used both by kombu and this module...


    """

    _tcp_default_params = {
        #: default Redis server hostname (`localhost`).
        'host' : 'localhost',

        #: default Redis server port (6379)
        'port' : 6379,

        #: default Redis db number (0)
        'db' : 0,

        #: default Redis password (:const:`None`)
        'password' : None,

        'connection_class': redis.Connection
    }

    _unix_default_params = {
        #: default Redis db number (0)
        'db' : 0,

        #: default Redis password (:const:`None`)
        'password' : None,

        'connection_class': redis.UnixDomainSocketConnection
    }

    @staticmethod
    def prepare_connection_params(given_params, default_params_1=None, default_params_2=None):
        """ Creates a dictionary with connection parameters, where a key in 'given_params' has greater
            priority, default_params_1 has lower, and default_params_2 has the lowest. In all the cases,
            a key not present or with value None will trigger a lookup in the following level.
        """
        assert isinstance(given_params, dict)
        if default_params_1 is None: default_params_1 = {}

        connection_class = given_params.get('connection_class')
        if connection_class and connection_class is redis.UnixDomainSocketConnection:
            default_params = RedisConnectionParams._unix_default_params
        else:
            default_params = RedisConnectionParams._tcp_default_params

        if default_params_2 is None:
            default_params_2 = default_params

        result = given_params.copy()
        for key in default_params.keys():
            if key not in result:
                possible_value = default_params_1.get(key) or default_params_2.get(key)
                if possible_value is not None:
                    result[key] = possible_value
        return result

    @staticmethod
    def connparams_from_url(url):
        scheme, host, port, user, password, path, query = _parse_url(url)

        connparams = {}
        if host: connparams['host'] = host
        if port: connparams['port'] = port
        if user: connparams['user'] = user
        if password: connparams['password'] = password

        if query and 'virtual_host' in query:
            db_no = query['virtual_host']
            del query['virtual_host']
            query['db'] = int(db_no)

        if scheme == 'socket':
            # Use 'path' as path to the socket... in this case
            # the database number should be given in 'query'
            connparams.update({
                'connection_class': redis.UnixDomainSocketConnection,
                'path': '/' + path})
            connparams.pop('host', None)
            connparams.pop('port', None)
        else:
            #  Use 'path' to deduce a database number
            maybe_vhost = re.search(r'/(\d+)/?$', path)
            if maybe_vhost:
                db = int(maybe_vhost.group(1))
                connparams['db'] = db
        # Query parameters override other parameters
        connparams.update(query)
        return connparams


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

        old_config_port = _get('PORT')

        connparams = RedisConnectionParams.prepare_connection_params(
            RedisConnectionParams.connparams_from_url(url) if url else {},
            {
                'host': _get('HOST'),
                'port': int(old_config_port) if old_config_port else None,
                'db': _get('DB'),
                'password': _get('PASSWORD')
            }
        )

        self.connparams = connparams
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
        pool = self.redis.ConnectionPool(max_connections=self.max_connections,
                                         **self.connparams)
        return self.redis.Redis(connection_pool=pool)

    def __reduce__(self, args=(), kwargs={}):
        kwargs.update(
            dict(expires=self.expires,
                 max_connections=self.max_connections, **self.connparams))
        return super(RedisBackend, self).__reduce__(args, kwargs)

