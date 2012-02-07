# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ..exceptions import ImproperlyConfigured
from ..utils import cached_property

from .base import KeyValueStoreBackend

try:
    import redis
    from redis.exceptions import ConnectionError
except ImportError:
    redis = None            # noqa
    ConnectionError = None  # noqa


class RedisBackend(KeyValueStoreBackend):
    """Redis task result store."""

    #: redis-py client module.
    redis = redis

    #: default Redis server hostname (`localhost`).
    host = "localhost"

    #: default Redis server port (6379)
    port = 6379

    #: default Redis db number (0)
    db = 0

    #: default Redis password (:const:`None`)
    password = None

    #: Maximium number of connections in the pool.
    max_connections = None

    supports_native_join = True

    def __init__(self, host=None, port=None, db=None, password=None,
            expires=None, max_connections=None, **kwargs):
        super(RedisBackend, self).__init__(**kwargs)
        conf = self.app.conf
        if self.redis is None:
            raise ImproperlyConfigured(
                    "You need to install the redis library in order to use "
                  + "Redis result store backend.")

        # For compatibility with the old REDIS_* configuration keys.
        def _get(key):
            for prefix in "CELERY_REDIS_%s", "REDIS_%s":
                try:
                    return conf[prefix % key]
                except KeyError:
                    pass

        self.host = host or _get("HOST") or self.host
        self.port = int(port or _get("PORT") or self.port)
        self.db = db or _get("DB") or self.db
        self.password = password or _get("PASSWORD") or self.password
        self.expires = self.prepare_expires(expires, type=int)
        self.max_connections = (max_connections
                                or _get("MAX_CONNECTIONS")
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

    def on_chord_apply(self, setid, body, result=None, **kwargs):
        self.app.TaskSetResult(setid, result).save()

    def on_chord_part_return(self, task, propagate=False):
        from ..task.sets import subtask
        from ..result import TaskSetResult
        setid = task.request.taskset
        if not setid:
            return
        key = self.get_key_for_chord(setid)
        deps = TaskSetResult.restore(setid, backend=task.backend)
        if self.client.incr(key) >= deps.total:
            subtask(task.request.chord).delay(deps.join(propagate=propagate))
            deps.delete()
            self.client.delete(key)
        else:
            self.client.expire(key, 86400)

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
