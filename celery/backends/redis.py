from __future__ import absolute_import

from kombu.utils import cached_property

from celery.backends.base import KeyValueStoreBackend
from celery.exceptions import ImproperlyConfigured

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

    def __init__(self, host=None, port=None, db=None, password=None,
            expires=None, **kwargs):
        super(RedisBackend, self).__init__(**kwargs)
        conf = self.app.conf
        if self.redis is None:
            raise ImproperlyConfigured(
                    "You need to install the redis library in order to use "
                  + "Redis result store backend.")

        # For compatability with the old REDIS_* configuration keys.
        def _get(key):
            for prefix in "REDIS_%s", "CELERY_REDIS_%s":
                try:
                    return conf[prefix % key]
                except KeyError:
                    pass

        self.host = host or _get("HOST") or self.host
        self.port = int(port or _get("PORT") or self.port)
        self.db = db or _get("DB") or self.db
        self.password = password or _get("PASSWORD") or self.password
        self.expires = self.prepare_expires(expires, type=int)

    def get(self, key):
        return self.client.get(key)

    def mget(self, keys):
        return self.client.mget(keys)

    def set(self, key, value):
        client = self.client
        client.set(key, value)
        if self.expires is not None:
            client.expire(key, self.expires)

    def delete(self, key):
        self.client.delete(key)

    def process_cleanup(self):
        pass

    def on_chord_apply(self, *args, **kwargs):
        pass

    def on_chord_part_return(self, task, keyprefix="chord-unlock-%s"):
        from celery.task.sets import subtask
        from celery.result import TaskSetResult
        setid = task.request.taskset
        key = keyprefix % setid
        deps = TaskSetResult.restore(setid, backend=task.backend)
        if self.client.incr(key) >= deps.total:
            subtask(task.request.chord).delay(deps.join())
            deps.delete()
        self.client.expire(key, 86400)

    @cached_property
    def client(self):
        return self.redis.Redis(host=self.host, port=self.port,
                                db=self.db, password=self.password)
