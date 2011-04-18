"""
This is here for backwards compatibility only.

Please use :class:`celery.backends.redis.RedisBackend` instead.

"""
from __future__ import absolute_import

from celery.backends import redis


class RedisBackend(redis.RedisBackend):

    def __init__(self, redis_host=None, redis_port=None, redis_db=None,
            redis_password=None, **kwargs):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password
        super(RedisBackend, self).__init__(host=redis_host,
                                           port=redis_port, db=redis_db,
                                           password=redis_password, **kwargs)
