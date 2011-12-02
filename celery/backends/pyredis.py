# -*- coding: utf-8 -*-
"""
This is here for backwards compatibility only.

Please use :class:`celery.backends.redis.RedisBackend` instead.

"""
from __future__ import absolute_import

from . import redis


class RedisBackend(redis.RedisBackend):

    def __init__(self, redis_host=None, redis_port=None, redis_db=None,
            redis_password=None, **kwargs):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password
        # Changed in order to avoid duplicated arguments
        super(RedisBackend, self).__init__(**dict(kwargs, host=redis_host,
                                           port=redis_port, db=redis_db,
                                           password=redis_password))

    def __reduce__(self, args=(), kwargs={}):
        # Not very useful, but without the following, the redis_* attributes
        # would not be set.
        kwargs.update(
            dict(redis_host=self.redis_host,
                 redis_port=self.redis_port,
                 redis_db=self.redis_db,
                 redis_password=self.redis_password))
        return super(RedisBackend, self).__reduce__(args, kwargs)
