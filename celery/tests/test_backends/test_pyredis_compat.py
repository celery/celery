from __future__ import absolute_import

from nose import SkipTest

from celery.exceptions import ImproperlyConfigured
from celery.tests.utils import Case


class test_RedisBackend(Case):

    def test_constructor(self):
        from celery.backends import pyredis
        try:
            x = pyredis.RedisBackend(redis_host="foobar", redis_port=312,
                                    redis_db=1, redis_password="foo")
        except ImproperlyConfigured:
            raise SkipTest("redis-py not installed")
        self.assertEqual(x.redis_host, "foobar")
        self.assertEqual(x.redis_port, 312)
        self.assertEqual(x.redis_db, 1)
        self.assertEqual(x.redis_password, "foo")
