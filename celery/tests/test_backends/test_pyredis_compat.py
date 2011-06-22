from celery.backends import pyredis
from celery.tests.utils import unittest


class test_RedisBackend(unittest.TestCase):

    def test_constructor(self):
        x = pyredis.RedisBackend(redis_host="foobar", redis_port=312,
                                 redis_db=1, redis_password="foo")
        self.assertEqual(x.redis_host, "foobar")
        self.assertEqual(x.redis_port, 312)
        self.assertEqual(x.redis_db, 1)
        self.assertEqual(x.redis_password, "foo")
