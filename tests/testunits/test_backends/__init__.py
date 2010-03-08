import unittest


from celery.backends.database import DatabaseBackend
from celery.backends.amqp import AMQPBackend
from celery.backends.pyredis import RedisBackend
from celery import backends


class TestBackends(unittest.TestCase):

    def test_get_backend_aliases(self):
        self.assertTrue(issubclass(
            backends.get_backend_cls("amqp"), AMQPBackend))
        self.assertTrue(issubclass(
            backends.get_backend_cls("database"), DatabaseBackend))
        self.assertTrue(issubclass(
            backends.get_backend_cls("db"), DatabaseBackend))
        self.assertTrue(issubclass(
            backends.get_backend_cls("redis"), RedisBackend))

    def test_get_backend_cahe(self):
        backends._backend_cache = {}
        backends.get_backend_cls("amqp")
        self.assertTrue("amqp" in backends._backend_cache)
        amqp_backend = backends.get_backend_cls("amqp")
        self.assertTrue(amqp_backend is backends._backend_cache["amqp"])
